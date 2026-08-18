import { type Readable, Transform, Writable } from "node:stream";
import { crc32, createDeflateRaw } from "node:zlib";
import { finished, pipeline } from "node:stream/promises";

import { randomBytes } from "node:crypto";

import {
  CENTRAL_FILE_HEADER_LEN,
  DOS_DATE_DEFAULT,
  ZIP64_SIZE,
  ZIPCRYPTO_HEADER_LEN,
  ZIP_FLAG_DATA_DESCRIPTOR,
  ZIP_FLAG_ENCRYPTED,
  ZIP_FLAG_UTF8,
  ZIP_METHOD_DEFLATE,
  ZIP_UINT16_MAX,
} from "./zip/constants.js";
import {
  type CdRecord,
  encodeCentralHeader,
  encodeDataDescriptor,
  encodeEocd,
  encodeLocalHeader,
} from "./zip/records.js";
import { type ZipCrypto, createZipCrypto, expectedCheckByte } from "./zip/crypto.js";
import { asBuffer } from "./stream/pull.js";
import { posixEntryName } from "./zip/entry-name.js";
import { toError } from "./stream/to-error.js";

/** Public error when `add` or `end` runs after `end()`. */
const ZIP_ALREADY_CLOSED = "zip already closed";

/** Public error when `add`/`end` runs while another write is in flight. */
const ZIP_WRITE_IN_PROGRESS = "zip write in progress";

/** Public prefix when `add`/`end` runs after a previous write failed. */
const ZIP_WRITER_FAILED = "zip writer failed";

/** Public error when `dest` closes or errors instead of accepting bytes. */
const ZIP_DESTINATION_CLOSED = "zip destination closed";

/** Options for a streaming zip writer. */
export type ZipWriteOptions = {
  /** Uncompressed-byte cap per `add`. Unlimited when unset. */
  maxEntrySize?: number;
  /** ZipCrypto password. Omitted/`undefined` → unencrypted; `""` is a real password. */
  password?: string;
  /** Password bytes encoding. Default UTF-8. */
  passwordEncoding?: BufferEncoding;
};

/**
 * Sequential streaming zip writer: local entries, then CD+EOCD on `end`.
 * File payloads are not buffered; only central-directory records are kept.
 */
export type ZipWriter = {
  /** Append one deflated (and optionally ZipCrypto) entry. */
  add(name: string, body: Readable): Promise<void>;
  /** Write the central directory and EOCD, then wait until `dest` has finished. */
  end(): Promise<void>;
};

/**
 * APPNOTE 32-bit size/offset fields use 0xffffffff as the Zip64 sentinel.
 * Writing that value would make our own reader reject the archive.
 */
function assertNotZip64(size: number, context: string): void {
  if (size >= ZIP64_SIZE) {
    throw new Error(`Zip64 not supported: ${context}`);
  }
}

/**
 * Honor `write()` backpressure: a false return means the sink is full, and
 * ignoring `drain` would unbounded-buffer the zip in memory. `drain` can hang
 * forever if the sink closes without it — race `close`/`finish`/`error`.
 */
async function writeDest(dest: Writable, buf: Buffer): Promise<void> {
  if (buf.length === 0) {
    return;
  }
  if (!dest.writable || dest.destroyed) {
    throw new Error(ZIP_DESTINATION_CLOSED);
  }
  if (dest.write(buf)) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    const onDrain = (): void => {
      cleanup();
      resolve();
    };
    const onClosed = (): void => {
      cleanup();
      reject(new Error(ZIP_DESTINATION_CLOSED));
    };
    const cleanup = (): void => {
      dest.off("close", onClosed);
      dest.off("drain", onDrain);
      dest.off("error", onClosed);
      dest.off("finish", onClosed);
    };

    dest.once("drain", onDrain);
    dest.once("close", onClosed);
    dest.once("error", onClosed);
    dest.once("finish", onClosed);
  });
}

/**
 * Write a ZipCrypto (or unencrypted) zip to `dest` without buffering payloads.
 * Local sizes are zero with bit 3; CRC and sizes follow in a 16-byte descriptor.
 */
export function zipEncrypted(dest: Writable, options: ZipWriteOptions = {}): ZipWriter {
  const maxEntrySize = options.maxEntrySize;
  const password = options.password;
  const passwordEncoding = options.passwordEncoding ?? "utf8";
  const entries: CdRecord[] = [];
  let closed = false;
  let busy = false;
  let failed: Error | undefined;
  let offset = 0;

  const write = async (buf: Buffer): Promise<void> => {
    await writeDest(dest, buf);
    offset += buf.length;
  };

  const failIfUnusable = (): void => {
    if (failed !== undefined) {
      throw new Error(`${ZIP_WRITER_FAILED}: ${failed.message}`);
    }
    if (closed) {
      throw new Error(ZIP_ALREADY_CLOSED);
    }
    if (busy) {
      throw new Error(ZIP_WRITE_IN_PROGRESS);
    }
  };

  const run = async (work: () => Promise<void>): Promise<void> => {
    failIfUnusable();
    busy = true;
    try {
      await work();
    } catch (err) {
      failed = toError(err);
      throw failed;
    } finally {
      busy = false;
    }
  };

  const addEntry = async (name: string, body: Readable): Promise<void> => {
    const entryName = posixEntryName(name);
    const nameBuf = Buffer.from(entryName, "utf8");
    const flags =
      ZIP_FLAG_UTF8 | ZIP_FLAG_DATA_DESCRIPTOR | (password !== undefined ? ZIP_FLAG_ENCRYPTED : 0);
    const modTime = 0;
    const modDate = DOS_DATE_DEFAULT;

    if (nameBuf.length > ZIP_UINT16_MAX) {
      throw new Error(`Zip64 not supported: ${entryName}`);
    }

    assertNotZip64(offset, entryName);

    const localOffset = offset;

    await write(encodeLocalHeader(flags, nameBuf, modTime, modDate));
    await write(nameBuf);

    // New keys per file: reusing ZipCrypto state across entries mixes the stream.
    // `""` is a real ZipCrypto password (`Buffer.from("", encoding)` is valid).
    const crypto: ZipCrypto | undefined =
      password !== undefined ? createZipCrypto(Buffer.from(password, passwordEncoding)) : undefined;
    let compressedSize = 0;
    let uncompressedSize = 0;
    let crc = 0;

    if (crypto !== undefined) {
      const headerPlain = randomBytes(ZIPCRYPTO_HEADER_LEN);

      // Bit 3 is set, so the check byte is the high DOS time (crc is still 0).
      headerPlain[ZIPCRYPTO_HEADER_LEN - 1] = expectedCheckByte(flags, 0, modTime);
      await write(crypto.encrypt(headerPlain));
      compressedSize += ZIPCRYPTO_HEADER_LEN;
    }

    const countAndCrc = new Transform({
      transform(chunk: Buffer, _encoding, callback): void {
        try {
          const buf = asBuffer(chunk);

          uncompressedSize += buf.length;
          if (maxEntrySize !== undefined && uncompressedSize > maxEntrySize) {
            callback(new Error(`entry exceeds maxEntrySize: ${entryName}`));

            return;
          }
          assertNotZip64(uncompressedSize, entryName);
          crc = crc32(buf, crc) >>> 0;
          callback(null, buf);
        } catch (err) {
          callback(toError(err));
        }
      },
    });
    const sink = new Writable({
      write(chunk: Buffer, _encoding, callback): void {
        void (async () => {
          const raw = asBuffer(chunk);
          const data = crypto !== undefined ? crypto.encrypt(raw) : raw;

          compressedSize += data.length;
          assertNotZip64(compressedSize, entryName);
          await write(data);
        })().then(
          () => {
            callback();
          },
          (err: unknown) => {
            callback(toError(err));
          },
        );
      },
    });

    await pipeline(body, countAndCrc, createDeflateRaw(), sink);
    assertNotZip64(compressedSize, entryName);
    assertNotZip64(uncompressedSize, entryName);
    // Descriptor is plaintext even when the payload is ZipCrypto (APPNOTE 6.1).
    await write(encodeDataDescriptor(crc, compressedSize, uncompressedSize));

    entries.push({
      compressedSize,
      crc,
      flags,
      localOffset,
      method: ZIP_METHOD_DEFLATE,
      modDate,
      modTime,
      nameBuf,
      uncompressedSize,
    });
  };

  const finishArchive = async (): Promise<void> => {
    closed = true;

    if (entries.length > ZIP_UINT16_MAX) {
      throw new Error("Zip64 not supported: central directory");
    }

    const cdOffset = offset;
    const cdSize = entries.reduce(
      (sum, entry) => sum + CENTRAL_FILE_HEADER_LEN + entry.nameBuf.length,
      0,
    );

    assertNotZip64(cdSize, "central directory");
    assertNotZip64(cdOffset, "central directory");

    for (const entry of entries) {
      await write(encodeCentralHeader(entry));
      await write(entry.nameBuf);
    }

    await write(encodeEocd(entries.length, cdSize, cdOffset));
    dest.end();
    await finished(dest);
  };

  return {
    add(name: string, body: Readable): Promise<void> {
      return run(async () => addEntry(name, body));
    },

    end(): Promise<void> {
      return run(finishArchive);
    },
  };
}
