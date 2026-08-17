import { Readable, type ReadableOptions, Writable } from "node:stream";
import { crc32, createInflateRaw } from "node:zlib";
import { finished, pipeline } from "node:stream/promises";

import { once } from "node:events";

import {
  AES_EXTRA,
  CENTRAL,
  DATA_DESCRIPTOR_NO_SIG_LEN,
  DATA_DESCRIPTOR_WITH_SIG_LEN,
  DD_SIG,
  EOCD,
  EXTRA_FIELD_HEADER_LEN,
  LOCAL,
  LOCAL_FILE_HEADER_AFTER_SIG_LEN,
  ZIP64_EXTRA,
  ZIP64_SIZE,
  ZIPCRYPTO_HEADER_LEN,
  ZIP_AES_METHOD,
  ZIP_FLAG_DATA_DESCRIPTOR,
  ZIP_FLAG_ENCRYPTED,
  ZIP_FLAG_UTF8,
  ZIP_METHOD_DEFLATE,
  ZIP_SIGNATURE_LEN,
} from "./zip/constants.js";
import { PULL_CHUNK_SIZE, type PullReader, createPull } from "./stream/pull.js";
import { type ZipCrypto, createZipCrypto, expectedCheckByte } from "./zip/crypto.js";

/** Password and filter options for one-pass ZipCrypto unzip. */
export type UnzipOptions = {
  /** Required only for encrypted entries; an encrypted entry without it fails. */
  password?: string;
  /** OEM/cp437 archives; default is UTF-8. */
  passwordEncoding?: BufferEncoding;
  /** If false, discard ciphertext by size (no inflate, no yield). */
  filter?: (path: string) => boolean;
  /**
   * Hard cap on uncompressed bytes per entry. Unlimited when unset.
   * Local-header sizes are attacker-controlled, so only this bounds a zip bomb.
   */
  maxEntrySize?: number;
};

/**
 * One zip local-file entry. Consume via `pipeline` or `autodrain` before
 * calling `next()`, or the parser waits forever on the unread body.
 */
export type ZipEntry = Readable & {
  path: string;
  type: "File" | "Directory";
  compressedSize?: number;
  uncompressedSize?: number;
  encrypted: boolean;
  autodrain(): Promise<void>;
};

type EntryMeta = {
  path: string;
  type: "File" | "Directory";
  compressedSize: number;
  uncompressedSize: number;
  encrypted: boolean;
};

type BodyCursor = {
  remaining: number;
};

type BodyResult = {
  /** CRC32 of the bytes handed to the consumer. */
  crc: number;
  /** Number of uncompressed bytes produced. */
  size: number;
};

type BodyOptions = {
  crypto: ZipCrypto | undefined;
  method: number;
  /** Local-header CRC32, or undefined when it lives in the data descriptor. */
  expectedCrc: number | undefined;
  /** Local-header uncompressed size, or undefined when the header omits it. */
  expectedSize: number | undefined;
  maxEntrySize: number | undefined;
};

/**
 * Yielded Readable: `_read` pulls zip ciphertext only when the consumer wants
 * bytes, so unused entries can skip by `compressedSize` instead of inflate.
 */
class ZipEntryStream extends Readable implements ZipEntry {
  readonly path: string;
  readonly type: "File" | "Directory";
  readonly compressedSize: number;
  readonly uncompressedSize: number;
  readonly encrypted: boolean;
  onDemand: (() => void) | undefined;

  constructor(meta: EntryMeta, options?: ReadableOptions) {
    super(options);
    this.path = meta.path;
    this.type = meta.type;
    this.compressedSize = meta.compressedSize;
    this.uncompressedSize = meta.uncompressedSize;
    this.encrypted = meta.encrypted;
  }

  override _read(): void {
    this.onDemand?.();
  }

  autodrain(): Promise<void> {
    return pipeline(
      this,
      new Writable({
        write(_chunk, _encoding, callback) {
          callback();
        },
      }),
    );
  }
}

/**
 * Fail closed on short reads: a truncated header would otherwise look like
 * the next signature or a valid extra field.
 */
async function readExact(pull: PullReader, n: number, what: string): Promise<Buffer> {
  const buf = await pull.read(n);

  if (!buf || buf.length < n) {
    throw new Error(`unexpected EOF while reading ${what}`);
  }

  return buf;
}

/** Walk the id/size records once; a record running past the end is fatal. */
function extraFieldIds(extra: Buffer, name: string): Set<number> {
  const ids = new Set<number>();
  let offset = 0;

  while (offset + EXTRA_FIELD_HEADER_LEN <= extra.length) {
    const id = extra.readUInt16LE(offset);
    const size = extra.readUInt16LE(offset + 2);

    if (offset + EXTRA_FIELD_HEADER_LEN + size > extra.length) {
      throw new Error(`malformed extra field: ${name}`);
    }
    ids.add(id);
    offset += EXTRA_FIELD_HEADER_LEN + size;
  }

  return ids;
}

type DataDescriptor = {
  crc: number;
  compressedSize: number;
  uncompressedSize: number;
};

/**
 * The signature is optional, so the first word is either it or the CRC. Reading
 * 4 then 8/12 bytes consumes the record exactly, without needing to unread.
 */
async function readDataDescriptor(pull: PullReader, name: string): Promise<DataDescriptor> {
  const what = `data descriptor: ${name}`;
  const first = await readExact(pull, ZIP_SIGNATURE_LEN, what);

  if (first.readUInt32LE(0) === DD_SIG) {
    const rest = await readExact(pull, DATA_DESCRIPTOR_WITH_SIG_LEN - first.length, what);

    return {
      compressedSize: rest.readUInt32LE(4),
      crc: rest.readUInt32LE(0),
      uncompressedSize: rest.readUInt32LE(8),
    };
  }
  const rest = await readExact(pull, DATA_DESCRIPTOR_NO_SIG_LEN - first.length, what);

  return {
    compressedSize: rest.readUInt32LE(0),
    crc: first.readUInt32LE(0),
    uncompressedSize: rest.readUInt32LE(4),
  };
}

/**
 * Decrypt the 12-byte ZipCrypto header and check byte 11 (APPNOTE 6.1).
 * New crypto state is required per entry; this only consumes the header.
 */
async function skipEncryptedHeader(
  pull: PullReader,
  crypto: ZipCrypto,
  flags: number,
  crc: number,
  modTime: number,
  name: string,
): Promise<void> {
  const header = await readExact(pull, ZIPCRYPTO_HEADER_LEN, `zip crypto header: ${name}`);
  const plain = crypto.decrypt(header);
  const check = plain.at(ZIPCRYPTO_HEADER_LEN - 1);

  if (check !== expectedCheckByte(flags, crc, modTime)) {
    throw new Error(`invalid zip password: ${name}`);
  }
}

/**
 * Advance past unused ciphertext by size (no inflate). If bit 3 is set,
 * still consume the data descriptor so the next signature is aligned.
 */
async function skipRemaining(
  pull: PullReader,
  remaining: number,
  dataDescriptor: boolean,
  name: string,
): Promise<void> {
  if (remaining > 0) {
    await pull.discard(remaining);
  }
  if (dataDescriptor) {
    await readDataDescriptor(pull, name);
  }
}

function entryFailError(entry: ZipEntryStream): Error {
  const err = entry.errored;

  return err instanceof Error ? err : new Error("entry closed");
}

/**
 * Body pump must stop if the consumer destroys the entry; otherwise
 * `pull.read` would keep draining the zip after the generator should move on.
 */
async function raceEntryAbort<T>(entry: ZipEntryStream, work: Promise<T>): Promise<T> {
  if (entry.destroyed) {
    throw entryFailError(entry);
  }
  let onError: ((err: Error) => void) | undefined;
  let onClose: (() => void) | undefined;
  const aborted = new Promise<never>((_, reject) => {
    onError = (err) => {
      reject(err);
    };
    onClose = () => {
      reject(entryFailError(entry));
    };
    entry.once("error", onError);
    entry.once("close", onClose);
  });

  try {
    return await Promise.race([work, aborted]);
  } finally {
    if (onError) {
      entry.off("error", onError);
    }
    if (onClose) {
      entry.off("close", onClose);
    }
  }
}

/**
 * Stored bodies pause when `push` returns false; resume only on the next
 * `_read` so the zip source is not read ahead of demand.
 */
async function waitForDemand(entry: ZipEntryStream): Promise<void> {
  await raceEntryAbort(
    entry,
    new Promise<void>((resolve) => {
      entry.onDemand = () => {
        resolve();
      };
    }),
  );
}

function assertCrc(actual: number, expected: number | undefined, name: string): void {
  if (expected === undefined) {
    return;
  }
  if (actual >>> 0 !== expected >>> 0) {
    throw new Error(`crc mismatch: ${name}`);
  }
}

function assertSize(actual: number, expected: number | undefined, name: string): void {
  if (expected === undefined) {
    return;
  }
  if (actual !== expected) {
    throw new Error(`size mismatch: ${name}`);
  }
}

/**
 * Stop as soon as output passes a bound instead of after the fact: the declared
 * size catches lying headers, `maxEntrySize` catches a header that declares a
 * huge size honestly.
 */
function checkGrowth(written: number, options: BodyOptions, name: string): void {
  const { expectedSize, maxEntrySize } = options;

  if (maxEntrySize !== undefined && written > maxEntrySize) {
    throw new Error(`entry exceeds maxEntrySize: ${name}`);
  }
  if (expectedSize !== undefined && written > expectedSize) {
    throw new Error(`size mismatch: ${name}`);
  }
}

/**
 * Ciphertext → ZipCrypto (if bit 0) → inflateRaw or stored → entry.
 * Reads the zip only while the entry is flowing; directories discard any
 * claimed size without producing bytes.
 */
async function pumpBody(
  entry: ZipEntryStream,
  pull: PullReader,
  cursor: BodyCursor,
  options: BodyOptions,
): Promise<BodyResult> {
  const { crypto, expectedCrc, expectedSize, method } = options;

  if (entry.type === "Directory") {
    if (cursor.remaining > 0) {
      await pull.discard(cursor.remaining);
      cursor.remaining = 0;
    }
    entry.push(null);

    return { crc: 0, size: 0 };
  }

  if (cursor.remaining <= 0) {
    assertSize(0, expectedSize, entry.path);
    assertCrc(0, expectedCrc, entry.path);
    entry.push(null);

    return { crc: 0, size: 0 };
  }

  let outCrc = 0;
  let written = 0;

  if (method === ZIP_METHOD_DEFLATE) {
    const inflate = createInflateRaw();

    inflate.on("data", (chunk: Buffer) => {
      written += chunk.length;
      try {
        checkGrowth(written, options, entry.path);
      } catch (err) {
        inflate.destroy(err instanceof Error ? err : new Error(String(err)));

        return;
      }
      outCrc = crc32(chunk, outCrc) >>> 0;
      if (!entry.push(chunk)) {
        inflate.pause();
      }
    });
    inflate.on("end", () => {
      try {
        assertSize(written, expectedSize, entry.path);
        assertCrc(outCrc, expectedCrc, entry.path);
        if (!entry.destroyed) {
          entry.push(null);
        }
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));

        entry.destroy(error);
      }
    });
    inflate.on("error", (err: Error) => {
      entry.destroy(err);
    });
    const previous = entry.onDemand;

    entry.onDemand = () => {
      inflate.resume();
      previous?.();
    };

    try {
      while (cursor.remaining > 0 && !entry.destroyed) {
        const n = Math.min(cursor.remaining, PULL_CHUNK_SIZE);
        const chunk = await pull.read(n);

        if (!chunk || chunk.length === 0) {
          throw new Error("unexpected EOF while reading file data");
        }
        cursor.remaining -= chunk.length;
        const plain = crypto ? crypto.decrypt(chunk) : chunk;

        if (!inflate.write(plain)) {
          await raceEntryAbort(entry, once(inflate, "drain"));
        }
      }
      if (entry.destroyed) {
        throw entryFailError(entry);
      }
      const inflateDone = finished(inflate);

      inflate.end();
      inflate.resume();
      await inflateDone;
      if (entry.errored) {
        throw entryFailError(entry);
      }
    } catch (err) {
      inflate.destroy();
      throw err instanceof Error ? err : new Error(String(err));
    }

    return { crc: outCrc, size: written };
  }

  while (cursor.remaining > 0 && !entry.destroyed) {
    const n = Math.min(cursor.remaining, PULL_CHUNK_SIZE);
    const chunk = await pull.read(n);

    if (!chunk || chunk.length === 0) {
      throw new Error("unexpected EOF while reading file data");
    }
    cursor.remaining -= chunk.length;
    const plain = crypto ? crypto.decrypt(chunk) : chunk;

    written += plain.length;
    checkGrowth(written, options, entry.path);
    outCrc = crc32(plain, outCrc) >>> 0;
    if (!entry.push(plain) && cursor.remaining > 0) {
      await waitForDemand(entry);
    }
  }
  if (entry.destroyed) {
    throw entryFailError(entry);
  }
  assertSize(written, expectedSize, entry.path);
  assertCrc(outCrc, expectedCrc, entry.path);
  entry.push(null);

  return { crc: outCrc, size: written };
}

/**
 * Walk local file headers until CD/EOCD (APPNOTE). One ZipCrypto state per
 * encrypted entry; yield before reading the body so skip can discard by size.
 */
async function* parseEntries(
  pull: PullReader,
  options: UnzipOptions,
): AsyncGenerator<ZipEntry, void, unknown> {
  const password =
    options.password === undefined
      ? undefined
      : Buffer.from(options.password, options.passwordEncoding ?? "utf8");

  for (;;) {
    const sigBuf = await pull.read(ZIP_SIGNATURE_LEN);

    if (!sigBuf || sigBuf.length < ZIP_SIGNATURE_LEN) {
      return;
    }
    const sig = sigBuf.readUInt32LE(0);

    if (sig === CENTRAL || sig === EOCD) {
      return;
    }
    if (sig !== LOCAL) {
      throw new Error(`bad zip signature 0x${sig.toString(16)}`);
    }

    const header = await readExact(pull, LOCAL_FILE_HEADER_AFTER_SIG_LEN, "local file header");
    const flags = header.readUInt16LE(2);
    const method = header.readUInt16LE(4);
    const modTime = header.readUInt16LE(6);
    const crc = header.readUInt32LE(10);
    const compressedSize = header.readUInt32LE(14);
    const uncompressedSize = header.readUInt32LE(18);
    const nameLen = header.readUInt16LE(22);
    const extraLen = header.readUInt16LE(24);

    const nameBuf = await readExact(pull, nameLen, "file name");
    const extra = extraLen > 0 ? await readExact(pull, extraLen, "extra field") : Buffer.alloc(0);
    const utf8 = Boolean(flags & ZIP_FLAG_UTF8);
    const name = nameBuf.toString(utf8 ? "utf8" : "latin1");

    if (method === ZIP_AES_METHOD) {
      throw new Error(`AES zip not supported: ${name}`);
    }
    const extraIds = extraFieldIds(extra, name);

    if (extraIds.has(AES_EXTRA)) {
      throw new Error(`AES zip not supported: ${name}`);
    }
    if (
      extraIds.has(ZIP64_EXTRA) ||
      compressedSize === ZIP64_SIZE ||
      uncompressedSize === ZIP64_SIZE
    ) {
      throw new Error(`Zip64 not supported: ${name}`);
    }
    if (method !== 0 && method !== ZIP_METHOD_DEFLATE) {
      throw new Error(`unsupported compression method ${method}: ${name}`);
    }

    const encrypted = Boolean(flags & ZIP_FLAG_ENCRYPTED);
    const dataDescriptor = Boolean(flags & ZIP_FLAG_DATA_DESCRIPTOR);

    if (dataDescriptor && compressedSize === 0) {
      throw new Error(`no size in local header: ${name}`);
    }

    // Only "/" marks a directory in the spec. A trailing "\" is accepted too,
    // but never for an entry that carries data — that body must not be dropped.
    const bodyBytes = encrypted
      ? Math.max(compressedSize - ZIPCRYPTO_HEADER_LEN, 0)
      : compressedSize;
    const type: "File" | "Directory" =
      name.endsWith("/") || (name.endsWith("\\") && bodyBytes === 0) ? "Directory" : "File";

    const cursor: BodyCursor = { remaining: compressedSize };
    let crypto: ZipCrypto | undefined;

    if (encrypted) {
      if (password === undefined) {
        throw new Error(`password required: ${name}`);
      }
      if (cursor.remaining < ZIPCRYPTO_HEADER_LEN) {
        throw new Error(`truncated zip crypto header: ${name}`);
      }
      crypto = createZipCrypto(password);
      await skipEncryptedHeader(pull, crypto, flags, crc, modTime, name);
      cursor.remaining -= ZIPCRYPTO_HEADER_LEN;
    }

    if (options.filter && !options.filter(name)) {
      await skipRemaining(pull, cursor.remaining, dataDescriptor, name);
      continue;
    }

    const entry = new ZipEntryStream(
      {
        compressedSize,
        encrypted,
        path: name,
        type,
        uncompressedSize,
      },
      { highWaterMark: PULL_CHUNK_SIZE },
    );

    let settleDone: (() => void) | undefined;
    let settleFail: ((err: Error) => void) | undefined;
    let settled = false;
    const done = new Promise<void>((resolve, reject) => {
      settleDone = resolve;
      settleFail = reject;
    });

    void done.catch(() => {
      /* awaited after yield; prevent unhandled rejection if the consumer already failed */
    });
    const resolveDone = (): void => {
      if (!settled) {
        settled = true;
        settleDone?.();
      }
    };
    const rejectDone = (err: Error): void => {
      if (!settled) {
        settled = true;
        settleFail?.(err);
      }
    };

    const bodyOptions: BodyOptions = {
      crypto,
      expectedCrc: dataDescriptor && crc === 0 ? undefined : crc,
      expectedSize: dataDescriptor && uncompressedSize === 0 ? undefined : uncompressedSize,
      maxEntrySize: options.maxEntrySize,
      method,
    };

    let descriptorSkipped = false;
    let bodyDone = false;
    let started = false;

    const skipRest = async (): Promise<void> => {
      await skipRemaining(pull, cursor.remaining, dataDescriptor && !descriptorSkipped, name);
      cursor.remaining = 0;
      descriptorSkipped = dataDescriptor;
    };

    const consume = async (): Promise<void> => {
      try {
        const body = await pumpBody(entry, pull, cursor, bodyOptions);

        bodyDone = true;
        if (dataDescriptor) {
          const descriptor = await readDataDescriptor(pull, name);

          descriptorSkipped = true;
          assertSize(descriptor.compressedSize, compressedSize, name);
          assertSize(descriptor.uncompressedSize, body.size, name);
          assertCrc(body.crc, descriptor.crc, name);
        }
        resolveDone();
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        // A finished body that fails later (bad descriptor) is a real error, even
        // though `autoDestroy` already closed the entry without an error.
        const abortedByConsumer = !bodyDone && entry.destroyed && !entry.errored;

        try {
          await skipRest();
        } catch {
          /* keep the original error */
        }
        if (!entry.destroyed) {
          entry.destroy(error);
        }
        if (abortedByConsumer) {
          resolveDone();
        } else {
          rejectDone(error);
        }
      }
    };

    const start = (): void => {
      if (started) {
        return;
      }
      started = true;
      void consume();
    };

    entry.onDemand = start;

    // `destroy()` on an unread entry never triggers `_read`, so the body would
    // stay in the source and the generator would wait forever.
    entry.once("close", () => {
      if (started || settled || bodyDone) {
        return;
      }
      started = true;
      void skipRest().then(resolveDone, rejectDone);
    });

    yield entry;
    await done;
  }
}

/**
 * One-pass unzip of a Node Readable: local headers only, ZipCrypto password
 * zips. Does not seek or buffer the archive.
 */
export async function* unzipEncrypted(
  source: Readable,
  options: UnzipOptions,
): AsyncGenerator<ZipEntry, void, unknown> {
  const pull = createPull(source);

  try {
    yield* parseEntries(pull, options);
  } finally {
    await pull.dispose();
  }
}
