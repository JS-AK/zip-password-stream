import { crc32, deflateRawSync } from "node:zlib";
import { createWriteStream } from "node:fs";
import { finished } from "node:stream/promises";

import {
  AES_EXTRA,
  CENTRAL,
  CENTRAL_FILE_HEADER_LEN,
  DATA_DESCRIPTOR_NO_SIG_LEN,
  DATA_DESCRIPTOR_WITH_SIG_LEN,
  DD_SIG,
  DOS_DATE_DEFAULT,
  EOCD,
  EOCD_LEN,
  LOCAL,
  LOCAL_FILE_HEADER_LEN,
  ZIP64_SIZE,
  ZIPCRYPTO_HEADER_LEN,
  ZIP_AES_METHOD,
  ZIP_FLAG_DATA_DESCRIPTOR,
  ZIP_FLAG_ENCRYPTED,
  ZIP_FLAG_UTF8,
  ZIP_METHOD_DEFLATE,
  ZIP_VERSION_NEEDED,
} from "../../lib/zip/constants.js";
import { createZipCrypto, expectedCheckByte } from "../../lib/zip/crypto.js";

/** Writer knobs for password, UTF-8 names, and data-descriptor layouts. */
export type FixtureZipOptions = {
  password?: string;
  passwordEncoding?: BufferEncoding;
  utf8?: boolean;
  dataDescriptor?: "none" | "12" | "16";
  /** Write a wrong crc/size into the data descriptor to test validation. */
  corruptDescriptor?: "crc" | "uncompressedSize" | "compressedSize";
  /** Leave the local header crc at 0, as streaming writers do with bit 3. */
  zeroLocalCrc?: boolean;
  /**
   * Zero local-header crc, compressedSize, and uncompressedSize.
   * Only applies when `dataDescriptor` is `"12"` or `"16"` (bit 3 stays set).
   * The payload and data descriptor still carry real crc/sizes; the CD does too.
   */
  omitLocalSizes?: boolean;
};

/** One file (or directory) to pack into a test zip. */
export type ZipWriteFile = {
  name: string;
  data: Buffer;
  method?: 0 | 8;
  directory?: boolean;
  modTime?: number;
  modDate?: number;
};

/** AE-1 vendor version stored in a WinZip AES extra payload. */
const AES_EXTRA_AE1 = 0x0001;
/** Claimed extra payload size larger than the buffer, so the parser must reject it. */
const MALFORMED_EXTRA_CLAIMED_SIZE = 20;
const MALFORMED_EXTRA_ID = 0x7777;
const TINY_PDF_BODY = Buffer.from("%PDF-1.1\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n");
/** Minimal JPEG SOI used as a `.jpg` entry body in tests. */
const TINY_JPEG_BODY = Buffer.from([0xff, 0xd8, 0xff, 0x00]);

/** Shared three-file archive used by parser, fixture, and README tests. */
export const FIXTURE_FILES: ZipWriteFile[] = [
  { data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" },
  { data: TINY_PDF_BODY, method: ZIP_METHOD_DEFLATE, name: "nested/b.pdf" },
  { data: Buffer.from([0x00, 0x01, 0x02]), method: 0, name: "c.bin" },
];

/** Tiny JPEG bytes used as a `.jpg` zip entry body. */
export const TINY_JPEG = TINY_JPEG_BODY;

/** Tiny PDF bytes used as a nested zip entry and as `isPdfMagic` input. */
export const TINY_PDF = TINY_PDF_BODY;

type WrittenEntry = {
  nameBuf: Buffer;
  flags: number;
  method: number;
  modTime: number;
  modDate: number;
  crc: number;
  compressedSize: number;
  uncompressedSize: number;
  localOffset: number;
};

function isDirectoryName(name: string, directory?: boolean): boolean {
  return Boolean(directory || /[/\\]$/.test(name));
}

/** Local header with a WinZip AES extra field (method still deflate). */
export function writeAesExtraStub(name: string): Buffer {
  const extra = Buffer.alloc(6);

  extra.writeUInt16LE(AES_EXTRA, 0);
  extra.writeUInt16LE(2, 2);
  extra.writeUInt16LE(AES_EXTRA_AE1, 4);

  return writeLocalHeaderStub({
    extra,
    flags: ZIP_FLAG_UTF8,
    method: ZIP_METHOD_DEFLATE,
    name,
  });
}

/** Local header that claims AES compression method 99. */
export function writeAesMethodStub(name: string): Buffer {
  return writeLocalHeaderStub({ method: ZIP_AES_METHOD, name });
}

/** Bit 3 set with zero sizes — v1 must throw rather than scan for a descriptor. */
export function writeDataDescriptorNoSizeStub(name: string): Buffer {
  return writeLocalHeaderStub({
    compressedSize: 0,
    flags: ZIP_FLAG_DATA_DESCRIPTOR,
    method: 0,
    name,
    uncompressedSize: 0,
  });
}

/** Stream a stored unencrypted zip to disk without buffering the file payload. */
export async function writeLargeStoredZipFile(
  filePath: string,
  name: string,
  size: number,
  fill = 0x61,
): Promise<void> {
  const nameBuf = Buffer.from(name, "utf8");
  const chunk = Buffer.alloc(64 * 1024, fill);
  let crc = 0;
  let remaining = size;

  while (remaining > 0) {
    const n = Math.min(remaining, chunk.length);

    crc = crc32(chunk.subarray(0, n), crc) >>> 0;
    remaining -= n;
  }

  const flags = ZIP_FLAG_UTF8;
  const local = Buffer.alloc(LOCAL_FILE_HEADER_LEN);

  local.writeUInt32LE(LOCAL, 0);
  local.writeUInt16LE(ZIP_VERSION_NEEDED, 4);
  local.writeUInt16LE(flags, 6);
  local.writeUInt16LE(0, 8);
  local.writeUInt16LE(0, 10);
  local.writeUInt16LE(DOS_DATE_DEFAULT, 12);
  local.writeUInt32LE(crc, 14);
  local.writeUInt32LE(size, 18);
  local.writeUInt32LE(size, 22);
  local.writeUInt16LE(nameBuf.length, 26);
  local.writeUInt16LE(0, 28);

  const out = createWriteStream(filePath);
  const write = async (buf: Buffer): Promise<void> => {
    if (!out.write(buf)) {
      await new Promise<void>((resolve) => {
        out.once("drain", resolve);
      });
    }
  };

  await write(local);
  await write(nameBuf);
  remaining = size;
  while (remaining > 0) {
    const n = Math.min(remaining, chunk.length);

    await write(chunk.subarray(0, n));
    remaining -= n;
  }

  const localOffset = 0;
  const cdOffset = LOCAL_FILE_HEADER_LEN + nameBuf.length + size;
  const cd = Buffer.alloc(CENTRAL_FILE_HEADER_LEN);

  cd.writeUInt32LE(CENTRAL, 0);
  cd.writeUInt16LE(ZIP_VERSION_NEEDED, 4);
  cd.writeUInt16LE(ZIP_VERSION_NEEDED, 6);
  cd.writeUInt16LE(flags, 8);
  cd.writeUInt16LE(0, 10);
  cd.writeUInt16LE(0, 12);
  cd.writeUInt16LE(DOS_DATE_DEFAULT, 14);
  cd.writeUInt32LE(crc, 16);
  cd.writeUInt32LE(size, 20);
  cd.writeUInt32LE(size, 22);
  cd.writeUInt16LE(nameBuf.length, 28);
  cd.writeUInt16LE(0, 30);
  cd.writeUInt16LE(0, 32);
  cd.writeUInt16LE(0, 34);
  cd.writeUInt16LE(0, 36);
  cd.writeUInt32LE(0, 38);
  cd.writeUInt32LE(localOffset, 42);

  const eocd = Buffer.alloc(EOCD_LEN);

  eocd.writeUInt32LE(EOCD, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(1, 8);
  eocd.writeUInt16LE(1, 10);
  eocd.writeUInt32LE(CENTRAL_FILE_HEADER_LEN + nameBuf.length, 12);
  eocd.writeUInt32LE(cdOffset, 16);
  eocd.writeUInt16LE(0, 20);

  await write(cd);
  await write(nameBuf);
  await write(eocd);
  out.end();
  await finished(out);
}

/** Local file header bytes only (no EOCD) — for stitching a truncated/bad body. */
export function writeLocalHeaderOnly(options: {
  name: string;
  method: number;
  flags?: number;
  crc?: number;
  compressedSize?: number;
  uncompressedSize?: number;
  extra?: Buffer;
}): Buffer {
  const nameBuf = Buffer.from(options.name, "utf8");
  const extra = options.extra ?? Buffer.alloc(0);
  const local = Buffer.alloc(LOCAL_FILE_HEADER_LEN);

  local.writeUInt32LE(LOCAL, 0);
  local.writeUInt16LE(ZIP_VERSION_NEEDED, 4);
  local.writeUInt16LE(options.flags ?? ZIP_FLAG_UTF8, 6);
  local.writeUInt16LE(options.method, 8);
  local.writeUInt16LE(0, 10);
  local.writeUInt16LE(DOS_DATE_DEFAULT, 12);
  local.writeUInt32LE(options.crc ?? 0, 14);
  local.writeUInt32LE(options.compressedSize ?? 0, 18);
  local.writeUInt32LE(options.uncompressedSize ?? 0, 22);
  local.writeUInt16LE(nameBuf.length, 26);
  local.writeUInt16LE(extra.length, 28);

  return Buffer.concat([local, nameBuf, extra]);
}

/** Local header plus EOCD so the parser stops after one throw-case entry. */
export function writeLocalHeaderStub(options: {
  name: string;
  method: number;
  flags?: number;
  compressedSize?: number;
  uncompressedSize?: number;
  extra?: Buffer;
}): Buffer {
  const eocd = Buffer.alloc(EOCD_LEN);

  eocd.writeUInt32LE(EOCD, 0);

  return Buffer.concat([writeLocalHeaderOnly(options), eocd]);
}

/** Extra field whose claimed size runs past the extra blob. */
export function writeMalformedExtraStub(name: string): Buffer {
  const extra = Buffer.alloc(8);

  extra.writeUInt16LE(MALFORMED_EXTRA_ID, 0);
  extra.writeUInt16LE(MALFORMED_EXTRA_CLAIMED_SIZE, 2);
  extra.writeUInt16LE(AES_EXTRA, 4);

  return writeLocalHeaderStub({
    extra,
    flags: ZIP_FLAG_UTF8,
    method: ZIP_METHOD_DEFLATE,
    name,
  });
}

/** Build a ZipCrypto or plain zip in memory for tests. */
export function writeZip(files: ZipWriteFile[], options: FixtureZipOptions = {}): Buffer {
  const utf8 = options.utf8 !== false;
  const parts: Buffer[] = [];
  const entries: WrittenEntry[] = [];
  let offset = 0;

  for (const file of files) {
    const directory = isDirectoryName(file.name, file.directory);
    const name = directory ? file.name.replaceAll("\\", "/").replace(/\/?$/, "/") : file.name;
    const nameBuf = Buffer.from(name, utf8 ? "utf8" : "latin1");
    const data = directory ? Buffer.alloc(0) : file.data;
    const method = file.method ?? (data.length === 0 ? 0 : ZIP_METHOD_DEFLATE);
    const payload = method === ZIP_METHOD_DEFLATE && data.length > 0 ? deflateRawSync(data) : data;
    const crc = (crc32(data) >>> 0) as number;
    const modTime = file.modTime ?? 0;
    const modDate = file.modDate ?? DOS_DATE_DEFAULT;

    let flags = 0;

    if (utf8) {
      flags |= ZIP_FLAG_UTF8;
    }
    if (options.password) {
      flags |= ZIP_FLAG_ENCRYPTED;
    }
    if (options.dataDescriptor && options.dataDescriptor !== "none") {
      flags |= ZIP_FLAG_DATA_DESCRIPTOR;
    }

    let body: Buffer;

    if (options.password) {
      const pwd = Buffer.from(options.password, options.passwordEncoding ?? "utf8");
      const crypto = createZipCrypto(pwd);
      const check = expectedCheckByte(flags, crc, modTime);
      const headerPlain = Buffer.alloc(ZIPCRYPTO_HEADER_LEN, 0xa5);

      headerPlain[ZIPCRYPTO_HEADER_LEN - 1] = check;
      const headerCipher = crypto.encrypt(headerPlain);
      const dataCipher = payload.length > 0 ? crypto.encrypt(payload) : Buffer.alloc(0);

      body = Buffer.concat([headerCipher, dataCipher]);
    } else {
      body = payload;
    }

    const omitLocalSizes =
      Boolean(options.omitLocalSizes) &&
      (options.dataDescriptor === "12" || options.dataDescriptor === "16");
    const local = Buffer.alloc(LOCAL_FILE_HEADER_LEN);

    local.writeUInt32LE(LOCAL, 0);
    local.writeUInt16LE(ZIP_VERSION_NEEDED, 4);
    local.writeUInt16LE(flags, 6);
    local.writeUInt16LE(method, 8);
    local.writeUInt16LE(modTime, 10);
    local.writeUInt16LE(modDate, 12);
    local.writeUInt32LE(omitLocalSizes || options.zeroLocalCrc ? 0 : crc, 14);
    local.writeUInt32LE(omitLocalSizes ? 0 : body.length, 18);
    local.writeUInt32LE(omitLocalSizes ? 0 : data.length, 22);
    local.writeUInt16LE(nameBuf.length, 26);
    local.writeUInt16LE(0, 28);

    entries.push({
      compressedSize: body.length,
      crc,
      flags,
      localOffset: offset,
      method,
      modDate,
      modTime,
      nameBuf,
      uncompressedSize: data.length,
    });

    parts.push(local, nameBuf, body);
    offset += local.length + nameBuf.length + body.length;

    const corrupt = options.corruptDescriptor;
    const ddCrc = corrupt === "crc" ? (crc ^ 0xffff) >>> 0 : crc;
    const ddCompressed = corrupt === "compressedSize" ? body.length + 1 : body.length;
    const ddUncompressed = corrupt === "uncompressedSize" ? data.length + 1 : data.length;

    if (options.dataDescriptor === "16") {
      const dd = Buffer.alloc(DATA_DESCRIPTOR_WITH_SIG_LEN);

      dd.writeUInt32LE(DD_SIG, 0);
      dd.writeUInt32LE(ddCrc, 4);
      dd.writeUInt32LE(ddCompressed, 8);
      dd.writeUInt32LE(ddUncompressed, 12);
      parts.push(dd);
      offset += DATA_DESCRIPTOR_WITH_SIG_LEN;
    } else if (options.dataDescriptor === "12") {
      const dd = Buffer.alloc(DATA_DESCRIPTOR_NO_SIG_LEN);

      dd.writeUInt32LE(ddCrc, 0);
      dd.writeUInt32LE(ddCompressed, 4);
      dd.writeUInt32LE(ddUncompressed, 8);
      parts.push(dd);
      offset += DATA_DESCRIPTOR_NO_SIG_LEN;
    }
  }

  const cdOffset = offset;

  for (const entry of entries) {
    const cd = Buffer.alloc(CENTRAL_FILE_HEADER_LEN);

    cd.writeUInt32LE(CENTRAL, 0);
    cd.writeUInt16LE(ZIP_VERSION_NEEDED, 4);
    cd.writeUInt16LE(ZIP_VERSION_NEEDED, 6);
    cd.writeUInt16LE(entry.flags, 8);
    cd.writeUInt16LE(entry.method, 10);
    cd.writeUInt16LE(entry.modTime, 12);
    cd.writeUInt16LE(entry.modDate, 14);
    cd.writeUInt32LE(entry.crc, 16);
    cd.writeUInt32LE(entry.compressedSize, 20);
    cd.writeUInt32LE(entry.uncompressedSize, 24);
    cd.writeUInt16LE(entry.nameBuf.length, 28);
    cd.writeUInt16LE(0, 30);
    cd.writeUInt16LE(0, 32);
    cd.writeUInt16LE(0, 34);
    cd.writeUInt16LE(0, 36);
    cd.writeUInt32LE(0, 38);
    cd.writeUInt32LE(entry.localOffset, 42);
    parts.push(cd, entry.nameBuf);
    offset += CENTRAL_FILE_HEADER_LEN + entry.nameBuf.length;
  }

  const eocd = Buffer.alloc(EOCD_LEN);

  eocd.writeUInt32LE(EOCD, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(entries.length, 8);
  eocd.writeUInt16LE(entries.length, 10);
  eocd.writeUInt32LE(offset - cdOffset, 12);
  eocd.writeUInt32LE(cdOffset, 16);
  eocd.writeUInt16LE(0, 20);
  parts.push(eocd);

  return Buffer.concat(parts);
}

/** Local header with Zip64 0xffffffff sizes so the parser must reject it. */
export function writeZip64SizeStub(name: string): Buffer {
  return writeLocalHeaderStub({
    compressedSize: ZIP64_SIZE,
    flags: ZIP_FLAG_UTF8,
    method: 0,
    name,
    uncompressedSize: ZIP64_SIZE,
  });
}
