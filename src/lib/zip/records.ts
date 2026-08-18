import {
  CENTRAL,
  CENTRAL_FILE_HEADER_LEN,
  DATA_DESCRIPTOR_WITH_SIG_LEN,
  DD_SIG,
  EOCD,
  EOCD_LEN,
  LOCAL,
  LOCAL_FILE_HEADER_LEN,
  ZIP_METHOD_DEFLATE,
  ZIP_VERSION_NEEDED,
} from "./constants.js";

/** Central-directory metadata kept per entry until `end()`. */
export type CdRecord = {
  compressedSize: number;
  crc: number;
  flags: number;
  localOffset: number;
  method: number;
  modDate: number;
  modTime: number;
  nameBuf: Buffer;
  uncompressedSize: number;
};

/** Central directory file header (APPNOTE); the file name is written after this. */
export function encodeCentralHeader(entry: CdRecord): Buffer {
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

  return cd;
}

/** 16-byte data descriptor with PK78 signature (APPNOTE). */
export function encodeDataDescriptor(
  crc: number,
  compressedSize: number,
  uncompressedSize: number,
): Buffer {
  const dd = Buffer.alloc(DATA_DESCRIPTOR_WITH_SIG_LEN);

  dd.writeUInt32LE(DD_SIG, 0);
  dd.writeUInt32LE(crc, 4);
  dd.writeUInt32LE(compressedSize, 8);
  dd.writeUInt32LE(uncompressedSize, 12);

  return dd;
}

/** End of central directory record (APPNOTE). */
export function encodeEocd(entryCount: number, cdSize: number, cdOffset: number): Buffer {
  const eocd = Buffer.alloc(EOCD_LEN);

  eocd.writeUInt32LE(EOCD, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(entryCount, 8);
  eocd.writeUInt16LE(entryCount, 10);
  eocd.writeUInt32LE(cdSize, 12);
  eocd.writeUInt32LE(cdOffset, 16);
  eocd.writeUInt16LE(0, 20);

  return eocd;
}

/** Local file header with zero crc/sizes (APPNOTE bit 3). */
export function encodeLocalHeader(
  flags: number,
  nameBuf: Buffer,
  modTime: number,
  modDate: number,
): Buffer {
  const local = Buffer.alloc(LOCAL_FILE_HEADER_LEN);

  local.writeUInt32LE(LOCAL, 0);
  local.writeUInt16LE(ZIP_VERSION_NEEDED, 4);
  local.writeUInt16LE(flags, 6);
  local.writeUInt16LE(ZIP_METHOD_DEFLATE, 8);
  local.writeUInt16LE(modTime, 10);
  local.writeUInt16LE(modDate, 12);
  // Bit 3: crc and sizes are 0 here; the data descriptor carries the real values.
  local.writeUInt32LE(0, 14);
  local.writeUInt32LE(0, 18);
  local.writeUInt32LE(0, 22);
  local.writeUInt16LE(nameBuf.length, 26);
  local.writeUInt16LE(0, 28);

  return local;
}
