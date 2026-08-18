import { describe, expect, it } from "vitest";

import {
  CENTRAL,
  CENTRAL_FILE_HEADER_LEN,
  DATA_DESCRIPTOR_WITH_SIG_LEN,
  DD_SIG,
  DOS_DATE_DEFAULT,
  EOCD,
  EOCD_LEN,
  LOCAL,
  LOCAL_FILE_HEADER_LEN,
  ZIP_FLAG_DATA_DESCRIPTOR,
  ZIP_FLAG_UTF8,
  ZIP_METHOD_DEFLATE,
  ZIP_VERSION_NEEDED,
} from "./constants.js";
import {
  type CdRecord,
  encodeCentralHeader,
  encodeDataDescriptor,
  encodeEocd,
  encodeLocalHeader,
} from "./records.js";

const FLAGS = ZIP_FLAG_UTF8 | ZIP_FLAG_DATA_DESCRIPTOR;
const NAME = Buffer.from("a.txt");

describe("zip record encoders", () => {
  it("encodes a bit-3 local header with zero crc and sizes", () => {
    const local = encodeLocalHeader(FLAGS, NAME, 0, DOS_DATE_DEFAULT);

    expect(local.length).toBe(LOCAL_FILE_HEADER_LEN);
    expect(local.readUInt32LE(0)).toBe(LOCAL);
    expect(local.readUInt16LE(4)).toBe(ZIP_VERSION_NEEDED);
    expect(local.readUInt16LE(6)).toBe(FLAGS);
    expect(local.readUInt16LE(8)).toBe(ZIP_METHOD_DEFLATE);
    expect(local.readUInt16LE(10)).toBe(0);
    expect(local.readUInt16LE(12)).toBe(DOS_DATE_DEFAULT);
    expect(local.readUInt32LE(14)).toBe(0);
    expect(local.readUInt32LE(18)).toBe(0);
    expect(local.readUInt32LE(22)).toBe(0);
    expect(local.readUInt16LE(26)).toBe(NAME.length);
    expect(local.readUInt16LE(28)).toBe(0);
  });

  it("encodes a 16-byte data descriptor with PK78", () => {
    const dd = encodeDataDescriptor(0x12345678, 12, 5);

    expect(dd.length).toBe(DATA_DESCRIPTOR_WITH_SIG_LEN);
    expect(dd.readUInt32LE(0)).toBe(DD_SIG);
    expect(dd.readUInt32LE(4)).toBe(0x12345678);
    expect(dd.readUInt32LE(8)).toBe(12);
    expect(dd.readUInt32LE(12)).toBe(5);
  });

  it("encodes a central-directory header matching the local flags and sizes", () => {
    const entry: CdRecord = {
      compressedSize: 12,
      crc: 0x12345678,
      flags: FLAGS,
      localOffset: 0,
      method: ZIP_METHOD_DEFLATE,
      modDate: DOS_DATE_DEFAULT,
      modTime: 0,
      nameBuf: NAME,
      uncompressedSize: 5,
    };
    const cd = encodeCentralHeader(entry);

    expect(cd.length).toBe(CENTRAL_FILE_HEADER_LEN);
    expect(cd.readUInt32LE(0)).toBe(CENTRAL);
    expect(cd.readUInt16LE(8)).toBe(FLAGS);
    expect(cd.readUInt16LE(10)).toBe(ZIP_METHOD_DEFLATE);
    expect(cd.readUInt32LE(16)).toBe(0x12345678);
    expect(cd.readUInt32LE(20)).toBe(12);
    expect(cd.readUInt32LE(24)).toBe(5);
    expect(cd.readUInt16LE(28)).toBe(NAME.length);
    expect(cd.readUInt32LE(42)).toBe(0);
  });

  it("encodes EOCD with entry count, cd size, and cd offset", () => {
    const eocd = encodeEocd(2, 100, 50);

    expect(eocd.length).toBe(EOCD_LEN);
    expect(eocd.readUInt32LE(0)).toBe(EOCD);
    expect(eocd.readUInt16LE(8)).toBe(2);
    expect(eocd.readUInt16LE(10)).toBe(2);
    expect(eocd.readUInt32LE(12)).toBe(100);
    expect(eocd.readUInt32LE(16)).toBe(50);
  });
});
