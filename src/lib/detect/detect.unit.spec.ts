import { describe, expect, it } from "vitest";

import {
  type DetectKind,
  isBmpMagic,
  isBmpPath,
  isGifMagic,
  isGifPath,
  isGzipMagic,
  isGzipPath,
  isIcoMagic,
  isIcoPath,
  isJpegMagic,
  isJpegPath,
  isKindMagic,
  isKindPath,
  isPdfMagic,
  isPdfPath,
  isPngMagic,
  isPngPath,
  isTiffMagic,
  isTiffPath,
  isWavMagic,
  isWavPath,
  isWebpMagic,
  isWebpPath,
  isXmlMagic,
  isXmlPath,
  isZipMagic,
  isZipPath,
} from "./detect.js";

const HELLO = Buffer.from("hello");

const PNG_MAGIC = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const JPEG_MAGIC = Buffer.from([0xff, 0xd8, 0xff]);
const GIF87A_MAGIC = Buffer.from("GIF87a");
const GIF89A_MAGIC = Buffer.from("GIF89a");
const WEBP_MAGIC = Buffer.concat([Buffer.from("RIFF"), Buffer.alloc(4), Buffer.from("WEBP")]);
const RIFF_NOT_WEBP = Buffer.concat([Buffer.from("RIFF"), Buffer.alloc(4), Buffer.from("XXXX")]);
const ZIP_MAGIC = Buffer.from("PK\x03\x04");
const GZIP_MAGIC = Buffer.from([0x1f, 0x8b]);
const XML_MAGIC = Buffer.from("<?xml");
const BMP_MAGIC = Buffer.from("BM");
const TIFF_II_MAGIC = Buffer.from("II*\0");
const TIFF_MM_MAGIC = Buffer.from("MM\0*");
const ICO_MAGIC = Buffer.from([0x00, 0x00, 0x01, 0x00]);
const WAV_MAGIC = Buffer.concat([Buffer.from("RIFF"), Buffer.alloc(4), Buffer.from("WAVE")]);
const RIFF_NOT_WAVE = Buffer.concat([Buffer.from("RIFF"), Buffer.alloc(4), Buffer.from("WEBP")]);

type KindRow = {
  ext: string;
  isMagic: (chunk: Uint8Array) => boolean;
  isPath: (name: string) => boolean;
  kind: DetectKind;
  magic: Uint8Array;
};

const KIND_ROWS: KindRow[] = [
  {
    ext: ".pdf",
    isMagic: isPdfMagic,
    isPath: isPdfPath,
    kind: "pdf",
    magic: Buffer.from("%PDF-"),
  },
  {
    ext: ".png",
    isMagic: isPngMagic,
    isPath: isPngPath,
    kind: "png",
    magic: PNG_MAGIC,
  },
  {
    ext: ".jpg",
    isMagic: isJpegMagic,
    isPath: isJpegPath,
    kind: "jpeg",
    magic: JPEG_MAGIC,
  },
  {
    ext: ".gif",
    isMagic: isGifMagic,
    isPath: isGifPath,
    kind: "gif",
    magic: GIF89A_MAGIC,
  },
  {
    ext: ".webp",
    isMagic: isWebpMagic,
    isPath: isWebpPath,
    kind: "webp",
    magic: WEBP_MAGIC,
  },
  {
    ext: ".zip",
    isMagic: isZipMagic,
    isPath: isZipPath,
    kind: "zip",
    magic: ZIP_MAGIC,
  },
  {
    ext: ".gz",
    isMagic: isGzipMagic,
    isPath: isGzipPath,
    kind: "gzip",
    magic: GZIP_MAGIC,
  },
  {
    ext: ".xml",
    isMagic: isXmlMagic,
    isPath: isXmlPath,
    kind: "xml",
    magic: XML_MAGIC,
  },
  {
    ext: ".bmp",
    isMagic: isBmpMagic,
    isPath: isBmpPath,
    kind: "bmp",
    magic: BMP_MAGIC,
  },
  {
    ext: ".tif",
    isMagic: isTiffMagic,
    isPath: isTiffPath,
    kind: "tiff",
    magic: TIFF_II_MAGIC,
  },
  {
    ext: ".ico",
    isMagic: isIcoMagic,
    isPath: isIcoPath,
    kind: "ico",
    magic: ICO_MAGIC,
  },
  {
    ext: ".wav",
    isMagic: isWavMagic,
    isPath: isWavPath,
    kind: "wav",
    magic: WAV_MAGIC,
  },
];

describe("isPdfPath", () => {
  it("matches a .pdf file name and ignores directories", () => {
    expect(isPdfPath("nested/b.pdf")).toBe(true);
    expect(isPdfPath("nested\\B.PDF")).toBe(true);
    expect(isPdfPath("a.txt")).toBe(false);
    expect(isPdfPath("pdf")).toBe(false);
  });
});

describe("isPdfMagic", () => {
  it("detects the %PDF- prefix", () => {
    expect(isPdfMagic(Buffer.from("%PDF-1.1\n"))).toBe(true);
    expect(isPdfMagic(Buffer.from("%PDF-"))).toBe(true);
    expect(isPdfMagic(Buffer.from("%PD"))).toBe(false);
    expect(isPdfMagic(Buffer.from("hello"))).toBe(false);
  });
});

describe.each(KIND_ROWS)("$kind helpers", ({ ext, isMagic, isPath, kind, magic }) => {
  it("detects the magic on exact matching bytes", () => {
    expect(isMagic(magic)).toBe(true);
    expect(isKindMagic(kind, magic)).toBe(true);
  });

  it("rejects a too-short buffer and unrelated bytes", () => {
    const truncated = magic.subarray(0, Math.max(0, magic.length - 1));

    expect(isMagic(truncated)).toBe(false);
    expect(isMagic(HELLO)).toBe(false);
  });

  it("matches a nested basename, any case and separators", () => {
    expect(isPath(`dir/file${ext}`)).toBe(true);
    expect(isKindPath(kind, `dir/file${ext}`)).toBe(true);
    expect(isPath(`dir\\FILE${ext.toUpperCase()}`)).toBe(true);
    expect(isPath("dir/file.txt")).toBe(false);
  });
});

describe("isJpegPath", () => {
  it("matches both .jpg and .jpeg", () => {
    expect(isJpegPath("dir/photo.jpg")).toBe(true);
    expect(isJpegPath("dir/photo.jpeg")).toBe(true);
    expect(isJpegPath("dir/photo.JPEG")).toBe(true);
  });
});

describe("isGzipPath", () => {
  it("matches both .gz and .gzip", () => {
    expect(isGzipPath("dir/a.gz")).toBe(true);
    expect(isGzipPath("dir/a.gzip")).toBe(true);
  });
});

describe("isTiffPath", () => {
  it("matches both .tif and .tiff", () => {
    expect(isTiffPath("dir/a.tif")).toBe(true);
    expect(isTiffPath("dir/a.tiff")).toBe(true);
  });
});

describe("isGifMagic", () => {
  it("accepts GIF87a and GIF89a", () => {
    expect(isGifMagic(GIF87A_MAGIC)).toBe(true);
    expect(isGifMagic(GIF89A_MAGIC)).toBe(true);
  });
});

describe("isTiffMagic", () => {
  it("accepts both endian magics", () => {
    expect(isTiffMagic(TIFF_II_MAGIC)).toBe(true);
    expect(isTiffMagic(TIFF_MM_MAGIC)).toBe(true);
  });
});

describe("isWebpMagic", () => {
  it("requires RIFF at 0 and WEBP at offset 8", () => {
    expect(isWebpMagic(WEBP_MAGIC)).toBe(true);
    expect(isWebpMagic(RIFF_NOT_WEBP)).toBe(false);
    expect(isWebpMagic(WAV_MAGIC)).toBe(false);
  });
});

describe("isWavMagic", () => {
  it("requires RIFF at 0 and WAVE at offset 8", () => {
    expect(isWavMagic(WAV_MAGIC)).toBe(true);
    expect(isWavMagic(RIFF_NOT_WAVE)).toBe(false);
  });
});

describe("isKindMagic / isKindPath", () => {
  it("dispatches by kind id", () => {
    expect(isKindMagic("png", PNG_MAGIC)).toBe(true);
    expect(isKindPath("jpeg", "x.JPG")).toBe(true);
  });
});
