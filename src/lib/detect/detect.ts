import { LOCAL, ZIP_SIGNATURE_LEN } from "../zip/constants.js";

/** File kinds this module can recognize from magic bytes or a basename. */
export type DetectKind =
  "bmp" | "gif" | "gzip" | "ico" | "jpeg" | "pdf" | "png" | "tiff" | "wav" | "webp" | "xml" | "zip";

/** One magic sequence at a byte offset; a group matches only if every needle fits. */
type MagicNeedle = {
  bytes: Uint8Array;
  offset: number;
};

/**
 * Per-kind row: basename extensions, and OR of magic groups (AND of needles
 * inside a group). GIF/TIFF endian or version variants are separate groups.
 */
type KindSpec = {
  extensions: readonly string[];
  magicGroups: readonly (readonly MagicNeedle[])[];
};

/** BMP file magic (`BM`) at offset 0. */
const BMP_MAGIC = Buffer.from("BM");

/** GIF87a signature at offset 0. */
const GIF87A_MAGIC = Buffer.from("GIF87a");

/** GIF89a signature at offset 0. */
const GIF89A_MAGIC = Buffer.from("GIF89a");

/** GZIP ID1/ID2 (`1F 8B`) at offset 0. */
const GZIP_MAGIC = Buffer.from([0x1f, 0x8b]);

/** ICO reserved + type (`00 00 01 00`) at offset 0. */
const ICO_MAGIC = Buffer.from([0x00, 0x00, 0x01, 0x00]);

/** JPEG SOI plus the first marker byte (`FF D8 FF`) at offset 0. */
const JPEG_MAGIC = Buffer.from([0xff, 0xd8, 0xff]);

/** PDF file header (`%PDF-`) at offset 0. */
const PDF_MAGIC = Buffer.from("%PDF-");

/** PNG signature (`89 50 4E 47 0D 0A 1A 0A`) at offset 0. */
const PNG_MAGIC = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);

/** RIFF container fourcc at offset 0 (WebP and WAVE). */
const RIFF_MAGIC = Buffer.from("RIFF");

/** Byte offset of the RIFF payload fourcc (WEBP or WAVE). */
const RIFF_FOURCC_OFFSET = 8;

/** TIFF little-endian magic (`II*\0`) at offset 0. */
const TIFF_II_MAGIC = Buffer.from("II*\0");

/** TIFF big-endian magic (`MM\0*`) at offset 0. */
const TIFF_MM_MAGIC = Buffer.from("MM\0*");

/** WAVE fourcc at {@link RIFF_FOURCC_OFFSET} inside a RIFF container. */
const WAVE_FOURCC = Buffer.from("WAVE");

/** WEBP fourcc at {@link RIFF_FOURCC_OFFSET} inside a RIFF container. */
const WEBP_FOURCC = Buffer.from("WEBP");

/** XML declaration prefix (`<?xml`) at offset 0. */
const XML_MAGIC = Buffer.from("<?xml");

/** PKZIP local file header as on-disk bytes (little-endian {@link LOCAL}). */
function zipLocalMagic(): Buffer {
  const bytes = Buffer.alloc(ZIP_SIGNATURE_LEN);

  bytes.writeUInt32LE(LOCAL);

  return bytes;
}

/** On-disk local-file signature (`PK\x03\x04`). */
const ZIP_LOCAL_MAGIC = zipLocalMagic();

const KIND_SPECS: Record<DetectKind, KindSpec> = {
  bmp: {
    extensions: [".bmp"],
    magicGroups: [[{ bytes: BMP_MAGIC, offset: 0 }]],
  },
  gif: {
    extensions: [".gif"],
    magicGroups: [[{ bytes: GIF87A_MAGIC, offset: 0 }], [{ bytes: GIF89A_MAGIC, offset: 0 }]],
  },
  gzip: {
    extensions: [".gz", ".gzip"],
    magicGroups: [[{ bytes: GZIP_MAGIC, offset: 0 }]],
  },
  ico: {
    extensions: [".ico"],
    magicGroups: [[{ bytes: ICO_MAGIC, offset: 0 }]],
  },
  jpeg: {
    extensions: [".jpg", ".jpeg"],
    magicGroups: [[{ bytes: JPEG_MAGIC, offset: 0 }]],
  },
  pdf: {
    extensions: [".pdf"],
    magicGroups: [[{ bytes: PDF_MAGIC, offset: 0 }]],
  },
  png: {
    extensions: [".png"],
    magicGroups: [[{ bytes: PNG_MAGIC, offset: 0 }]],
  },
  tiff: {
    extensions: [".tif", ".tiff"],
    magicGroups: [[{ bytes: TIFF_II_MAGIC, offset: 0 }], [{ bytes: TIFF_MM_MAGIC, offset: 0 }]],
  },
  wav: {
    extensions: [".wav"],
    magicGroups: [
      [
        { bytes: RIFF_MAGIC, offset: 0 },
        { bytes: WAVE_FOURCC, offset: RIFF_FOURCC_OFFSET },
      ],
    ],
  },
  webp: {
    extensions: [".webp"],
    magicGroups: [
      [
        { bytes: RIFF_MAGIC, offset: 0 },
        { bytes: WEBP_FOURCC, offset: RIFF_FOURCC_OFFSET },
      ],
    ],
  },
  xml: {
    extensions: [".xml"],
    magicGroups: [[{ bytes: XML_MAGIC, offset: 0 }]],
  },
  zip: {
    extensions: [".zip"],
    magicGroups: [[{ bytes: ZIP_LOCAL_MAGIC, offset: 0 }]],
  },
};

/**
 * True when `bytes` appears in `chunk` at `offset` and the chunk is long enough.
 */
function matchesNeedle(chunk: Uint8Array, needle: MagicNeedle): boolean {
  const { bytes, offset } = needle;

  if (chunk.length < offset + bytes.length) {
    return false;
  }

  for (let i = 0; i < bytes.length; i++) {
    if (chunk[offset + i] !== bytes[i]) {
      return false;
    }
  }

  return true;
}

/** A magic group is AND of needles (e.g. RIFF @0 and WEBP @8). */
function matchesGroup(chunk: Uint8Array, group: readonly MagicNeedle[]): boolean {
  return group.every((needle) => matchesNeedle(chunk, needle));
}

function matchMagic(kind: DetectKind, chunk: Uint8Array): boolean {
  return KIND_SPECS[kind].magicGroups.some((group) => matchesGroup(chunk, group));
}

function matchPath(kind: DetectKind, name: string): boolean {
  const normalized = name.replaceAll("\\", "/");
  const base = normalized.slice(normalized.lastIndexOf("/") + 1).toLowerCase();

  return KIND_SPECS[kind].extensions.some((ext) => base.endsWith(ext));
}

function bindMagic(kind: DetectKind): (chunk: Uint8Array) => boolean {
  return (chunk: Uint8Array): boolean => matchMagic(kind, chunk);
}

function bindPath(kind: DetectKind): (name: string) => boolean {
  return (name: string): boolean => matchPath(kind, name);
}

/** True when the first bytes are a BMP (`BM`). */
export const isBmpMagic: (chunk: Uint8Array) => boolean = bindMagic("bmp");

/** True when the zip entry name looks like a BMP (basename, any case). */
export const isBmpPath: (name: string) => boolean = bindPath("bmp");

/** True when the first bytes are GIF87a or GIF89a. */
export const isGifMagic: (chunk: Uint8Array) => boolean = bindMagic("gif");

/** True when the zip entry name looks like a GIF (basename, any case). */
export const isGifPath: (name: string) => boolean = bindPath("gif");

/** True when the first bytes are gzip (`1F 8B`). */
export const isGzipMagic: (chunk: Uint8Array) => boolean = bindMagic("gzip");

/** True when the zip entry name looks like gzip (`.gz` / `.gzip`). */
export const isGzipPath: (name: string) => boolean = bindPath("gzip");

/** True when the first bytes are an ICO (`00 00 01 00`). */
export const isIcoMagic: (chunk: Uint8Array) => boolean = bindMagic("ico");

/** True when the zip entry name looks like an ICO (basename, any case). */
export const isIcoPath: (name: string) => boolean = bindPath("ico");

/** True when the first bytes are a JPEG (`FF D8 FF`). */
export const isJpegMagic: (chunk: Uint8Array) => boolean = bindMagic("jpeg");

/** True when the zip entry name looks like a JPEG (`.jpg` / `.jpeg`). */
export const isJpegPath: (name: string) => boolean = bindPath("jpeg");

/** True when `chunk` matches the magic for `kind` (any one group). */
export const isKindMagic: (kind: DetectKind, chunk: Uint8Array) => boolean = matchMagic;

/** True when the zip entry basename matches an extension for `kind`. */
export const isKindPath: (kind: DetectKind, name: string) => boolean = matchPath;

/** True when the first bytes are the PDF file header (`%PDF-`). */
export const isPdfMagic: (chunk: Uint8Array) => boolean = bindMagic("pdf");

/** True when the zip entry name looks like a PDF (basename, any case). */
export const isPdfPath: (name: string) => boolean = bindPath("pdf");

/** True when the first bytes are a PNG signature. */
export const isPngMagic: (chunk: Uint8Array) => boolean = bindMagic("png");

/** True when the zip entry name looks like a PNG (basename, any case). */
export const isPngPath: (name: string) => boolean = bindPath("png");

/** True when the first bytes are TIFF little-endian or big-endian magic. */
export const isTiffMagic: (chunk: Uint8Array) => boolean = bindMagic("tiff");

/** True when the zip entry name looks like a TIFF (`.tif` / `.tiff`). */
export const isTiffPath: (name: string) => boolean = bindPath("tiff");

/** True when the first bytes are RIFF at 0 and WAVE at offset 8. */
export const isWavMagic: (chunk: Uint8Array) => boolean = bindMagic("wav");

/** True when the zip entry name looks like a WAVE (basename, any case). */
export const isWavPath: (name: string) => boolean = bindPath("wav");

/** True when the first bytes are RIFF at 0 and WEBP at offset 8. */
export const isWebpMagic: (chunk: Uint8Array) => boolean = bindMagic("webp");

/** True when the zip entry name looks like a WebP (basename, any case). */
export const isWebpPath: (name: string) => boolean = bindPath("webp");

/** True when the first bytes are an XML declaration (`<?xml`). */
export const isXmlMagic: (chunk: Uint8Array) => boolean = bindMagic("xml");

/** True when the zip entry name looks like XML (basename, any case). */
export const isXmlPath: (name: string) => boolean = bindPath("xml");

/** True when the first bytes are a PKZIP local file header. */
export const isZipMagic: (chunk: Uint8Array) => boolean = bindMagic("zip");

/** True when the zip entry name looks like a zip (basename, any case). */
export const isZipPath: (name: string) => boolean = bindPath("zip");
