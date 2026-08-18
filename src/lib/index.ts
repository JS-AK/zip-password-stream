export type { DetectKind } from "./detect/detect.js";
export type { UnzipOptions, ZipEntry } from "./unzip.js";
export type { ZipCreate } from "./zip-api.js";
export type { ZipWriteOptions, ZipWriter } from "./zip-write.js";
export { Zip } from "./zip-api.js";
export {
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
} from "./detect/detect.js";
export { unzipEncrypted } from "./unzip.js";
export { zipEncrypted } from "./zip-write.js";
