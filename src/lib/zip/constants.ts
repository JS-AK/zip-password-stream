/** WinZip AES extra-field id; v1 rejects this archive. */
export const AES_EXTRA = 0x9901;
/** Central directory file header signature (APPNOTE). */
export const CENTRAL = 0x02014b50;
/** Central directory file header length excluding the file name (APPNOTE). */
export const CENTRAL_FILE_HEADER_LEN = 46;
/** Data descriptor without signature: crc + compressed + uncompressed. */
export const DATA_DESCRIPTOR_NO_SIG_LEN = 12;
/** Data descriptor with optional PK78 signature: 4 + crc + sizes. */
export const DATA_DESCRIPTOR_WITH_SIG_LEN = 16;
/** Optional data-descriptor signature PK78 (APPNOTE). */
export const DD_SIG = 0x08074b50;
/** End of central directory signature (APPNOTE). */
export const EOCD = 0x06054b50;
/** End of central directory record length (APPNOTE). */
export const EOCD_LEN = 22;
/** Extra-field record prefix: id + size (APPNOTE). */
export const EXTRA_FIELD_HEADER_LEN = 4;
/** PKZIP local file header signature (APPNOTE). */
export const LOCAL = 0x04034b50;
/** Local file header after the signature: 30 − 4 (APPNOTE). */
export const LOCAL_FILE_HEADER_AFTER_SIG_LEN = 26;
/** Local file header including the 4-byte signature (APPNOTE). */
export const LOCAL_FILE_HEADER_LEN = 30;
/** Zip64 extra-field id; v1 rejects this archive. */
export const ZIP64_EXTRA = 0x0001;
/** Zip64 sentinel in 32-bit size fields. */
export const ZIP64_SIZE = 0xffffffff;
/** 12-byte encryption header prepended to ZipCrypto ciphertext (APPNOTE 6.1). */
export const ZIPCRYPTO_HEADER_LEN = 12;
/** WinZip AES method; v1 rejects this archive. */
export const ZIP_AES_METHOD = 99;
/** APPNOTE general-purpose bit 3: CRC and sizes follow the payload. */
export const ZIP_FLAG_DATA_DESCRIPTOR = 0x08;
/** General-purpose bit 0: traditional PKZIP encryption. */
export const ZIP_FLAG_ENCRYPTED = 0x01;
/** General-purpose bit 11: UTF-8 file name. */
export const ZIP_FLAG_UTF8 = 0x800;
/** Deflate (`inflateRaw`) compression method. */
export const ZIP_METHOD_DEFLATE = 8;
/** Signature width shared by local/CD/EOCD/data-descriptor records. */
export const ZIP_SIGNATURE_LEN = 4;
/** Version needed to extract (2.0) in local and central headers (APPNOTE). */
export const ZIP_VERSION_NEEDED = 20;
