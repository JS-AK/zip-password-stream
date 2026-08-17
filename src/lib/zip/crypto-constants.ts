/** IEEE CRC32 init/final XOR; unwrap so ZipCrypto matches PKZIP raw CRC32. */
export const CRC32_IEEE_XOR = 0xffffffff;
/** ZipCrypto key0 seed (APPNOTE 6.1). */
export const ZIPCRYPTO_KEY0_INIT = 0x12345678;
/** ZipCrypto key1 seed (APPNOTE 6.1). */
export const ZIPCRYPTO_KEY1_INIT = 0x23456789;
/** Multiplier in the ZipCrypto `key1` update (APPNOTE 6.1). */
export const ZIPCRYPTO_KEY1_MULT = 134775813;
/** ZipCrypto key2 seed (APPNOTE 6.1). */
export const ZIPCRYPTO_KEY2_INIT = 0x34567890;
