import { crc32 } from "node:zlib";

import {
  CRC32_IEEE_XOR,
  ZIPCRYPTO_KEY0_INIT,
  ZIPCRYPTO_KEY1_INIT,
  ZIPCRYPTO_KEY1_MULT,
  ZIPCRYPTO_KEY2_INIT,
} from "./crypto-constants.js";
import { ZIP_FLAG_DATA_DESCRIPTOR } from "./constants.js";

/** Traditional PKZIP encryption (APPNOTE 6.1); one state per zip entry. */
export type ZipCrypto = {
  decrypt(src: Buffer): Buffer;
  encrypt(src: Buffer): Buffer;
};

/**
 * ZipCrypto uses the PKZIP raw CRC32 primitive (no init/final XOR).
 * Node `zlib.crc32` is IEEE CRC32, which is that primitive wrapped in
 * XOR 0xFFFFFFFF — unwrap so keys match Python `zipfile._ZipDecrypter`.
 */
function crcByte(scratch: Buffer, byte: number, key: number): number {
  scratch[0] = byte;

  return (crc32(scratch, (key ^ CRC32_IEEE_XOR) >>> 0) ^ CRC32_IEEE_XOR) >>> 0;
}

/**
 * Fresh ZipCrypto keys for one entry (APPNOTE 6.1): seed, then update each
 * password byte, then each plaintext byte. Reusing state across files mixes keys.
 */
export function createZipCrypto(password: Buffer): ZipCrypto {
  let key0 = ZIPCRYPTO_KEY0_INIT;
  let key1 = ZIPCRYPTO_KEY1_INIT;
  let key2 = ZIPCRYPTO_KEY2_INIT;
  const scratch = Buffer.allocUnsafe(1);

  const update = (byte: number): void => {
    key0 = crcByte(scratch, byte, key0);
    key1 = (Math.imul((key1 + (key0 & 0xff)) >>> 0, ZIPCRYPTO_KEY1_MULT) + 1) >>> 0;
    key2 = crcByte(scratch, key1 >>> 24, key2);
  };

  const decryptByte = (): number => {
    const temp = key2 | 2;

    return (Math.imul(temp, temp ^ 1) >>> 8) & 0xff;
  };

  for (const byte of password) {
    update(byte);
  }

  return {
    decrypt(src: Buffer): Buffer {
      const out = Buffer.allocUnsafe(src.length);

      for (let i = 0; i < src.length; i++) {
        const cipher = src[i] as number;
        const plain = cipher ^ decryptByte();

        out[i] = plain;
        update(plain);
      }

      return out;
    },
    encrypt(src: Buffer): Buffer {
      const out = Buffer.allocUnsafe(src.length);

      for (let i = 0; i < src.length; i++) {
        const plain = src[i] as number;
        const cipher = plain ^ decryptByte();

        out[i] = cipher;
        update(plain);
      }

      return out;
    },
  };
}

/**
 * 12th decrypted encryption-header byte (APPNOTE 6.1): high CRC byte, or high
 * DOS time when bit 3 is set. A match is only 1/256; inflate may still fail.
 */
export function expectedCheckByte(flags: number, crc: number, modTime: number): number {
  if (flags & ZIP_FLAG_DATA_DESCRIPTOR) {
    return (modTime >> 8) & 0xff;
  }

  return (crc >>> 24) & 0xff;
}
