import { describe, expect, it } from "vitest";

import { createZipCrypto, expectedCheckByte } from "./crypto.js";
import { findPython, runPythonBinary } from "../../test/test-support/python.js";
import { ZIP_FLAG_DATA_DESCRIPTOR } from "./constants.js";

const hasPython = findPython() !== null;

const PYTHON_INPUT = Buffer.from(Array.from({ length: 32 }, (_, i) => i));
const PYTHON_DECRYPTED = Buffer.from(
  "c84ed9bdad8b91daa848363543fb3393fa31a885cf176e8c009a441a83c1fd47",
  "hex",
);

describe("ZipCrypto", () => {
  it("matches Python zipfile._ZipDecrypter on a known vector", () => {
    const crypto = createZipCrypto(Buffer.from("secret"));
    const plain = crypto.decrypt(PYTHON_INPUT);

    expect(plain.equals(PYTHON_DECRYPTED)).toBe(true);
  });

  it("round-trips encrypt then decrypt", () => {
    const password = Buffer.from("secret");
    const payload = Buffer.from("hello zipcrypto\n");
    const cipher = createZipCrypto(password).encrypt(payload);
    const plain = createZipCrypto(password).decrypt(cipher);

    expect(plain.equals(payload)).toBe(true);
  });

  it("resets keys for two sequential sessions with the same password", () => {
    const password = Buffer.from("secret");
    const first = Buffer.from("file-one-body");
    const second = Buffer.from("file-two-body");

    const enc1 = createZipCrypto(password).encrypt(first);
    const enc2 = createZipCrypto(password).encrypt(second);

    const dec1 = createZipCrypto(password);

    expect(dec1.decrypt(enc1).equals(first)).toBe(true);

    const dec2 = createZipCrypto(password);

    expect(dec2.decrypt(enc2).equals(second)).toBe(true);

    const reusedWrong = dec1.decrypt(enc2);

    expect(reusedWrong.equals(second)).toBe(false);
  });

  it("check-byte uses CRC high byte, or modTime when data descriptor bit is set", () => {
    const crc = 0xabcdef01;
    const modTime = 0x1234;

    expect(expectedCheckByte(0, crc, modTime)).toBe(0xab);
    expect(expectedCheckByte(ZIP_FLAG_DATA_DESCRIPTOR, crc, modTime)).toBe(0x12);
  });

  it.skipIf(!hasPython)("cross-checks decrypt against a live Python _ZipDecrypter", () => {
    const password = "secret";
    const cipher = Buffer.from("0123456789abcdef0123456789abcdef");
    const local = createZipCrypto(Buffer.from(password)).decrypt(cipher);

    const script = `
import zipfile, sys
dec = zipfile._ZipDecrypter(sys.argv[1].encode("utf-8"))
cipher = bytes.fromhex(sys.argv[2])
sys.stdout.buffer.write(dec(cipher))
`;
    const result = runPythonBinary(script, [password, cipher.toString("hex")]);

    expect(result.status, String(result.stderr)).toBe(0);
    expect(result.stdout.equals(local)).toBe(true);
  });
});
