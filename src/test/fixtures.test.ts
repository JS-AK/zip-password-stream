import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import path from "node:path";

import { describe, expect, it } from "vitest";

import {
  FIXTURE_FILES,
  writeAesExtraStub,
  writeAesMethodStub,
  writeZip,
} from "./test-support/write-zip.js";
import { LOCAL, ZIPCRYPTO_HEADER_LEN, ZIP_FLAG_ENCRYPTED } from "../lib/zip/constants.js";
import { findPython, runPython } from "./test-support/python.js";

const hasPython = findPython() !== null;

/**
 * Walk up from this spec until a package.json that sits beside `src/`.
 * `build/esm/package.json` is only `{ type: "module" }` and must be skipped so
 * compiled Vitest still finds checked-in zips under `src/test/fixtures`.
 */
function findRepoRoot(fromDir: string): string {
  let dir = fromDir;

  while (true) {
    if (existsSync(path.join(dir, "package.json")) && existsSync(path.join(dir, "src"))) {
      return dir;
    }

    const parent = path.dirname(dir);

    if (parent === dir) {
      throw new Error(`package.json not found walking up from ${fromDir}`);
    }

    dir = parent;
  }
}

const fixturesDir = path.join(
  findRepoRoot(path.dirname(fileURLToPath(import.meta.url))),
  "src",
  "test",
  "fixtures",
);

function ensureFixtures(): { encrypted: Buffer; plain: Buffer } {
  mkdirSync(fixturesDir, { recursive: true });
  const plain = writeZip(FIXTURE_FILES);
  const encrypted = writeZip(FIXTURE_FILES, { password: "secret" });

  writeFileSync(path.join(fixturesDir, "plain.zip"), plain);
  writeFileSync(path.join(fixturesDir, "encrypted-secret.zip"), encrypted);
  writeFileSync(path.join(fixturesDir, "aes-method99.zip"), writeAesMethodStub("secret.pdf"));
  writeFileSync(path.join(fixturesDir, "aes-extra.zip"), writeAesExtraStub("secret.pdf"));

  return { encrypted, plain };
}

describe("fixtures", () => {
  it("writes tiny ZipCrypto and control zips with known local sizes", () => {
    const { encrypted, plain } = ensureFixtures();

    expect(plain.readUInt32LE(0)).toBe(LOCAL);
    expect(encrypted.readUInt32LE(0)).toBe(LOCAL);
    const encFlags = encrypted.readUInt16LE(6);

    expect(encFlags & ZIP_FLAG_ENCRYPTED).toBe(ZIP_FLAG_ENCRYPTED);
    const encComp = encrypted.readUInt32LE(18);

    expect(encComp).toBeGreaterThan(ZIPCRYPTO_HEADER_LEN);
    expect(existsSync(path.join(fixturesDir, "README.md"))).toBe(true);
  });

  it.skipIf(!hasPython)("Python zipfile can extract the encrypted fixture with pwd=secret", () => {
    ensureFixtures();
    const zipPath = path.join(fixturesDir, "encrypted-secret.zip");
    const script = `
import zipfile, sys
from pathlib import Path
p = Path(sys.argv[1])
with zipfile.ZipFile(p) as zf:
    assert zf.read("a.txt", pwd=b"secret") == b"hello\\n"
    pdf = zf.read("nested/b.pdf", pwd=b"secret")
    assert pdf.startswith(b"%PDF-")
    assert zf.read("c.bin", pwd=b"secret") == bytes([0, 1, 2])
print("ok")
`;
    const result = runPython(script, [zipPath]);

    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain("ok");
  });

  it("checked-in fixtures stay readable as zip signatures", () => {
    ensureFixtures();
    const encrypted = readFileSync(path.join(fixturesDir, "encrypted-secret.zip"));

    expect(encrypted.subarray(0, 4).equals(Buffer.from("PK\x03\x04"))).toBe(true);
  });
});

describe("fixture buffers", () => {
  it("are valid Node Readable sources", () => {
    const { plain } = ensureFixtures();
    const src = Readable.from([plain]);

    expect(src.readable).toBe(true);
  });
});
