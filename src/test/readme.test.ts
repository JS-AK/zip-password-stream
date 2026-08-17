import { fileURLToPath } from "node:url";
import path from "node:path";
import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

/** Compiled specs live in `build/esm/test`; three hops reach the repo root. */
const readme = readFileSync(
  path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "README.md"),
  "utf8",
);

describe("README", () => {
  it("documents HTTP/file + password + PDF-only + basename + limits", () => {
    expect(readme).toMatch(/unzipEncrypted/);
    expect(readme).toMatch(/password/);
    expect(readme).toMatch(/\.pdf/i);
    expect(readme).toMatch(/path\.basename/);
    expect(readme).toMatch(/pipeline/);
    expect(readme).toMatch(/AES/);
    expect(readme).toMatch(/data descriptor/i);
    expect(readme).toMatch(/Zip64/);
    expect(readme).toMatch(/Node/);
    expect(readme).toMatch(/autodrain/);
    expect(readme).toMatch(/passwordEncoding/);
    expect(readme).toMatch(/filter/);
    expect(readme).toMatch(/isPdfMagic/);
    expect(readme).toMatch(/maxEntrySize/);
    expect(readme).toMatch(/entry\.destroy\(\)/);
  });
});
