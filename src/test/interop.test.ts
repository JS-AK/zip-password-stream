import { mkdtemp, rm } from "node:fs/promises";
import { Readable } from "node:stream";
import { createReadStream } from "node:fs";
import os from "node:os";
import path from "node:path";

import { describe, expect, it } from "vitest";

import { findPython, runPython } from "./test-support/python.js";
import { unzipEncrypted } from "../lib/unzip.js";

const hasPython = findPython() !== null;

const WRITE_ZIP = `
import sys, zipfile
target = sys.argv[1]
pdf = b"%PDF-1.1\\n1 0 obj<<>>endobj\\ntrailer<<>>\\n%%EOF\\n"
with zipfile.ZipFile(target, "w") as zf:
    zf.writestr("a.txt", "hello\\n", zipfile.ZIP_DEFLATED)
    zf.writestr("d/", b"")
    zf.writestr("d/nested.pdf", pdf, zipfile.ZIP_DEFLATED)
    zf.writestr("c.bin", bytes([0, 1, 2]), zipfile.ZIP_STORED)
    zf.writestr("big.txt", "x" * 100000, zipfile.ZIP_DEFLATED)
print("ok")
`;

async function readAll(source: Readable): Promise<{ data: Buffer; path: string; type: string }[]> {
  const out: { data: Buffer; path: string; type: string }[] = [];

  for await (const entry of unzipEncrypted(source, { password: "" })) {
    const chunks: Buffer[] = [];

    for await (const chunk of entry) {
      chunks.push(Buffer.from(chunk));
    }
    out.push({
      data: Buffer.concat(chunks),
      path: entry.path,
      type: entry.type,
    });
  }

  return out;
}

// CPython is an independent zip *writer*: its deflate streams, extra fields and
// header layout are not produced by our own test helper.
describe.skipIf(!hasPython)("interop: archive written by CPython zipfile", () => {
  it("parses deflate, stored, directory and nested entries in order", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "zip-interop-"));
    const zipPath = path.join(dir, "cpython.zip");

    try {
      const written = runPython(WRITE_ZIP, [zipPath]);

      expect(written.status, written.stderr).toBe(0);

      const entries = await readAll(createReadStream(zipPath, { highWaterMark: 1024 }));

      expect(entries.map((e) => e.path)).toEqual([
        "a.txt",
        "d/",
        "d/nested.pdf",
        "c.bin",
        "big.txt",
      ]);
      expect(entries[0]?.data.toString()).toBe("hello\n");
      expect(entries[1]?.type).toBe("Directory");
      expect(entries[2]?.data.subarray(0, 5).toString()).toBe("%PDF-");
      expect(entries[3]?.data.equals(Buffer.from([0, 1, 2]))).toBe(true);
      expect(entries[4]?.data.length).toBe(100000);
    } finally {
      await rm(dir, { force: true, recursive: true });
    }
  });
});
