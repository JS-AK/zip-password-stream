import { Readable, Writable } from "node:stream";
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { pipeline } from "node:stream/promises";
import zlib from "node:zlib";

import { FIXTURE_FILES, writeZip } from "../../build/esm/test/test-support/write-zip.js";
import { isPdfMagic, isPdfPath, unzipEncrypted } from "../../build/esm/index.js";
import { LOCAL } from "../../build/esm/lib/zip/constants.js";

/**
 * Smoke test for the lowest Node version allowed by `engines.node`.
 *
 * It runs under `node --test` against the compiled output only: no Vitest, no
 * TypeScript, nothing from `node_modules`. The dev toolchain needs Node
 * >=20.19, so on the declared floor the build must already exist and none of
 * the tooling is installable.
 */

const require = createRequire(import.meta.url);

/** Password of the shared test fixture archive. */
const PASSWORD = "secret";
/** Derived from the fixture password so no second credential is hard-coded. */
const WRONG_PASSWORD = `${PASSWORD}-wrong`;
/** PDF file header, checked as text next to the `isPdfMagic` helper. */
const PDF_HEADER = "%PDF-";
const PDF_ENTRY = "nested/b.pdf";

/**
 * Drain one entry into a Buffer. `pipeline` is the consumption pattern the
 * README documents and it settles only once the body is fully read.
 */
async function readEntry(entry) {
  const chunks = [];

  await pipeline(
    entry,
    new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(chunk);
        callback();
      },
    }),
  );

  return Buffer.concat(chunks);
}

/**
 * Walk the whole archive through the public generator, consuming every entry
 * so the pull-reader is never left waiting on an unread body.
 */
async function collectEntries(zip, options) {
  const bodies = new Map();

  for await (const entry of unzipEncrypted(Readable.from([zip]), options)) {
    bodies.set(entry.path, await readEntry(entry));
  }

  return bodies;
}

describe("engine floor", () => {
  it("exposes zlib.crc32, the API that sets the engines.node floor", () => {
    // `zlib.crc32` arrived in Node 20.15.0 and is the newest runtime API used
    // anywhere in src/lib, so it is what `engines.node` is pinned to. If this
    // fails, the declared floor is wrong.
    assert.equal(typeof zlib.crc32, "function");
  });

  it("extracts every fixture entry through the ESM build", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: PASSWORD });

    assert.equal(zip.readUInt32LE(0), LOCAL);

    const bodies = await collectEntries(zip, { password: PASSWORD });

    assert.deepEqual([...bodies.keys()], ["a.txt", PDF_ENTRY, "c.bin"]);
    assert.equal(bodies.get("a.txt").toString("utf8"), "hello\n");
    assert.ok(bodies.get(PDF_ENTRY).toString("latin1").startsWith(PDF_HEADER));
    assert.ok(bodies.get("c.bin").equals(Buffer.from([0x00, 0x01, 0x02])));
  });

  it("rejects a wrong password instead of yielding garbage", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: PASSWORD });

    await assert.rejects(
      () => collectEntries(zip, { password: WRONG_PASSWORD }),
      /invalid zip password:/,
    );
  });

  it("skips filtered-out entries without yielding them", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: PASSWORD });
    const bodies = await collectEntries(zip, { filter: isPdfPath, password: PASSWORD });

    assert.deepEqual([...bodies.keys()], [PDF_ENTRY]);
    assert.ok(isPdfMagic(bodies.get(PDF_ENTRY)));
  });

  it("detects PDF entries by path and by magic", () => {
    assert.equal(isPdfPath(PDF_ENTRY), true);
    assert.equal(isPdfPath("a.txt"), false);
    assert.equal(isPdfMagic(Buffer.from(`${PDF_HEADER}1.1\n`)), true);
    assert.equal(isPdfMagic(Buffer.from("hello\n")), false);
  });

  it("loads the CJS build via require()", () => {
    const cjs = require("../../build/cjs/index.js");

    assert.equal(typeof cjs.unzipEncrypted, "function");
    assert.equal(typeof cjs.isPdfPath, "function");
    assert.equal(typeof cjs.isPdfMagic, "function");
  });
});
