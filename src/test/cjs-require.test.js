import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { describe, it } from "node:test";

const require = createRequire(import.meta.url);
const {
  Zip,
  isPdfMagic,
  isPdfPath,
  unzipEncrypted,
  zipEncrypted,
} = require("../../build/cjs/index.js");

describe("cjs dual package", () => {
  it("loads via require()", () => {
    assert.equal(typeof unzipEncrypted, "function");
    assert.equal(typeof zipEncrypted, "function");
    assert.equal(typeof Zip.open, "function");
    assert.equal(typeof Zip.create, "function");
    assert.equal(typeof isPdfPath, "function");
    assert.equal(typeof isPdfMagic, "function");
  });

  it("resolves package exports for require()", () => {
    const pkg = require("../..");
    assert.equal(typeof pkg.unzipEncrypted, "function");
    assert.equal(typeof pkg.zipEncrypted, "function");
    assert.equal(typeof pkg.Zip.open, "function");
    assert.equal(typeof pkg.Zip.create, "function");
    assert.equal(typeof pkg.isPdfPath, "function");
    assert.equal(typeof pkg.isPdfMagic, "function");
  });
});
