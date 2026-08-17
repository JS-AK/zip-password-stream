import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { describe, it } from "node:test";

const require = createRequire(import.meta.url);
const { isTest } = require("../../build/cjs/index.js");

describe("cjs dual package", () => {
  it("loads via require()", () => {
    assert.equal(typeof isTest, "function");
    assert.equal(isTest("TEST"), true);
  });

  it("resolves package exports for require()", () => {
    const pkg = require("../..");
    assert.equal(typeof pkg.isTest, "function");
    assert.equal(pkg.isTest("TEST"), true);
  });
});
