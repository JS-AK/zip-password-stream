import { describe, expect, it } from "vitest";

import { Zip, isPdfMagic, isPdfPath, unzipEncrypted, zipEncrypted } from "./index.js";

describe("package skeleton", () => {
  it("runs in Node via Vitest", () => {
    expect(process.versions.node).toBeDefined();
  });

  it("exports unzipEncrypted and PDF helpers", () => {
    expect(typeof unzipEncrypted).toBe("function");
    expect(typeof isPdfPath).toBe("function");
    expect(typeof isPdfMagic).toBe("function");
  });

  it("exports Zip.open, Zip.create, and zipEncrypted", () => {
    expect(typeof Zip.open).toBe("function");
    expect(typeof Zip.create).toBe("function");
    expect(typeof zipEncrypted).toBe("function");
  });
});
