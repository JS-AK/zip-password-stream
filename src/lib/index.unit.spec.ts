import { describe, expect, it } from "vitest";

import { isPdfMagic, isPdfPath, unzipEncrypted } from "./index.js";

describe("package skeleton", () => {
  it("runs in Node via Vitest", () => {
    expect(process.versions.node).toBeDefined();
  });

  it("exports unzipEncrypted and PDF helpers", () => {
    expect(typeof unzipEncrypted).toBe("function");
    expect(typeof isPdfPath).toBe("function");
    expect(typeof isPdfMagic).toBe("function");
  });
});
