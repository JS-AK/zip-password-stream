import { describe, expect, it } from "vitest";

import {
  assertSafeBasename,
  entrySafeName,
  posixBasename,
  posixEntryName,
  toPosixName,
  toPosixSlashes,
} from "./entry-name.js";

describe("entrySafeName (unzip ZipEntry.safeName)", () => {
  it("returns the basename of a backslash path", () => {
    expect(entrySafeName("a\\b\\c.pdf")).toBe("c.pdf");
  });

  it("returns the basename after parent-directory segments", () => {
    expect(entrySafeName("foo/../etc/passwd")).toBe("passwd");
  });

  it("strips trailing slashes so a directory still has a name", () => {
    expect(entrySafeName("foo/bar/")).toBe("bar");
    expect(entrySafeName("foo\\bar\\")).toBe("bar");
  });

  it.each(["..", ".", "", "foo/.."] as const)("throws for an unsafe name %j", (name) => {
    expect(() => entrySafeName(name)).toThrow(/unsafe entry name/);
  });
});

describe("posixEntryName (zip writer)", () => {
  it("returns the full POSIX path, not only the basename", () => {
    expect(posixEntryName("nested\\b.pdf")).toBe("nested/b.pdf");
    expect(posixEntryName("dir/file.txt/")).toBe("dir/file.txt");
  });

  it.each(["..", ".", ""] as const)("throws for an unsafe basename %j", (name) => {
    expect(() => posixEntryName(name)).toThrow(/unsafe entry name/);
  });

  it.each(["../../etc/passwd", "/etc/passwd", "C:\\Windows\\evil.dll", "foo/../passwd"] as const)(
    "throws for a traversal or absolute name %j",
    (name) => {
      expect(() => posixEntryName(name)).toThrow(/unsafe entry name/);
    },
  );

  it("does not treat a parent segment as a basename-only check", () => {
    expect(() => posixEntryName("foo/../passwd")).toThrow(/unsafe entry name/);
    expect(entrySafeName("foo/../passwd")).toBe("passwd");
  });
});

describe("matchPath helpers (detect, no throw, keep trailing slash)", () => {
  it("converts backslashes without stripping a trailing slash", () => {
    expect(toPosixSlashes("nested\\B.PDF")).toBe("nested/B.PDF");
    expect(toPosixSlashes("dir/file.pdf/")).toBe("dir/file.pdf/");
  });

  it("takes basename after slashes only, so a trailing slash is an empty name", () => {
    expect(posixBasename("nested\\B.PDF").toLowerCase()).toBe("b.pdf");
    expect(posixBasename("dir/file.pdf/")).toBe("");
  });

  it("does not throw on parent segments or absolute paths", () => {
    expect(posixBasename("../secret.pdf").toLowerCase()).toBe("secret.pdf");
    expect(posixBasename("/etc/passwd.pdf").toLowerCase()).toBe("passwd.pdf");
  });
});

describe("toPosixName / assertSafeBasename", () => {
  it("strips trailing slashes after POSIX conversion", () => {
    expect(toPosixName("a\\b\\")).toBe("a/b");
  });

  it("assertSafeBasename uses the original path in the error", () => {
    expect(() => assertSafeBasename("foo/..", "..")).toThrow(/unsafe entry name: foo\/\.\./);
  });
});
