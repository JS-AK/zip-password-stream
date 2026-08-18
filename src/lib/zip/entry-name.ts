/** Basename results that must not be used as a file or archive entry name. */
const UNSAFE_ENTRY_BASENAMES = new Set(["", ".", ".."]);

/** APPNOTE 4.4.17.1: a drive letter in an entry name is zip-slip. */
const DRIVE_LETTER_PREFIX = /^[A-Za-z]:/u;

/** Throw `unsafe entry name: <original>` when `base` is empty, `.`, or `..`. */
export function assertSafeBasename(original: string, base: string): void {
  if (isUnsafeEntryBasename(base)) {
    throw new Error(`unsafe entry name: ${original}`);
  }
}

/**
 * Basename after POSIX slashes and trailing-`/` strip.
 * Throws `unsafe entry name: <path>` for empty / `.` / `..` basename.
 * Does not reject parent segments (`foo/../etc/passwd` → `passwd`).
 */
export function entrySafeName(path: string): string {
  const base = posixBasename(toPosixName(path));

  assertSafeBasename(path, base);

  return base;
}

/** True when `base` is empty, `.`, or `..`. */
export function isUnsafeEntryBasename(base: string): boolean {
  return UNSAFE_ENTRY_BASENAMES.has(base);
}

/**
 * Last path segment after POSIX slashes. Does not strip trailing `/`
 * (`"dir/"` → `""`).
 */
export function posixBasename(name: string): string {
  const posix = toPosixSlashes(name);

  return posix.slice(posix.lastIndexOf("/") + 1);
}

/**
 * Writer zip-slip rules: POSIX slashes, strip trailing `/`, reject a leading
 * `/`, a drive letter, any `..` segment, or an unsafe basename. Returns the
 * full normalized path.
 */
export function posixEntryName(name: string): string {
  const normalized = toPosixName(name);

  if (normalized.startsWith("/") || DRIVE_LETTER_PREFIX.test(normalized)) {
    throw new Error(`unsafe entry name: ${name}`);
  }

  const segments = normalized.split("/");
  const base = segments.at(-1) ?? "";

  if (segments.some((segment) => segment === "..") || isUnsafeEntryBasename(base)) {
    throw new Error(`unsafe entry name: ${name}`);
  }

  return normalized;
}

/** POSIX slashes, then strip trailing `/` so a directory entry still has a name. */
export function toPosixName(name: string): string {
  return toPosixSlashes(name).replace(/\/+$/u, "");
}

/**
 * Replace `\` with `/`. Does not strip trailing slashes — `matchPath` keeps
 * `"dir/"` as an empty basename so a directory name is not an extension hit.
 */
export function toPosixSlashes(name: string): string {
  return name.replaceAll("\\", "/");
}
