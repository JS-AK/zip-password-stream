# @js-ak/zip-password-stream

Streaming **ZipCrypto** unzip **and zip** for **Node.js**. Parse a password-protected zip from a `node:stream` `Readable` in one pass, reading local file headers only, or write one to a `Writable` without buffering file payloads. Entries arrive through `for await`; each file body is itself a stream.

There is no `files[]` list and no central-directory seek on read, so the archive is never buffered in memory and never needs to be seekable — a socket or an HTTP response works as well as a file.

The public API is Node `Readable` / `Writable` only. This package does not export Web Streams and is not a browser bundle.

## Install

```bash
npm install @js-ak/zip-password-stream
```

Node.js **20.15+** (`engines.node`), zero runtime dependencies.

### ESM and CommonJS

The package ships both builds, with types for either entry point.

```ts
import { Zip, unzipEncrypted, zipEncrypted, isPdfMagic } from "@js-ak/zip-password-stream"; // ESM
```

```js
const { Zip, unzipEncrypted, zipEncrypted, isPdfMagic, isPdfPath } = require("@js-ak/zip-password-stream"); // CJS
```

## Quick start

Extract names that look like PDFs from a password zip on disk. `.only("pdf")` is a **basename** check (`isKindPath`) — it does not sniff file magic. Verify the body with `isPdfMagic` and write with `entry.safeName()` so zip-slip paths cannot escape the output directory.

```ts
import fs from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Zip, isPdfMagic } from "@js-ak/zip-password-stream";

const password = process.env.ZIP_PASSWORD;
if (!password) {
  throw new Error("ZIP_PASSWORD is required");
}

const source = fs.createReadStream("archive.zip");

for await (const entry of Zip.open(source).password(password).only("pdf")) {
  const dest = fs.createWriteStream(path.join("out", entry.safeName()));
  let first = true;
  await pipeline(
    entry,
    async function* (chunks) {
      for await (const chunk of chunks) {
        if (first) {
          first = false;
          if (!isPdfMagic(chunk)) {
            throw new Error("not a PDF");
          }
        }
        yield chunk;
      }
    },
    dest,
  );
}
```

### HTTP source

`fetch` returns a Web `ReadableStream`; adapt it on the caller side with `Readable.fromWeb`.

```ts
import fs from "node:fs";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { Zip, isPdfMagic } from "@js-ak/zip-password-stream";

const password = process.env.ZIP_PASSWORD;
if (!password) {
  throw new Error("ZIP_PASSWORD is required");
}

const res = await fetch("https://example.com/archive.zip");
if (!res.body) {
  throw new Error("missing body");
}
const source = Readable.fromWeb(res.body);

for await (const entry of Zip.open(source)
  .password(password)
  .only("pdf")
  .maxEntrySize(32 * 1024 * 1024)) {
  const dest = fs.createWriteStream(path.join("out", entry.safeName()));
  let first = true;
  await pipeline(
    entry,
    async function* (chunks) {
      for await (const chunk of chunks) {
        if (first) {
          first = false;
          if (!isPdfMagic(chunk)) {
            throw new Error("not a PDF");
          }
        }
        yield chunk;
      }
    },
    dest,
  );
}
```

Configure the builder, then iterate **once**. A second `for await`, or a builder call after iteration started, throws `zip already opened`.

### Create a password zip

`Zip.create` streams deflate (ZipCrypto when `.password` is set). Local crc/sizes are zero; a 16-byte data descriptor follows each body. `end()` writes the central directory and EOCD, calls `dest.end()`, and waits until `dest` has finished. `ZipCreate` is a **type-only** package export — construct writers only via `Zip.create` (private constructor).

ZipCrypto is traditional PKWARE encryption. This library writes it for **interop** with tools that require a password zip. It is **not** confidentiality-grade (known-plaintext attacks). If you need real protection, encrypt the payload before `add`.

```ts
import fs from "node:fs";
import { Zip } from "@js-ak/zip-password-stream";

const password = process.env.ZIP_PASSWORD;
if (!password) {
  throw new Error("ZIP_PASSWORD is required");
}

const dest = fs.createWriteStream("out.zip");
const zip = Zip.create(dest).password(password);

await zip.add("nested/b.pdf", fs.createReadStream("b.pdf"));
await zip.end();
```

`zipEncrypted(dest, { password })` is the same writer. Call `end()` or the central directory is missing. `add` after `end` throws `zip already closed`. A builder method after the first `add`/`end` throws `zip already started`. Overlapping `add`/`end` throws `zip write in progress`. After any failed `add`, further `add`/`end` throw `zip writer failed`. Empty / `.` / `..` names, a `..` path segment, a leading `/`, or a drive letter throw `unsafe entry name: <path>`. If the sink closes or errors while writing, `add`/`end` throw `zip destination closed`.

## API

### Zip

```ts
class Zip implements AsyncIterable<ZipEntry> {
  static open(source: Readable): Zip;
  static create(dest: Writable): ZipCreate;
  password(password: string): this;
  passwordEncoding(encoding: BufferEncoding): this;
  only(...kinds: [DetectKind, ...DetectKind[]]): this;
  filter(fn: (path: string) => boolean): this;
  maxEntrySize(bytes: number): this;
}

/** Type-only export. Construct only via Zip.create (private constructor). */
class ZipCreate implements ZipWriter {
  password(password: string): this;
  passwordEncoding(encoding: BufferEncoding): this;
  maxEntrySize(bytes: number): this;
  add(name: string, body: Readable): Promise<void>;
  end(): Promise<void>; // CD + EOCD, dest.end(), wait until dest has finished
}
```

`Zip.open` wraps a Node `Readable`. Builder methods do not read the zip; `for await` starts `unzipEncrypted` with the options collected so far. `Zip.create` wraps a Node `Writable`; the first `add` or `end` starts `zipEncrypted`. The package exports `ZipCreate` as a type only (`export type { ZipCreate }`); the class is not constructible from the package.

| Method             | Effect                                                                                         |
| ------------------ | ---------------------------------------------------------------------------------------------- |
| `password`         | ZipCrypto password. Last call wins. Omit/`undefined` → unencrypted; `""` is a real password.   |
| `passwordEncoding` | Password bytes encoding. Default UTF-8. Last call wins.                                        |
| `only`             | Yield names whose **basename** matches any kind (`isKindPath`). Replaces a previous `.only`.   |
| `filter`           | Yield when `fn(localHeaderName)` is true. Last `.filter` wins. AND with `.only` when both set. |
| `maxEntrySize`     | Uncompressed-byte cap per yielded entry. Last call wins. Unlimited when unset.                 |

`.only()` with no kinds, or a hole in the list (`.only("pdf", undefined)` from JS), throws `only() requires a kind` (TypeScript already requires one argument).

### unzipEncrypted

Low-level generator. Same parser, same `ZipEntry` streams.

```ts
export function unzipEncrypted(
  source: Readable,
  options: UnzipOptions = {},
): AsyncGenerator<ZipEntry, void, unknown>;
```

Walks local file headers until the central directory or EOCD signature and yields one `ZipEntry` per file. Encrypted members (`flags & 1`) get a **new ZipCrypto key state per entry**.

### zipEncrypted

Low-level writer. Same layout as `Zip.create` (deflate, bit 3, optional ZipCrypto).

```ts
export function zipEncrypted(dest: Writable, options?: ZipWriteOptions): ZipWriter;
```

```ts
export type ZipWriteOptions = {
  password?: string;
  passwordEncoding?: BufferEncoding; // default utf8
  maxEntrySize?: number; // uncompressed bytes per add(); unlimited when unset
};

export type ZipWriter = {
  add(name: string, body: Readable): Promise<void>;
  end(): Promise<void>; // CD + EOCD, dest.end(), wait until dest has finished
};
```

```ts
export type UnzipOptions = {
  password?: string; // required only for encrypted entries
  passwordEncoding?: BufferEncoding; // default utf8
  filter?: (path: string) => boolean;
  maxEntrySize?: number; // uncompressed bytes per entry; unlimited when unset
};
```

### ZipEntry

```ts
export type ZipEntry = Readable & {
  path: string;
  type: "File" | "Directory";
  compressedSize?: number;
  uncompressedSize?: number;
  encrypted: boolean;
  autodrain(): Promise<void>;
  isFile(): boolean; // type === "File"
  isDirectory(): boolean; // type === "Directory"
  safeName(): string;
};
```

`password` may be omitted (`undefined`) for a fully unencrypted archive; an empty string is a real (empty) ZipCrypto password. An encrypted entry without a password throws `password required: <name>`.

`safeName()` is the basename after `\` → `/` (trailing `/` stripped so directory entries still have a name). It throws `unsafe entry name: <path>` when that basename is empty, `.`, or `..`. It does not resolve `..` in parent segments (`foo/../etc/passwd` → `passwd`).

Without `.only` / `filter`, directory entries are yielded; consume or skip each one (`entry.isDirectory()` then `autodrain()`, or `destroy()`).

### Detect helpers

```ts
export type DetectKind =
  "bmp" | "gif" | "gzip" | "ico" | "jpeg" | "pdf" | "png" | "tiff" | "wav" | "webp" | "xml" | "zip";

export function isKindMagic(kind: DetectKind, chunk: Uint8Array): boolean;
export function isKindPath(kind: DetectKind, name: string): boolean;

export function isPdfMagic(chunk: Uint8Array): boolean;
export function isPdfPath(name: string): boolean;
```

Every `DetectKind` also has a named `isXMagic` / `isXPath` pair (`isPngMagic`, `isJpegPath`, …). Magic checks the first bytes of a chunk; path checks the **basename** extension after normalizing `\`, in any case.

`.only("pdf")` calls `isKindPath`; `isPdfMagic` is for the body after yield. These helpers sniff zip **entry** bytes and names so you can decide without trusting `entry.path` alone. They are not a `file-type` replacement: no MIME map, no deep sniff, no formats beyond this table.

| Kind   | Magic (first bytes)        | Path (basename, any case) |
| ------ | -------------------------- | ------------------------- |
| `bmp`  | `BM`                       | `.bmp`                    |
| `gif`  | GIF87a or GIF89a           | `.gif`                    |
| `gzip` | `1F 8B`                    | `.gz` / `.gzip`           |
| `ico`  | `00 00 01 00`              | `.ico`                    |
| `jpeg` | `FF D8 FF`                 | `.jpg` / `.jpeg`          |
| `pdf`  | `%PDF-`                    | `.pdf`                    |
| `png`  | PNG signature              | `.png`                    |
| `tiff` | little- or big-endian TIFF | `.tif` / `.tiff`          |
| `wav`  | RIFF + `WAVE` at offset 8  | `.wav`                    |
| `webp` | RIFF + `WEBP` at offset 8  | `.webp`                   |
| `xml`  | `<?xml`                    | `.xml`                    |
| `zip`  | PKZIP local file header    | `.zip`                    |

## Behavior

### Entry lifecycle

Consume **or skip** every yielded entry. Holding one without doing either stalls the parser, because the body is still sitting in the source stream (the same contract as unzipper).

- Consume or skip each entry: `pipeline(entry, …)`, `entry.autodrain()`, or `entry.destroy()`.
- `entry.destroy()` skips the rest of a **yielded** body (no `push` of remaining bytes) and iteration continues with the next entry. For bit-3 unknown-length, destroy still inflates internally to find the data descriptor — including unread destroy, and after a mid-read abort.
- Before leaving the loop early, finish or `destroy()` the current entry — the parser is still waiting on it, and `break` alone cannot interrupt that wait.
- Once the loop exits (normally or by `break`/`throw`), the source stream is destroyed.

### Skipping entries cheaply

Prefer `.only` / `.filter` (or `filter` on `unzipEncrypted`) over draining. A filtered entry with a **known** compressed size is discarded by that size and is never inflated or yielded. Unknown-length bit-3 deflate (including archives from this library's writer) still inflates to find the data descriptor. Encrypted skips still verify the ZipCrypto header, so a wrong password still fails. `.only("pdf")` is not a security boundary for file type.

`.only("pdf")` does not inspect file bytes. Sniff the body with `isPdfMagic` after yield if you need that.

### Password check byte

A wrong password normally fails on the ZipCrypto check byte with `invalid zip password: <name>`. That byte is only 8 bits, so a match is **not** proof of the password (1/256). Every entry is also CRC-checked and size-checked against the local header, or against the data descriptor when the header omits those values — so a false-positive check byte fails with `crc mismatch: <name>` rather than yielding garbage.

### Password encoding

Default is UTF-8. Many Windows-created zips store the password as OEM/cp437. Set `passwordEncoding` (for example `"latin1"`) when a UTF-8 password check byte fails on a known-good archive. Confirm against a real file; do not guess the code page. Node has no built-in `cp437` encoding.

### Zip slip

Local-header names are attacker-controlled and may contain `..` or absolute paths. Never hand `entry.path` to `createWriteStream`. Use `entry.safeName()` — the **basename** after normalizing `\` — so a crafted name cannot escape the output directory. Empty, `.`, and `..` basenames throw `unsafe entry name: <path>`. `safeName()` does not strip `:` (NTFS alternate data streams) or Windows reserved device names (`CON`, `NUL`, `PRN`); extra checks on those platforms are the caller's.

### Zip bombs

Local-header sizes come from the archive, so they bound nothing on their own. Set `maxEntrySize` (builder or `UnzipOptions` / `ZipWriteOptions`) to fail a **yielded** entry (or an `add`) that inflates past a limit (`entry exceeds maxEntrySize: <name>`). With `.only` / `filter`, a skipped entry with a **known** compressed size is never inflated; unknown-length bit-3 entries still inflate to find the descriptor, but a skipped entry never throws `entry exceeds maxEntrySize`. Always set `maxEntrySize` on untrusted input — including HTTP bodies.

## Errors

Every failure is a plain `Error`; match on the message. `<name>` / `<path>` is the zip entry name from the local header.

| Message                                      | Cause                                                                                             |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `zip already opened`                         | Second `for await` on the same `Zip`, or a builder method after iteration started                 |
| `zip already started`                        | A `Zip.create` builder method after the first `add`/`end`                                         |
| `zip already closed`                         | `add`/`end` after `end()`                                                                         |
| `zip write in progress`                      | Overlapping `add`/`end` on the same writer                                                        |
| `zip writer failed`                          | `add`/`end` after a previous write failed                                                         |
| `zip destination closed`                     | The writable closed or errored before the archive was fully written                               |
| `only() requires a kind`                     | `.only()` with no kinds, or a hole (`undefined`) in the kinds list                                |
| `unsafe entry name: <path>`                  | `safeName()` basename is empty / `.` / `..`, or `add()` name is absolute, has a drive letter, or a `..` segment |
| `password required: <name>`                  | Entry has the encrypted flag but no `password` was passed                                         |
| `invalid zip password: <name>`               | ZipCrypto check byte did not match the expected CRC/DOS-time byte                                 |
| `truncated zip crypto header: <name>`        | Encrypted entry whose declared compressed size is under the 12-byte ZipCrypto header              |
| `crc mismatch: <name>`                       | Output CRC32 differs from the local header or data descriptor (also a late wrong-password signal) |
| `size mismatch: <name>`                      | Output size, or a data-descriptor size, differs from the local header                             |
| `entry exceeds maxEntrySize: <name>`         | Uncompressed output passed `maxEntrySize`                                                         |
| `AES zip not supported: <name>`              | WinZip AES: method `99` or extra field `0x9901`                                                   |
| `Zip64 not supported: <name>`                | Extra field `0x0001`, or a size of `0xFFFFFFFF`                                                   |
| `unsupported compression method <n>: <name>` | Method other than `0` (stored) or `8` (deflate)                                                   |
| `no size in local header: <name>`            | `flags & 0x08` and `compressedSize === 0` on a **stored** entry (deflate can find the end)        |
| `malformed extra field: <name>`              | An extra-field record claims more bytes than the extra field holds                                |
| `bad zip signature 0x<hex>`                  | Next 4 bytes are neither a local header, central directory, nor EOCD                              |
| `unexpected EOF while reading <what>`        | Archive ends inside a header, name, extra field, crypto header, descriptor, or file data          |
| `unexpected EOF while discarding zip data`   | Archive ends while skipping a filtered or destroyed entry's body                                  |

## Limitations (v1)

| Topic                                                                                            | Behavior                                            |
| ------------------------------------------------------------------------------------------------ | --------------------------------------------------- |
| ZipCrypto (traditional PKWARE)                                                                   | Supported                                           |
| WinZip AES (method 99 / extra `0x9901`)                                                          | Throws `AES zip not supported: <name>`              |
| Data descriptor **without** size in the local header (`flags & 0x08` and `compressedSize === 0`) | Deflate: inflate until Z_STREAM_END, then read the descriptor. Stored: throws `no size in local header: <name>` |
| Data descriptor **with** size in the local header                                                | Supported; its crc and sizes are validated          |
| Zip64 (extra `0x0001` or size `0xFFFFFFFF`)                                                      | Throws `Zip64 not supported: <name>`                |
| Stored (0) and deflate (8)                                                                       | Supported                                           |
| Any other compression method                                                                     | Throws `unsupported compression method <n>: <name>` |
| 7z / RAR / multi-volume                                                                          | Not supported                                       |
| Node.js `Readable`                                                                               | Supported                                           |
| Browser / Deno / Bun / Web Streams export                                                        | Not a v1 target                                     |
| PDF `/Encrypt`                                                                                   | Not unzip; this library only decrypts the zip       |

## Runtime

No `unzipper`, `jszip`, or `@zip.js/zip.js` at runtime. Compression uses `node:zlib` (`createInflateRaw`, `createDeflateRaw`, `crc32`); everything else comes from `node:stream`, `node:stream/promises`, `node:events`, and `node:crypto` (`randomBytes` for ZipCrypto headers).

`zlib.crc32` (Node 20.15 / 22.2) is the newest API in use and is what sets the supported floor. Developing this repo needs Node 20.19+ — Vitest and ESLint require it — but the published build does not: CI builds on 20.x and then runs the compiled package on 20.15.0 so the declared floor stays honest.
