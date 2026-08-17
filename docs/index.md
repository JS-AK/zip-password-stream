# @js-ak/zip-password-stream

Streaming **ZipCrypto** unzip for **Node.js**. Parse a password-protected zip from a `node:stream` `Readable` in one pass, reading local file headers only. Entries arrive through `for await`; each file body is itself a stream.

There is no `files[]` list and no central-directory seek, so the archive is never buffered in memory and never needs to be seekable — a socket or an HTTP response works as well as a file.

The public API is Node `Readable` only. This package does not export Web Streams and is not a browser bundle.

## Install

```bash
npm install @js-ak/zip-password-stream
```

Node.js **20.15+** (`engines.node`), zero runtime dependencies.

### ESM and CommonJS

The package ships both builds, with types for either entry point.

```ts
import { unzipEncrypted, isPdfMagic } from "@js-ak/zip-password-stream"; // ESM
```

```js
const { unzipEncrypted, isPdfMagic, isPdfPath } = require("@js-ak/zip-password-stream"); // CJS
```

## Quick start

Extract only the PDFs from a password zip on disk, verifying the magic bytes of each body and writing with a safe filename.

```ts
import fs from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { unzipEncrypted, isPdfMagic } from "@js-ak/zip-password-stream";

const password = process.env.ZIP_PASSWORD;
if (!password) {
  throw new Error("ZIP_PASSWORD is required");
}

const source = fs.createReadStream("archive.zip");

for await (const entry of unzipEncrypted(source, {
  password,
  filter: (name) => name.replaceAll("\\", "/").toLowerCase().endsWith(".pdf"),
})) {
  const dest = fs.createWriteStream(
    path.join("out", path.basename(entry.path.replaceAll("\\", "/"))),
  );
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
import { unzipEncrypted } from "@js-ak/zip-password-stream";

const password = process.env.ZIP_PASSWORD;
if (!password) {
  throw new Error("ZIP_PASSWORD is required");
}

const res = await fetch("https://example.com/archive.zip");
if (!res.body) {
  throw new Error("missing body");
}
const source = Readable.fromWeb(res.body);

for await (const entry of unzipEncrypted(source, {
  password,
  filter: (name) => name.replaceAll("\\", "/").toLowerCase().endsWith(".pdf"),
})) {
  await pipeline(
    entry,
    fs.createWriteStream(path.join("out", path.basename(entry.path.replaceAll("\\", "/")))),
  );
}
```

## API

### unzipEncrypted

```ts
export function unzipEncrypted(
  source: Readable,
  options: UnzipOptions,
): AsyncGenerator<ZipEntry, void, unknown>;
```

Walks local file headers until the central directory or EOCD signature and yields one `ZipEntry` per file. Encrypted members (`flags & 1`) get a **new ZipCrypto key state per entry**.

```ts
export type UnzipOptions = {
  password?: string; // required only for encrypted entries
  passwordEncoding?: BufferEncoding; // default utf8
  filter?: (path: string) => boolean;
  maxEntrySize?: number; // uncompressed bytes per entry; unlimited when unset
};

export type ZipEntry = Readable & {
  path: string;
  type: "File" | "Directory";
  compressedSize?: number;
  uncompressedSize?: number;
  encrypted: boolean;
  autodrain(): Promise<void>;
};
```

`password` may be omitted for a fully unencrypted archive; an encrypted entry without it throws `password required: <name>`.

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

These helpers sniff zip **entry** bytes and names so you can decide without trusting `entry.path` alone. They are not a `file-type` replacement: no MIME map, no deep sniff, no formats beyond this table.

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
- `entry.destroy()` skips the rest of that body and iteration continues with the next entry.
- Before leaving the loop early, finish or `destroy()` the current entry — the parser is still waiting on it, and `break` alone cannot interrupt that wait.
- Once the loop exits (normally or by `break`/`throw`), the source stream is destroyed.

### Skipping entries cheaply

Prefer `filter` over draining. A filtered entry is discarded by **compressed** size and is never inflated or yielded, which is safer on untrusted archives than `autodrain()`, which decompresses into a sink. Encrypted skips still verify the ZipCrypto header, so a wrong password still fails.

### Password check byte

A wrong password normally fails on the ZipCrypto check byte with `invalid zip password: <name>`. That byte is only 8 bits, so a match is **not** proof of the password (1/256). Every entry is also CRC-checked and size-checked against the local header, or against the data descriptor when the header omits those values — so a false-positive check byte fails with `crc mismatch: <name>` rather than yielding garbage.

### Password encoding

Default is UTF-8. Many Windows-created zips store the password as OEM/cp437. Set `passwordEncoding` (for example `"latin1"`) when a UTF-8 password check byte fails on a known-good archive. Confirm against a real file; do not guess the code page. Node has no built-in `cp437` encoding.

### Zip slip

Local-header names are attacker-controlled and may contain `..` or absolute paths. Never hand `entry.path` to `createWriteStream`; take `path.basename` after normalizing `\` so a crafted name cannot escape the output directory.

### Zip bombs

Local-header sizes come from the archive, so they bound nothing on their own. Set `maxEntrySize` to fail an entry that inflates past a limit (`entry exceeds maxEntrySize: <name>`); with `filter`, a skipped entry is never inflated at all.

## Errors

Every failure is a plain `Error`; match on the message. `<name>` is the zip entry name from the local header.

| Message                                      | Cause                                                                                             |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `password required: <name>`                  | Entry has the encrypted flag but no `password` was passed                                         |
| `invalid zip password: <name>`               | ZipCrypto check byte did not match the expected CRC/DOS-time byte                                 |
| `truncated zip crypto header: <name>`        | Encrypted entry whose declared compressed size is under the 12-byte ZipCrypto header              |
| `crc mismatch: <name>`                       | Output CRC32 differs from the local header or data descriptor (also a late wrong-password signal) |
| `size mismatch: <name>`                      | Output size, or a data-descriptor size, differs from the local header                             |
| `entry exceeds maxEntrySize: <name>`         | Uncompressed output passed `maxEntrySize`                                                         |
| `AES zip not supported: <name>`              | WinZip AES: method `99` or extra field `0x9901`                                                   |
| `Zip64 not supported: <name>`                | Extra field `0x0001`, or a size of `0xFFFFFFFF`                                                   |
| `unsupported compression method <n>: <name>` | Method other than `0` (stored) or `8` (deflate)                                                   |
| `no size in local header: <name>`            | `flags & 0x08` (data descriptor) with `compressedSize === 0` — one pass cannot find the end       |
| `malformed extra field: <name>`              | An extra-field record claims more bytes than the extra field holds                                |
| `bad zip signature 0x<hex>`                  | Next 4 bytes are neither a local header, central directory, nor EOCD                              |
| `unexpected EOF while reading <what>`        | Archive ends inside a header, name, extra field, crypto header, descriptor, or file data          |
| `unexpected EOF while discarding zip data`   | Archive ends while skipping a filtered or destroyed entry's body                                  |

## Limitations (v1)

| Topic                                                                                            | Behavior                                            |
| ------------------------------------------------------------------------------------------------ | --------------------------------------------------- |
| ZipCrypto (traditional PKWARE)                                                                   | Supported                                           |
| WinZip AES (method 99 / extra `0x9901`)                                                          | Throws `AES zip not supported: <name>`              |
| Data descriptor **without** size in the local header (`flags & 0x08` and `compressedSize === 0`) | Throws `no size in local header: <name>`            |
| Data descriptor **with** size in the local header                                                | Supported; its crc and sizes are validated          |
| Zip64 (extra `0x0001` or size `0xFFFFFFFF`)                                                      | Throws `Zip64 not supported: <name>`                |
| Stored (0) and deflate (8)                                                                       | Supported                                           |
| Any other compression method                                                                     | Throws `unsupported compression method <n>: <name>` |
| 7z / RAR / multi-volume                                                                          | Not supported                                       |
| Node.js `Readable`                                                                               | Supported                                           |
| Browser / Deno / Bun / Web Streams export                                                        | Not a v1 target                                     |
| PDF `/Encrypt`                                                                                   | Not unzip; this library only decrypts the zip       |

## Runtime

No `unzipper`, `jszip`, or `@zip.js/zip.js` at runtime. Compression uses `node:zlib` (`createInflateRaw`, `crc32`); everything else comes from `node:stream`, `node:stream/promises`, and `node:events`.

`zlib.crc32` (Node 20.15 / 22.2) is the newest API in use and is what sets the supported floor. Developing this repo needs Node 20.19+ — Vitest and ESLint require it — but the published build does not: CI builds on 20.x and then runs the compiled package on 20.15.0 so the declared floor stays honest.
