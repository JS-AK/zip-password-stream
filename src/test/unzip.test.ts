import { Readable, Transform, Writable } from "node:stream";
import { mkdtemp, unlink } from "node:fs/promises";
import { crc32 } from "node:zlib";
import { createReadStream } from "node:fs";
import { once } from "node:events";
import os from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";

import { describe, expect, it } from "vitest";

import { EOCD, EOCD_LEN, ZIP_METHOD_DEFLATE } from "../lib/zip/constants.js";
import {
  FIXTURE_FILES,
  TINY_PDF,
  writeAesExtraStub,
  writeAesMethodStub,
  writeDataDescriptorNoSizeStub,
  writeLargeStoredZipFile,
  writeLocalHeaderOnly,
  writeMalformedExtraStub,
  writeZip,
  writeZip64SizeStub,
} from "./test-support/write-zip.js";
import { type UnzipOptions, type ZipEntry, unzipEncrypted } from "../lib/unzip.js";
import { isPdfMagic, isPdfPath } from "../lib/detect/detect.js";

function chunked(data: Buffer, size = 3): Readable {
  const chunks: Buffer[] = [];

  for (let i = 0; i < data.length; i += size) {
    chunks.push(Buffer.from(data.subarray(i, i + size)));
  }

  return Readable.from(chunks);
}

async function collect(entry: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];

  for await (const chunk of entry) {
    chunks.push(Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

async function collectNamed(
  source: Readable,
  password: string,
  filter?: (path: string) => boolean,
  extra?: Partial<UnzipOptions>,
): Promise<{ data: Buffer; path: string; type: ZipEntry["type"] }[]> {
  const out: { data: Buffer; path: string; type: ZipEntry["type"] }[] = [];

  for await (const entry of unzipEncrypted(source, {
    filter,
    password,
    ...extra,
  })) {
    out.push({
      data: await collect(entry),
      path: entry.path,
      type: entry.type,
    });
  }

  return out;
}

describe("local header parser", () => {
  it("yields unencrypted names in order and stops at the central directory", async () => {
    const zip = writeZip(FIXTURE_FILES);
    const entries = await collectNamed(chunked(zip, 2), "unused");

    expect(entries.map((e) => e.path)).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
    expect(entries.map((e) => e.data)).toEqual([
      Buffer.from("hello\n"),
      TINY_PDF,
      Buffer.from([0, 1, 2]),
    ]);
  });

  it("throws when AES compression method is used", async () => {
    const zip = writeAesMethodStub("secret.pdf");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /AES zip not supported: secret\.pdf/,
    );
  });

  it("throws when a WinZip AES extra field is present", async () => {
    const zip = writeAesExtraStub("secret.pdf");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /AES zip not supported: secret\.pdf/,
    );
  });

  it("throws when bit3 is set and local compressed size is 0", async () => {
    const zip = writeDataDescriptorNoSizeStub("open.bin");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /no size in local header: open\.bin/,
    );
  });

  it("throws on Zip64 size sentinels", async () => {
    const zip = writeZip64SizeStub("huge.bin");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /Zip64 not supported: huge\.bin/,
    );
  });

  it("throws on a malformed extra field instead of skipping AES extra", async () => {
    const zip = writeMalformedExtraStub("secret.pdf");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /malformed extra field/,
    );
  });
});

describe("entry body stream", () => {
  it("autodrains unused files so the next header can be parsed", async () => {
    const zip = writeZip(FIXTURE_FILES);
    const names: string[] = [];

    for await (const entry of unzipEncrypted(chunked(zip), {
      password: "unused",
    })) {
      names.push(entry.path);
      if (entry.path !== "c.bin") {
        await entry.autodrain();
        continue;
      }
      expect(await collect(entry)).toEqual(Buffer.from([0, 1, 2]));
    }
    expect(names).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
  });

  it("does not buffer a large stored file before the sink", async () => {
    const size = 16 * 1024 * 1024;
    const dir = await mkdtemp(path.join(os.tmpdir(), "zip-rss-"));
    const file = path.join(dir, "large.zip");

    await writeLargeStoredZipFile(file, "big.bin", size);
    const fsSrc = createReadStream(file, { highWaterMark: 64 * 1024 });

    try {
      let sourceBytes = 0;
      let received = 0;
      let peakLag = 0;
      const meter = new Transform({
        highWaterMark: 64 * 1024,
        transform(chunk, _enc, cb) {
          sourceBytes += chunk.length;
          peakLag = Math.max(peakLag, sourceBytes - received);
          cb(null, chunk);
        },
      });

      fsSrc.pipe(meter);
      for await (const entry of unzipEncrypted(meter, { password: "" })) {
        await pipeline(
          entry,
          new Writable({
            highWaterMark: 64 * 1024,
            write(chunk, _enc, cb) {
              received += chunk.length;
              peakLag = Math.max(peakLag, sourceBytes - received);
              cb();
            },
          }),
        );
      }
      expect(received).toBe(size);
      expect(peakLag).toBeLessThan(1024 * 1024);
    } finally {
      fsSrc.destroy();
      await unlink(file);
    }
  });

  it("discards a directory payload so the next file still extracts", async () => {
    const dirHeader = writeLocalHeaderOnly({
      compressedSize: 4,
      method: 0,
      name: "foo/",
      uncompressedSize: 4,
    });
    const zip = Buffer.concat([
      dirHeader,
      Buffer.from("junk"),
      writeZip([{ data: Buffer.from("hello\n"), method: 0, name: "a.txt" }]),
    ]);
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries.map((e) => e.path)).toEqual(["foo/", "a.txt"]);
    expect(entries[0]?.type).toBe("Directory");
    expect(entries[1]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });

  it("keeps the body of a file whose name ends with a backslash", async () => {
    const body = Buffer.from("data");
    const zip = Buffer.concat([
      writeLocalHeaderOnly({
        compressedSize: body.length,
        crc: crc32(body) >>> 0,
        method: 0,
        name: "weird\\",
        uncompressedSize: body.length,
      }),
      body,
    ]);
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries[0]?.type).toBe("File");
    expect(entries[0]?.data.toString()).toBe("data");
  });

  it("treats an empty backslash-terminated entry as a directory", async () => {
    const zip = Buffer.concat([
      writeLocalHeaderOnly({
        compressedSize: 0,
        method: 0,
        name: "weird\\",
        uncompressedSize: 0,
      }),
    ]);
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries[0]?.type).toBe("Directory");
  });

  it("continues iteration when an unread entry is destroyed", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const seen: string[] = [];

    for await (const entry of unzipEncrypted(Readable.from([zip]), {
      password: "secret",
    })) {
      seen.push(entry.path);
      if (entry.path === "a.txt") {
        entry.destroy();
        continue;
      }
      await entry.autodrain();
    }
    expect(seen).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
  });

  it("continues iteration when an entry is destroyed mid-read", async () => {
    const big = Buffer.alloc(512 * 1024, 0x61);
    const zip = writeZip([
      { data: big, method: 0, name: "big.bin" },
      { data: Buffer.from("after\n"), method: 0, name: "after.txt" },
    ]);
    const seen: string[] = [];
    let tail: Buffer | undefined;

    for await (const entry of unzipEncrypted(chunked(zip, 8192), {
      password: "",
    })) {
      seen.push(entry.path);
      if (entry.path === "big.bin") {
        await once(entry, "readable");
        entry.read();
        entry.destroy();
        continue;
      }
      tail = await collect(entry);
    }
    expect(seen).toEqual(["big.bin", "after.txt"]);
    expect(tail?.equals(Buffer.from("after\n"))).toBe(true);
  });

  it("destroys the source stream when the consumer breaks early", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const source = Readable.from([zip]);

    for await (const entry of unzipEncrypted(source, { password: "secret" })) {
      await entry.autodrain();
      break;
    }
    expect(source.destroyed).toBe(true);
  });

  it("does not hang the generator when deflate fails under a catching consumer", async () => {
    const payload = Buffer.alloc(32, 0x03);
    const header = writeLocalHeaderOnly({
      compressedSize: payload.length,
      method: ZIP_METHOD_DEFLATE,
      name: "bad.txt",
      uncompressedSize: 64,
    });
    const eocd = Buffer.alloc(EOCD_LEN);

    eocd.writeUInt32LE(EOCD, 0);
    const zip = Buffer.concat([header, payload, eocd]);

    let sawEntryError = false;

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {
          password: "",
        })) {
          try {
            await collect(entry);
          } catch {
            sawEntryError = true;
          }
        }
      })(),
    ).rejects.toThrow();
    expect(sawEntryError).toBe(true);
  });
});

describe("optional password", () => {
  it("reads an unencrypted archive with no password option", async () => {
    const zip = writeZip(FIXTURE_FILES);
    const out: string[] = [];

    for await (const entry of unzipEncrypted(Readable.from([zip]), {})) {
      out.push(entry.path);
      await entry.autodrain();
    }
    expect(out).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
  });

  it("names the entry that needs a password", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {})) {
          await entry.autodrain();
        }
      })(),
    ).rejects.toThrow(/password required: a\.txt/);
  });
});

describe("size limits", () => {
  it("stops a deflate entry that grows past maxEntrySize", async () => {
    const bomb = Buffer.alloc(1024 * 1024, 0x00);
    const zip = writeZip([{ data: bomb, method: ZIP_METHOD_DEFLATE, name: "bomb.bin" }]);

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {
          maxEntrySize: 64 * 1024,
          password: "",
        })) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/entry exceeds maxEntrySize: bomb\.bin/);
  });

  it("accepts an entry exactly at maxEntrySize", async () => {
    const data = Buffer.alloc(4096, 0x41);
    const zip = writeZip([{ data, method: ZIP_METHOD_DEFLATE, name: "fits.bin" }]);
    const entries = await collectNamed(Readable.from([zip]), "", undefined, {
      maxEntrySize: data.length,
    });

    expect(entries[0]?.data.length).toBe(data.length);
  });

  it("rejects a deflate entry whose local header understates the size", async () => {
    const zip = writeZip([
      { data: Buffer.alloc(4096, 0x42), method: ZIP_METHOD_DEFLATE, name: "liar.bin" },
    ]);

    // uncompressedSize lives at local header offset 22
    zip.writeUInt32LE(16, 22);
    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {
          password: "",
        })) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/size mismatch: liar\.bin/);
  });

  it("rejects a stored entry whose local header overstates the size", async () => {
    const zip = writeZip([{ data: Buffer.from("abc"), method: 0, name: "short.bin" }]);

    // Lie about uncompressed size (not AES method 99).
    const overstatedUncompressedSize = 99;

    zip.writeUInt32LE(overstatedUncompressedSize, 22);
    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {
          password: "",
        })) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/size mismatch: short\.bin/);
  });
});

describe("password path", () => {
  it("extracts every ZipCrypto file including the second (keys reset per entry)", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const entries = await collectNamed(chunked(zip, 1), "secret");

    expect(entries.map((e) => e.path)).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
    expect(entries[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
    expect(entries[1]?.data.equals(TINY_PDF)).toBe(true);
    expect(entries[2]?.data.equals(Buffer.from([0, 1, 2]))).toBe(true);
  });

  it("rejects a wrong password without emitting garbage", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });

    await expect(collectNamed(Readable.from([zip]), "wrong")).rejects.toThrow(
      /invalid zip password/,
    );
  });

  it("skips filter misses without yielding them", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const entries = await collectNamed(chunked(zip), "secret", (name) => name.endsWith(".pdf"));

    expect(entries.map((e) => e.path)).toEqual(["nested/b.pdf"]);
    expect(entries[0]?.data.equals(TINY_PDF)).toBe(true);
  });

  it("still fails the password on an encrypted filter skip", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });

    await expect(collectNamed(Readable.from([zip]), "nope", () => false)).rejects.toThrow(
      /invalid zip password/,
    );
  });

  it("pipelines the middle PDF to equal fixture bytes", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const chunks: Buffer[] = [];

    for await (const entry of unzipEncrypted(chunked(zip, 5), {
      password: "secret",
    })) {
      if (!isPdfPath(entry.path)) {
        await entry.autodrain();
        continue;
      }
      await pipeline(
        entry,
        new Writable({
          write(chunk, _enc, cb) {
            const buf = Buffer.from(chunk);

            if (chunks.length === 0) {
              expect(isPdfMagic(buf)).toBe(true);
            }
            chunks.push(buf);
            cb();
          },
        }),
      );
    }
    expect(Buffer.concat(chunks).equals(TINY_PDF)).toBe(true);
  });

  it("skips a 16-byte data descriptor after a sized encrypted body", async () => {
    const zip = writeZip([{ data: Buffer.from("hello\n"), method: 0, name: "a.txt" }], {
      dataDescriptor: "16",
      password: "secret",
    });
    const entries = await collectNamed(Readable.from([zip]), "secret");

    expect(entries).toHaveLength(1);
    expect(entries[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });

  it("skips a 12-byte data descriptor after a sized stored body", async () => {
    const zip = writeZip(
      [
        { data: Buffer.from("hello\n"), method: 0, name: "a.txt" },
        { data: Buffer.from("world\n"), method: 0, name: "b.txt" },
      ],
      { dataDescriptor: "12" },
    );
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries.map((e) => e.data.toString())).toEqual(["hello\n", "world\n"]);
  });

  it("validates the data descriptor when the local header carries no crc", async () => {
    const zip = writeZip(
      [{ data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" }],
      {
        dataDescriptor: "16",
        zeroLocalCrc: true,
      },
    );
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries[0]?.data.toString()).toBe("hello\n");
  });

  it.each([
    ["crc", /crc mismatch: a\.txt/],
    ["uncompressedSize", /size mismatch: a\.txt/],
    ["compressedSize", /size mismatch: a\.txt/],
  ] as const)("rejects a data descriptor with a wrong %s", async (field, message) => {
    const zip = writeZip(
      [{ data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" }],
      {
        corruptDescriptor: field,
        dataDescriptor: "16",
        zeroLocalCrc: true,
      },
    );

    await expect(collectNamed(Readable.from([zip]), "")).rejects.toThrow(message);
  });

  it("decodes a latin1 name when the UTF-8 flag is unset", async () => {
    const zip = writeZip([{ data: Buffer.from("ok"), method: 0, name: "café.txt" }], {
      utf8: false,
    });
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries[0]?.path).toBe("café.txt");
  });

  it("decrypts with a latin1 password encoding", async () => {
    const password = "café";
    const zip = writeZip([{ data: Buffer.from("hello\n"), method: 0, name: "a.txt" }], {
      password,
      passwordEncoding: "latin1",
    });
    const entries = await collectNamed(Readable.from([zip]), password, undefined, {
      passwordEncoding: "latin1",
    });

    expect(entries[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });
});
