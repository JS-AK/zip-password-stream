import { Readable, Writable } from "node:stream";

import { describe, expect, it } from "vitest";

import { FIXTURE_FILES, TINY_JPEG, TINY_PDF, writeZip } from "./test-support/write-zip.js";
import { ZIP_METHOD_DEFLATE } from "../lib/zip/constants.js";
import { Zip } from "../lib/index.js";

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

describe("Zip.open", () => {
  it("yields the same encrypted fixture paths as unzipEncrypted", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const paths: string[] = [];
    let pdf: Buffer | undefined;

    for await (const entry of Zip.open(chunked(zip)).password("secret")) {
      paths.push(entry.path);
      const data = await collect(entry);

      if (entry.path === "nested/b.pdf") {
        pdf = data;
      }
    }

    expect(paths).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
    expect(pdf?.equals(TINY_PDF)).toBe(true);
  });

  it("throws when a second iterator is created", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const handle = Zip.open(Readable.from([zip])).password("secret");

    for await (const entry of handle) {
      await collect(entry);
    }

    await expect(
      (async () => {
        for await (const entry of handle) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/zip already opened/);
  });

  it("throws when password() is called after iteration started", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const handle = Zip.open(Readable.from([zip])).password("secret");
    let sawBuilderError = false;

    for await (const entry of handle) {
      await collect(entry);
      if (!sawBuilderError) {
        expect(() => handle.password("secret")).toThrow(/zip already opened/);
        sawBuilderError = true;
      }
    }

    expect(sawBuilderError).toBe(true);
  });
});

describe("Zip.only / filter / limits", () => {
  it("only('pdf') yields the nested PDF and not txt or bin", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const paths: string[] = [];
    let pdf: Buffer | undefined;

    for await (const entry of Zip.open(Readable.from([zip]))
      .password("secret")
      .only("pdf")) {
      paths.push(entry.path);
      pdf = await collect(entry);
    }

    expect(paths).toEqual(["nested/b.pdf"]);
    expect(pdf?.equals(TINY_PDF)).toBe(true);
  });

  it("only('pdf', 'jpeg') yields both kinds and not txt", async () => {
    const zip = writeZip([
      { data: Buffer.from("hello\n"), method: 0, name: "a.txt" },
      { data: TINY_PDF, method: 0, name: "nested/b.pdf" },
      { data: TINY_JPEG, method: 0, name: "photo.jpg" },
    ]);
    const paths: string[] = [];

    for await (const entry of Zip.open(Readable.from([zip])).only("pdf", "jpeg")) {
      paths.push(entry.path);
      await collect(entry);
    }

    expect(paths).toEqual(["nested/b.pdf", "photo.jpg"]);
  });

  it("only('pdf') and filter AND skips a PDF outside nested/", async () => {
    const zip = writeZip([
      { data: TINY_PDF, method: 0, name: "top.pdf" },
      { data: TINY_PDF, method: 0, name: "nested/b.pdf" },
      { data: Buffer.from("hello\n"), method: 0, name: "nested/c.txt" },
    ]);
    const paths: string[] = [];

    for await (const entry of Zip.open(Readable.from([zip]))
      .only("pdf")
      .filter((name) => name.startsWith("nested/"))) {
      paths.push(entry.path);
      await collect(entry);
    }

    expect(paths).toEqual(["nested/b.pdf"]);
  });

  it("only() with zero kinds throws", () => {
    const zip = writeZip(FIXTURE_FILES);
    const handle = Zip.open(Readable.from([zip])) as { only: (...a: string[]) => unknown };

    expect(() => handle.only()).toThrow(/only\(\) requires a kind/);
  });

  it("only() with undefined throws", () => {
    const zip = writeZip(FIXTURE_FILES);
    const handle = Zip.open(Readable.from([zip])) as {
      only: (...a: (string | undefined)[]) => unknown;
    };

    expect(() => handle.only(undefined)).toThrow(/only\(\) requires a kind/);
  });

  it("only() with a hole in the kinds list throws", () => {
    const zip = writeZip(FIXTURE_FILES);
    const handle = Zip.open(Readable.from([zip])) as {
      only: (...a: (string | undefined)[]) => unknown;
    };

    expect(() => handle.only("pdf", undefined)).toThrow(/only\(\) requires a kind/);
  });

  it("still fails the password when only() skips encrypted entries", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });

    await expect(
      (async () => {
        for await (const entry of Zip.open(Readable.from([zip]))
          .password("wrong")
          .only("pdf")) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/invalid zip password/);
  });

  it("stops a deflate entry that grows past maxEntrySize", async () => {
    const bomb = Buffer.alloc(1024 * 1024, 0x00);
    const zip = writeZip([{ data: bomb, method: ZIP_METHOD_DEFLATE, name: "bomb.bin" }]);

    await expect(
      (async () => {
        for await (const entry of Zip.open(Readable.from([zip])).maxEntrySize(64 * 1024)) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/entry exceeds maxEntrySize/);
  });

  it("decrypts with a latin1 password encoding", async () => {
    const password = "café";
    const zip = writeZip([{ data: Buffer.from("hello\n"), method: 0, name: "a.txt" }], {
      password,
      passwordEncoding: "latin1",
    });
    const bodies: Buffer[] = [];

    for await (const entry of Zip.open(Readable.from([zip]))
      .password(password)
      .passwordEncoding("latin1")) {
      bodies.push(await collect(entry));
    }

    expect(bodies).toHaveLength(1);
    expect(bodies[0]?.equals(Buffer.from("hello\n"))).toBe(true);
  });
});

describe("Zip.create", () => {
  it("round-trips a password zip through Zip.open", async () => {
    const chunks: Buffer[] = [];
    const dest = new Writable({
      write(chunk: Buffer, _encoding, callback): void {
        chunks.push(Buffer.from(chunk));
        callback();
      },
    });
    const zipWriter = Zip.create(dest)
      .password("secret")
      .maxEntrySize(64 * 1024);

    await zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]));
    await zipWriter.add("nested/b.pdf", Readable.from([TINY_PDF]));
    await zipWriter.end();

    const seen: { data: Buffer; path: string }[] = [];

    for await (const entry of Zip.open(Readable.from([Buffer.concat(chunks)])).password("secret")) {
      seen.push({ data: await collect(entry), path: entry.path });
    }

    expect(seen.map((e) => e.path)).toEqual(["a.txt", "nested/b.pdf"]);
    expect(seen[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
    expect(seen[1]?.data.equals(TINY_PDF)).toBe(true);
  });

  it("throws when password() is called after add()", async () => {
    const dest = new Writable({
      write(_chunk: Buffer, _encoding, callback): void {
        callback();
      },
    });
    const zipWriter = Zip.create(dest);

    await zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]));

    expect(() => zipWriter.password("secret")).toThrow(/zip already started/);
    await zipWriter.end();
  });
});
