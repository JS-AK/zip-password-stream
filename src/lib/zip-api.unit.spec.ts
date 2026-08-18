import { Readable, Writable } from "node:stream";

import { describe, expect, it } from "vitest";

import { Zip } from "./zip-api.js";
import { zipEncrypted } from "./zip-write.js";

function collectWritable(): { dest: Writable; toBuffer: () => Buffer } {
  const chunks: Buffer[] = [];
  const dest = new Writable({
    write(chunk: Buffer, _encoding, callback): void {
      chunks.push(Buffer.from(chunk));
      callback();
    },
  });

  return {
    dest,
    toBuffer(): Buffer {
      return Buffer.concat(chunks);
    },
  };
}

async function collect(entry: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];

  for await (const chunk of entry) {
    chunks.push(Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

describe("Zip.open", () => {
  it("throws when only() is called with no kinds", () => {
    const handle = Zip.open(Readable.from([])) as { only: (...a: string[]) => unknown };

    expect(() => handle.only()).toThrow(/only\(\) requires a kind/);
  });

  it("throws when only() is called with undefined", () => {
    const handle = Zip.open(Readable.from([])) as {
      only: (...a: (string | undefined)[]) => unknown;
    };

    expect(() => handle.only(undefined)).toThrow(/only\(\) requires a kind/);
  });

  it("throws when only() is called with a hole in the kinds list", () => {
    const handle = Zip.open(Readable.from([])) as {
      only: (...a: (string | undefined)[]) => unknown;
    };

    expect(() => handle.only("pdf", undefined)).toThrow(/only\(\) requires a kind/);
  });

  it("throws zip already opened on a second iterator", async () => {
    const { dest, toBuffer } = collectWritable();
    const zipWriter = zipEncrypted(dest);

    await zipWriter.end();
    const handle = Zip.open(Readable.from([toBuffer()]));

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

  it("round-trips Zip.create through Zip.open", async () => {
    const { dest, toBuffer } = collectWritable();
    const zipWriter = Zip.create(dest);

    await zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]));
    await zipWriter.end();

    const paths: string[] = [];

    for await (const entry of Zip.open(Readable.from([toBuffer()]))) {
      paths.push(entry.path);
      expect((await collect(entry)).equals(Buffer.from("hello\n"))).toBe(true);
    }

    expect(paths).toEqual(["a.txt"]);
  });
});

describe("Zip.create", () => {
  it("throws zip already started when password() runs after add()", async () => {
    const { dest } = collectWritable();
    const zipWriter = Zip.create(dest);

    await zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]));

    expect(() => zipWriter.password("secret")).toThrow(/zip already started/);
    await zipWriter.end();
  });
});
