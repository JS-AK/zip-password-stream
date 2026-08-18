import { Readable, Writable } from "node:stream";

import { describe, expect, it } from "vitest";

import { ZIP_FLAG_ENCRYPTED } from "./zip/constants.js";
import { unzipEncrypted } from "./unzip.js";
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

describe("zipEncrypted", () => {
  it("writes a deflate entry that unzipEncrypted can read", async () => {
    const { dest, toBuffer } = collectWritable();
    const zipWriter = zipEncrypted(dest);

    await zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]));
    await zipWriter.end();

    const seen: { data: Buffer; path: string }[] = [];

    for await (const entry of unzipEncrypted(Readable.from([toBuffer()]), {})) {
      seen.push({ data: await collect(entry), path: entry.path });
    }

    expect(seen).toHaveLength(1);
    expect(seen[0]?.path).toBe("a.txt");
    expect(seen[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });

  it("treats an empty-string password as a real ZipCrypto password", async () => {
    const { dest, toBuffer } = collectWritable();
    const zipWriter = zipEncrypted(dest, { password: "" });

    await zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]));
    await zipWriter.end();

    const zip = toBuffer();

    expect(zip.readUInt16LE(6) & ZIP_FLAG_ENCRYPTED).toBe(ZIP_FLAG_ENCRYPTED);

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {})) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/password required/);

    const seen: { data: Buffer; path: string }[] = [];

    for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "" })) {
      seen.push({ data: await collect(entry), path: entry.path });
    }

    expect(seen).toHaveLength(1);
    expect(seen[0]?.path).toBe("a.txt");
    expect(seen[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "wrong" })) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/invalid zip password/);
  });

  it("throws zip already closed on a second end()", async () => {
    const { dest } = collectWritable();
    const zipWriter = zipEncrypted(dest);

    await zipWriter.end();

    await expect(zipWriter.end()).rejects.toThrow(/zip already closed/);
  });

  it("throws unsafe entry name for a parent-directory segment", async () => {
    const { dest } = collectWritable();
    const zipWriter = zipEncrypted(dest);

    await expect(zipWriter.add("foo/../passwd", Readable.from([Buffer.from("x")]))).rejects.toThrow(
      /unsafe entry name/,
    );
  });
});
