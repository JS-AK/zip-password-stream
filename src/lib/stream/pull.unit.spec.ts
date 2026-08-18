import { Readable } from "node:stream";

import { describe, expect, it } from "vitest";

import { asBuffer, createPull } from "./pull.js";

function chunked(data: Buffer, sizes: number[]): Readable {
  const chunks: Buffer[] = [];
  let offset = 0;
  let i = 0;

  while (offset < data.length) {
    const size = sizes[i % sizes.length] ?? 1;

    chunks.push(Buffer.from(data.subarray(offset, offset + size)));
    offset += size;
    i += 1;
  }

  return Readable.from(chunks);
}

describe("createPull", () => {
  it("reads exact N bytes across tiny chunks and leftover", async () => {
    const data = Buffer.from("abcdefghijklmnop");
    const pull = createPull(chunked(data, [1, 2, 3]));

    const a = await pull.read(5);

    expect(a?.equals(Buffer.from("abcde"))).toBe(true);

    const b = await pull.read(1);

    expect(b?.equals(Buffer.from("f"))).toBe(true);

    const c = await pull.read(10);

    expect(c?.equals(Buffer.from("ghijklmnop"))).toBe(true);
  });

  it("discard skips bytes without returning them", async () => {
    const data = Buffer.from("0123456789");
    const pull = createPull(chunked(data, [2, 1, 3]));

    await pull.discard(4);
    const rest = await pull.read(6);

    expect(rest?.equals(Buffer.from("456789"))).toBe(true);
  });

  it("returns leftover then null at EOF", async () => {
    const pull = createPull(chunked(Buffer.from("xyz"), [1]));
    const short = await pull.read(10);

    expect(short?.equals(Buffer.from("xyz"))).toBe(true);
    expect(await pull.read(1)).toBeNull();
    expect(await pull.read(4)).toBeNull();
  });

  it("throws when discard hits EOF before n bytes", async () => {
    const pull = createPull(chunked(Buffer.from("ab"), [1]));

    await expect(pull.discard(50)).rejects.toThrow(/unexpected EOF/);
  });

  it("keeps byte order over many single-byte chunks", async () => {
    const data = Buffer.alloc(4096);

    for (let i = 0; i < data.length; i++) {
      data[i] = i & 0xff;
    }
    const pull = createPull(chunked(data, [1]));

    const parts: Buffer[] = [];

    for (;;) {
      const chunk = await pull.read(700);

      if (!chunk || chunk.length === 0) {
        break;
      }
      parts.push(chunk);
    }
    expect(Buffer.concat(parts).equals(data)).toBe(true);
  });

  it("interleaves read and discard across chunk boundaries", async () => {
    const data = Buffer.from("AAAABBBBCCCCDDDD");
    const pull = createPull(chunked(data, [3, 5]));

    expect((await pull.read(4))?.toString()).toBe("AAAA");
    await pull.discard(4);
    expect((await pull.read(4))?.toString()).toBe("CCCC");
    await pull.discard(4);
    expect(await pull.read(1)).toBeNull();
  });

  it("skips zero-length chunks from the source", async () => {
    const pull = createPull(
      Readable.from([Buffer.alloc(0), Buffer.from("ab"), Buffer.alloc(0), Buffer.from("cd")]),
    );

    expect((await pull.read(4))?.toString()).toBe("abcd");
  });

  it("dispose destroys the source and is idempotent", async () => {
    const source = chunked(Buffer.from("0123456789"), [2]);
    const pull = createPull(source);

    await pull.read(2);

    await pull.dispose();
    await pull.dispose();

    expect(source.destroyed).toBe(true);
  });

  it("returns unread bytes before queued leftover", async () => {
    const pull = createPull(Readable.from([Buffer.from("cd")]));

    pull.unread(Buffer.from("ab"));

    expect((await pull.read(4))?.toString()).toBe("abcd");
  });

  it("unread after a partial read sits before the rest of that chunk", async () => {
    const pull = createPull(Readable.from([Buffer.from("abcdefgh")]));

    expect((await pull.read(3))?.toString()).toBe("abc");
    pull.unread(Buffer.from("XY"));

    expect((await pull.read(7))?.toString()).toBe("XYdefgh");
  });

  it("treats an empty unread as a no-op", async () => {
    const pull = createPull(Readable.from([Buffer.from("ab")]));

    pull.unread(Buffer.alloc(0));

    expect((await pull.read(2))?.toString()).toBe("ab");
  });
});

describe("asBuffer", () => {
  it("keeps a Uint8Array view's byteOffset", () => {
    const backing = new Uint8Array([0, 1, 2, 3, 4]);
    const view = backing.subarray(1, 4);

    expect(asBuffer(view).equals(Buffer.from([1, 2, 3]))).toBe(true);
  });

  it("returns a Buffer without copying", () => {
    const chunk = Buffer.from("ab");

    expect(asBuffer(chunk)).toBe(chunk);
  });

  it("encodes a string chunk (decodeStrings false)", () => {
    expect(asBuffer("ab").equals(Buffer.from("ab"))).toBe(true);
  });

  it("throws on an unsupported chunk type", () => {
    expect(() => asBuffer(1)).toThrow(/unsupported readable chunk type/);
  });
});
