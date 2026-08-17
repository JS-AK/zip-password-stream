import type { Readable } from "node:stream";

/**
 * Sequential reader over a Node Readable: exact `read(n)` / `discard(n)`
 * without seeking, so unzip can parse headers then pull body on demand.
 */
export type PullReader = {
  /** Return exactly `n` bytes, or fewer/null at EOF. */
  read(n: number): Promise<Buffer | null>;
  /** Skip `n` bytes without copying them out to a consumer. */
  discard(n: number): Promise<void>;
  /** Release the source stream. Idempotent. */
  dispose(): Promise<void>;
};

/** Consumed slots are nulled out so a long queue does not pin memory. */
const EMPTY = Buffer.alloc(0);

/** Compact the queue instead of `shift()`, which is O(n) on large arrays. */
const COMPACT_AFTER = 1024;

/** Typical zip body read size; callers request this from `read`/`discard`. */
export const PULL_CHUNK_SIZE = 64 * 1024;

/**
 * Wrap a stream chunk without copying. A producer must not mutate a chunk after
 * pushing it, so queued chunks are treated as read-only and never modified here.
 */
function asBuffer(value: unknown): Buffer {
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  }
  if (typeof value === "string") {
    return Buffer.from(value);
  }
  throw new Error("unsupported readable chunk type");
}

/**
 * Pull-driven adapter: leftover buffer plus one incoming chunk at a time.
 * `read`/`discard` never grow unbounded past the requested n plus one chunk.
 */
export function createPull(source: Readable): PullReader {
  const iter = source[Symbol.asyncIterator]();
  const queue: Buffer[] = [];
  /** Index of the first chunk still holding unserved bytes. */
  let index = 0;
  /** Bytes already served from `queue[index]`. */
  let offset = 0;
  let queued = 0;
  let ended = false;
  let disposed = false;

  function headChunk(): Buffer {
    const chunk = queue[index];

    if (!chunk) {
      throw new Error("pull queue underflow");
    }

    return chunk;
  }

  /**
   * Pull from the source until `queued >= min` or EOF. Empty chunks are skipped
   * so they do not stall a discard loop waiting for bytes.
   */
  async function fill(min: number): Promise<void> {
    while (queued < min && !ended) {
      const { done, value } = await iter.next();

      if (done) {
        ended = true;

        return;
      }
      const chunk = asBuffer(value);

      if (chunk.length === 0) {
        continue;
      }
      queue.push(chunk);
      queued += chunk.length;
    }
  }

  /** Advance past `count` bytes; caller guarantees `count <= queued`. */
  function drop(count: number): void {
    let left = count;

    while (left > 0) {
      const chunk = headChunk();
      const avail = chunk.length - offset;

      if (avail > left) {
        offset += left;
        left = 0;
        break;
      }
      queue[index] = EMPTY;
      index += 1;
      offset = 0;
      left -= avail;
    }
    queued -= count;
    if (index === queue.length) {
      queue.length = 0;
      index = 0;
    } else if (index >= COMPACT_AFTER) {
      queue.splice(0, index);
      index = 0;
    }
  }

  /** Take `count` bytes; caller guarantees `count <= queued` and `count > 0`. */
  function take(count: number): Buffer {
    const chunk = headChunk();

    if (chunk.length - offset >= count) {
      const out = chunk.subarray(offset, offset + count);

      drop(count);

      return out;
    }
    const out = Buffer.allocUnsafe(count);
    let written = 0;

    while (written < count) {
      const head = headChunk();
      const slice = head.subarray(offset, Math.min(head.length, offset + (count - written)));

      slice.copy(out, written);
      written += slice.length;
      drop(slice.length);
    }

    return out;
  }

  /** Exact `n` bytes, or fewer/null at EOF — never overshoots, so the next header stays aligned. */
  async function read(n: number): Promise<Buffer | null> {
    if (n <= 0) {
      return Buffer.alloc(0);
    }
    await fill(n);
    if (queued === 0) {
      return null;
    }

    return take(Math.min(n, queued));
  }

  /**
   * Skip `n` bytes without copying them to a consumer. Short source is fatal:
   * a local header already promised that many ciphertext bytes.
   */
  async function discard(n: number): Promise<void> {
    let left = n;

    while (left > 0) {
      if (queued === 0) {
        await fill(1);
        if (queued === 0) {
          throw new Error("unexpected EOF while discarding zip data");
        }
      }
      const step = Math.min(left, queued);

      drop(step);
      left -= step;
    }
  }

  /** Drop queued chunks and close the source iterator so the zip is not held open. */
  async function dispose(): Promise<void> {
    if (disposed) {
      return;
    }
    disposed = true;
    queue.length = 0;
    index = 0;
    offset = 0;
    queued = 0;
    await iter.return?.();
  }

  return { discard, dispose, read };
}
