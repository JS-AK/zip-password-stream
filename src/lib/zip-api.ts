import type { Readable, Writable } from "node:stream";

import { type DetectKind, isKindPath } from "./detect/detect.js";
import { type UnzipOptions, type ZipEntry, unzipEncrypted } from "./unzip.js";
import { type ZipWriteOptions, type ZipWriter, zipEncrypted } from "./zip-write.js";

/** Public error when a second iterator or a late builder call is made. */
const ZIP_ALREADY_OPENED = "zip already opened";

/** Public error when a write builder method runs after the first `add`/`end`. */
const ZIP_ALREADY_STARTED = "zip already started";

/** Public error when `.only()` is called with no kinds. */
const ONLY_REQUIRES_KIND = "only() requires a kind";

/** Password, encoding, and size fields shared by `Zip.open` and `Zip.create`. */
type SharedZipOptions = Pick<
  UnzipOptions & ZipWriteOptions,
  "maxEntrySize" | "password" | "passwordEncoding"
>;

/**
 * Mutable option bag for the fluent builders. `toOptions` omits unset keys so
 * `undefined` is not passed through as if the caller had set the field.
 */
class ZipOptionBag {
  private pwd: string | undefined;
  private encoding: BufferEncoding | undefined;
  private maxSize: number | undefined;

  password(password: string): void {
    this.pwd = password;
  }

  passwordEncoding(encoding: BufferEncoding): void {
    this.encoding = encoding;
  }

  maxEntrySize(bytes: number): void {
    this.maxSize = bytes;
  }

  toOptions(): SharedZipOptions {
    const options: SharedZipOptions = {};

    if (this.pwd !== undefined) {
      options.password = this.pwd;
    }
    if (this.encoding !== undefined) {
      options.passwordEncoding = this.encoding;
    }
    if (this.maxSize !== undefined) {
      options.maxEntrySize = this.maxSize;
    }

    return options;
  }
}

/**
 * AND of `.only` kinds (`isKindPath`) and a user path predicate.
 * Omit the unzip filter entirely when neither is configured, so every entry yields.
 */
function combineFilters(
  kinds: readonly DetectKind[] | undefined,
  pathFilter: ((path: string) => boolean) | undefined,
): ((path: string) => boolean) | undefined {
  if (kinds === undefined && pathFilter === undefined) {
    return undefined;
  }

  return (path: string): boolean => {
    const kindOk = kinds === undefined || kinds.some((kind) => isKindPath(kind, path));
    const filterOk = pathFilter === undefined || pathFilter(path);

    return kindOk && filterOk;
  };
}

/**
 * Fluent one-pass unzip (`Zip.open`) and streaming zip writer (`Zip.create`).
 */
export class Zip implements AsyncIterable<ZipEntry> {
  private readonly source: Readable;
  private readonly opts = new ZipOptionBag();
  private started = false;
  private kinds: readonly DetectKind[] | undefined;
  private pathFilter: ((path: string) => boolean) | undefined;

  private constructor(source: Readable) {
    this.source = source;
  }

  /** Wrap a Node Readable. Does not read until `for await`. */
  static open(source: Readable): Zip {
    return new Zip(source);
  }

  /** Wrap a Node Writable. Does not write until the first `add` or `end`. */
  static create(dest: Writable): ZipCreate {
    return ZipCreate.wrap(dest);
  }

  /** ZipCrypto password. Last call wins. Omitted → unencrypted archives only. */
  password(password: string): this {
    this.assertNotStarted();
    this.opts.password(password);

    return this;
  }

  /** Password bytes encoding. Default UTF-8. Last call wins. */
  passwordEncoding(encoding: BufferEncoding): this {
    this.assertNotStarted();
    this.opts.passwordEncoding(encoding);

    return this;
  }

  /**
   * Yield entries whose basename matches any kind (same rules as `isKindPath`).
   * Replaces a previous `.only`. Combined with `.filter` using AND.
   * Runtime: throws if called with no kinds.
   */
  only(...kinds: [DetectKind, ...DetectKind[]]): this {
    this.assertNotStarted();
    if (kinds.length === 0 || kinds.some((kind) => kind === undefined)) {
      throw new Error(ONLY_REQUIRES_KIND);
    }
    this.kinds = kinds;

    return this;
  }

  /**
   * Yield when `fn(localHeaderName)` is true.
   * Last `.filter` wins. AND with `.only` when both are set.
   */
  filter(fn: (path: string) => boolean): this {
    this.assertNotStarted();
    this.pathFilter = fn;

    return this;
  }

  /** Uncompressed-byte cap per yielded entry. Last call wins. */
  maxEntrySize(bytes: number): this {
    this.assertNotStarted();
    this.opts.maxEntrySize(bytes);

    return this;
  }

  /**
   * Starts the parser. Call once.
   * Throws `zip already opened` on a second iterator or on builder use after start.
   */
  [Symbol.asyncIterator](): AsyncGenerator<ZipEntry, void, unknown> {
    this.assertNotStarted();
    this.started = true;

    return unzipEncrypted(this.source, this.toUnzipOptions());
  }

  private assertNotStarted(): void {
    if (this.started) {
      throw new Error(ZIP_ALREADY_OPENED);
    }
  }

  private toUnzipOptions(): UnzipOptions {
    const options: UnzipOptions = this.opts.toOptions();
    const filter = combineFilters(this.kinds, this.pathFilter);

    if (filter !== undefined) {
      options.filter = filter;
    }

    return options;
  }
}

/**
 * Fluent streaming zip writer. Construct only via `Zip.create`.
 * Builder methods do not write; the first `add` or `end` starts `zipEncrypted`.
 */
export class ZipCreate implements ZipWriter {
  private readonly dest: Writable;
  private readonly opts = new ZipOptionBag();
  private writer: ZipWriter | undefined;

  private constructor(dest: Writable) {
    this.dest = dest;
  }

  /** Used by `Zip.create` so callers cannot `new ZipCreate`. */
  static wrap(dest: Writable): ZipCreate {
    return new ZipCreate(dest);
  }

  /** ZipCrypto password. Last call wins until the first `add`/`end`. */
  password(password: string): this {
    this.assertIdle();
    this.opts.password(password);

    return this;
  }

  /** Password bytes encoding. Default UTF-8. Last call wins until start. */
  passwordEncoding(encoding: BufferEncoding): this {
    this.assertIdle();
    this.opts.passwordEncoding(encoding);

    return this;
  }

  /** Uncompressed-byte cap per `add`. Last call wins until start. */
  maxEntrySize(bytes: number): this {
    this.assertIdle();
    this.opts.maxEntrySize(bytes);

    return this;
  }

  /** Append one deflated entry (ZipCrypto when `.password` was set). */
  add(name: string, body: Readable): Promise<void> {
    return this.inner().add(name, body);
  }

  /** Write CD+EOCD and `dest.end()`. Required. */
  end(): Promise<void> {
    return this.inner().end();
  }

  private assertIdle(): void {
    if (this.writer !== undefined) {
      throw new Error(ZIP_ALREADY_STARTED);
    }
  }

  private inner(): ZipWriter {
    if (this.writer === undefined) {
      this.writer = zipEncrypted(this.dest, this.opts.toOptions());
    }

    return this.writer;
  }
}
