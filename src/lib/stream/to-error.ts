/** Coerce a thrown value to Error without losing an existing Error instance. */
export function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}
