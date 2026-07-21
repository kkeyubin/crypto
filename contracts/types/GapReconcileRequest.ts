// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

/**
 * @minItems 1
 * @maxItems 100
 */
export type PartitionIds = [string, ...string[]];

interface GapReconcileRequestShape {
  partition_ids: PartitionIds;
}

export type GapReconcileRequest = DeepReadonly<GapReconcileRequestShape>;
