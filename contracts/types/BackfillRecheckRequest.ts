// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type PartitionId = string;

interface BackfillRecheckRequestShape {
  partition_id: PartitionId;
}

export type BackfillRecheckRequest = DeepReadonly<BackfillRecheckRequestShape>;
