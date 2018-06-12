open Core
open Oleveldb_lib_util

module BloomFilterBlockReader : sig
  type t

  val create : Slice.t -> (t, string) Result.t

  val key_may_match : t -> Slice.t -> int -> (bool, string) Result.t
end
