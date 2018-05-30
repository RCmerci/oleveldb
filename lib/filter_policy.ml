open Oleveldb_lib_util

module type S = sig
  type t

  val name : string

  val create : int -> t

  val create_filter : t -> Slice.t list -> Slice.t -> unit

  val key_may_match : t -> Slice.t -> Slice.t -> bool
end
