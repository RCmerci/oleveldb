open Stdint
open Oleveldb_lib_util

type value_type = TYPE_DELETION | TYPE_SEEK

val value_type_to_int : value_type -> int

val value_type_of_int : int -> value_type

val max_sequence : Uint64.t

module Internal_key : sig
  type t

  val decode_from : Slice.t -> t
  (** decode from slice  *)

  val decode_from_2 : Slice.t -> Uint64.t -> value_type -> t
  (** decode from user_key, sequence, value_type  *)

  val to_slice : t -> Slice.t

  val get_user_key : t -> Slice.t

  val get_value_type : t -> value_type

  val get_sequence : t -> Uint64.t
end

module type Internal_key_comparator_S = sig
  module User_comparator : Cmp.S

  val name : string

  val compare : Slice.t -> Slice.t -> Cmp.compare_result
end

module Internal_key_comparator_Make (User_comparator : Cmp.S) :
  Internal_key_comparator_S
