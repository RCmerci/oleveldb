type t

val create : int -> t

val from_string : string -> t

val from_substring : string -> pos:int -> len:int -> t

val length : t -> int

val add_string : t -> string -> unit

val add_char : t -> char -> unit

val add_slice : t -> t -> unit

val clear : t -> unit

val strip_head : t -> int -> string
(** return stripped head string *)

val to_string : t -> string

val to_substring : t -> pos:int -> len:int -> string

val output : t -> out_channel -> unit

val input : t -> in_channel -> int -> int

val get : t -> int -> char option

val get_exn : t -> int -> char

val set : t -> int -> char -> unit

val copy : t -> t
