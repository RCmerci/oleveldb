open Oleveldb_lib_util

(** default block_restart_interval: 16 *)
type config_option = {block_restart_interval: int}

val default_config_option : config_option

type t

val create : config_option -> t

val reset : t -> t

val add : t -> Slice.t -> Slice.t -> unit

val finish : t -> Slice.t

val current_size_estimate : t -> int

val is_empty : t -> bool
