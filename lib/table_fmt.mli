open Core
open Oleveldb_lib_util

type block_handle

val empty_block_handle : block_handle

val block_handle_encode_to : block_handle -> Slice.t

val block_handle_decode_from : Slice.t -> block_handle Option.t

val block_handle_set_offset : block_handle -> int -> block_handle

val block_handle_get_offset : block_handle -> int

val block_handle_set_size : block_handle -> int -> block_handle

val block_handle_get_size : block_handle -> int

type footer

val empty_footer : footer

val footer_set_metaindex_handle : footer -> block_handle -> footer

val footer_set_index_handle : footer -> block_handle -> footer

val footer_get_metaindex_handle : footer -> block_handle

val footer_get_index_handle : footer -> block_handle

val footer_encode_to : footer -> Slice.t

val footer_decode_from : Slice.t -> footer Option.t

val footer_encoded_length : int64
(** 2 * 20 + 8 *)

val read_block : block_handle -> In_channel.t -> Slice.t
(** util func for read block content  *)
