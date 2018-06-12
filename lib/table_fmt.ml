open Core
open Oleveldb_lib_util
open Stdint

type block_handle = {offset: int; size: int}

let empty_block_handle = {offset= 0; size= 0}

let block_handle_encode_to bh =
  let open Uint64 in
  let dst = Slice.create 20 in
  Coding.append_varint64 dst (of_int bh.offset) ;
  Coding.append_varint64 dst (of_int bh.size) ;
  dst


let block_handle_decode_from s =
  let open Option in
  Coding.decode_varint64 s
  >>= fun offset' ->
  return (Uint64.to_int offset')
  >>= fun offset ->
  Coding.decode_varint64 s
  >>= fun size' ->
  return (Uint64.to_int size') >>= fun size -> return {offset; size}


let block_handle_set_offset bh offset = {bh with offset}

let block_handle_get_offset bh = bh.offset

let block_handle_set_size bh size = {bh with size}

let block_handle_get_size bh = bh.size

(* =======================   footer   ================ *)
type footer = {metaindex_handle: block_handle; index_handle: block_handle}

let empty_footer =
  {metaindex_handle= empty_block_handle; index_handle= empty_block_handle}


let footer_set_metaindex_handle t bh = {t with metaindex_handle= bh}

let footer_set_index_handle t bh = {t with index_handle= bh}

let footer_get_metaindex_handle t = t.metaindex_handle

let footer_get_index_handle t = t.index_handle

let footer_encode_to t =
  let rst = Slice.create 48 in
  let meta = block_handle_encode_to t.metaindex_handle in
  let index = block_handle_encode_to t.index_handle in
  Slice.add_slice rst meta ;
  Slice.add_slice rst index ;
  (* 20: 2 * 10, 10=max varint64 length *)
  Slice.add_string rst
    (String.make (40 - Slice.length meta - Slice.length index) '\000') ;
  (* put magic num *)
  Slice.add_string rst "66666666" ;
  rst


let footer_decode_from s =
  let open Option in
  block_handle_decode_from s
  >>= fun meta ->
  block_handle_decode_from s
  >>= fun index -> return {metaindex_handle= meta; index_handle= index}


let footer_encoded_length = 48L

(* ---------------------------------------- *)

(** util func for read block content  *)
let read_block handle file =
  let size = block_handle_get_size handle in
  let buf = Slice.create size in
  In_channel.seek file (Int64.of_int (block_handle_get_offset handle)) ;
  let _ = Slice.input buf file size in
  buf
