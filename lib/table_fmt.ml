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


let block_handle_set_offset bh offset = {bh with offset}

let block_handle_get_offset bh = bh.offset

let block_handle_set_size bh size = {bh with size}

let block_handle_get_size bh = bh.size

(* footer *)
type footer = {metaindex_handle: block_handle; index_handle: block_handle}

let empty_footer =
  {metaindex_handle= empty_block_handle; index_handle= empty_block_handle}


let footer_set_metaindex_handle t bh = {t with metaindex_handle= bh}

let footer_set_index_handle t bh = {t with index_handle= bh}

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
