open Core
open Oleveldb_lib_util
open Log_fmt

type t = {dest: Out_channel.t; mutable block_offset: int}

let create dest = {dest; block_offset= 0}

let emit_physical_record t tp slice fragment_length =
  let record = Slice.strip_head slice fragment_length in
  let len1 = fragment_length land 0xff |> Char.of_int_exn in
  let len2 = fragment_length lsr 8 land 0xff |> Char.of_int_exn in
  let buf = Slice.create (kHeaderSize + fragment_length) in
  Slice.add_char buf len1 ;
  Slice.add_char buf len2 ;
  Slice.add_char buf (Char.of_int_exn tp) ;
  Slice.add_string buf record ;
  Slice.output buf t.dest ;
  t.block_offset <- t.block_offset + fragment_length + kHeaderSize ;
  Status.OK


let add_record t slice =
  let left = Slice.length slice in
  let rec aux left is_begin =
    let leftover = kBlockSize - t.block_offset in
    assert (leftover >= 0) ;
    let () =
      if leftover < kHeaderSize then
        let () =
          if leftover > 0 then
            Slice.output
              (Slice.from_substring "\000\000\000" 0 leftover)
              t.dest
        in
        t.block_offset <- 0
    in
    assert (kBlockSize - t.block_offset - kHeaderSize >= 0) ;
    let avail = kBlockSize - t.block_offset - kHeaderSize in
    let fragment_length = if left < avail then left else avail in
    let is_end = phys_equal left fragment_length in
    let tp =
      if is_begin && is_end then FullType
      else if is_begin then FirstType
      else if is_end then LastType
      else MiddleType
    in
    let stat =
      emit_physical_record t (record_to_int tp) slice fragment_length
    in
    let left_ = left - fragment_length in
    if left_ > 0 && Status.isOK stat then aux left_ false else stat
  in
  aux left true
