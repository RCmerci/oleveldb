open Core
open Stdint
open Oleveldb_lib_util

module Make (Policy : Filter_policy.S) = struct
  (*
+-------------------+
|    filter1        |
+-------------------+
|    filter2        |
+-------------------+
|    filter3        |
+-------------------+
|    filterN        |
+-------------------+ <-------+
|  filter_offset1   |	      |
+-------------------+	      |
|  filter_offset2   |	      |
+-------------------+	      |
|  filter_offset3   |	      |
+-------------------+	      |
|  filter_offsetN   |	      |
+-------------------+	      |
|  array_offset     |---------+
+-------------------+
| filter_base_lg(11)|
+-------------------+
 *)
  type t = {data: Slice.t; base_lg: int; filter_offset: int; num: int}

  let create data =
    let size = Slice.length data in
    let base_lg =
      let s = Slice.to_substring data ~pos:(size - 1) ~len:1 in
      Char.of_string s |> Char.to_int
    in
    let open Result in
    let data' = Slice.copy data in
    Slice.strip_head_2 data' (size - 5) ;
    Result.of_option ~error:"decode filter block array_offset "
      (Coding.decode_fix32 data')
    >>= fun filter_offset' ->
    let filter_offset = Uint32.to_int filter_offset' in
    let num = (size - 5 - filter_offset) / 4 in
    return {data; base_lg; num; filter_offset}


  let key_may_match t key block_offset =
    let open Result in
    let index = block_offset lsr t.base_lg in
    if index >= t.num then return true
    else
      let data' = Slice.copy t.data in
      Slice.strip_head_2 data' (t.filter_offset + index * 4) ;
      of_option ~error:"decode filter_offset"
        Option.(
          Coding.decode_fix32 data'
          >>= fun start ->
          Coding.decode_fix32 data' >>= fun limit -> return (start, limit))
      >>= fun (start', limit') ->
      let start = Uint32.to_int start' in
      let limit = Uint32.to_int limit' in
      if start = limit then return false
      else if start < limit && limit <= t.filter_offset then
        let filter_data =
          Slice.to_substring t.data ~pos:start ~len:(limit - start)
          |> Slice.from_string
        in
        return (Policy.key_may_match Policy.dummy key filter_data)
      else return true

end

module BloomFilterBlockReader = Make (Bloomfilter)
