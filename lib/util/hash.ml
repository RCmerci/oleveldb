open Core
open Stdint
open Coding

let murmur3_32 (key: Slice.t) seed =
  let open Uint32 in
  let key' = Slice.from_string (Slice.to_string key) in
  let c1 = of_int 0xcc9e2d51 in
  let c2 = of_int 0x1b873593 in
  let r1 = 15 in
  let r2 = 13 in
  let m = of_int 5 in
  let n = of_int 0xe6546b64 in
  let hash = of_int seed in
  (* for each 4 bytes  *)
  let each_4_byte_chunk chunk hash =
    let k' = of_bytes_little_endian (Slice.to_string chunk) 0 in
    let k'' = k' * c1 in
    let k''' =
      logor (shift_left k'' r1) (shift_right_logical k'' Pervasives.(32 - r1))
    in
    let k = k''' * c2 in
    let hash' = logxor hash k in
    let hash'' =
      logor (shift_left hash' r2)
        (shift_right_logical hash' Pervasives.(32 - r2))
    in
    let hash''' = hash'' * m + n in
    hash'''
  in
  (* for remaining bytes *)
  let remain_chunk chunk hash =
    let () =
      match Slice.length chunk with
      | 1 -> Slice.add_string chunk "\000\000\000"
      | 2 -> Slice.add_string chunk "\000\000"
      | 3 -> Slice.add_string chunk "\000"
      | _ -> assert false
    in
    let k' = of_bytes_little_endian (Slice.to_string chunk) 0 in
    let k'' = k' * c1 in
    let k''' =
      logor (shift_left k'' r1) (shift_right_logical k'' Pervasives.(32 - r1))
    in
    let k = k''' * c2 in
    let hash' = logxor hash k in
    hash'
  in
  let rec aux hash =
    let chunk = Slice.strip_head key' 4 |> Slice.from_string in
    let len = Slice.length chunk in
    if len < 4 && len > 0 then remain_chunk chunk hash
    else if len = 0 then hash
    else
      let hash' = each_4_byte_chunk chunk hash in
      aux hash'
  in
  let hash' = aux hash in
  let hash'' = logxor hash' (of_int (Slice.length key)) in
  let hash''' = logxor hash'' (shift_right_logical hash'' 16) in
  let hash'''' = hash''' * of_int 0x85ebca6b in
  let hash''''' = logxor hash'''' (shift_right_logical hash'''' 13) in
  let hash'''''' = hash''''' * of_int 0xc2b2ae35 in
  let hash''''''' = logxor hash'''''' (shift_right_logical hash'''''' 16) in
  hash'''''''
