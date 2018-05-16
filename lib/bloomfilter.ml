open Core
open Oleveldb_lib_util
open Stdint

(**
m: bloomfilter array size
n: data size
bits_per_key = m/n
k: hash function num
*)
type t = {bits_per_key: int; k: int}

let create bits_per_key =
  let k_ = float_of_int bits_per_key *. 0.69 |> int_of_float in
  let k = if k_ < 1 then 1 else if k_ > 30 then 30 else k_ in
  {bits_per_key; k}


let bloom_hash k = Hash.murmur3_32 k 0xbc9f1d34

let create_filter t (keys: Slice.t list) dst =
  let n = List.length keys in
  let bits' = n * t.bits_per_key in
  let bits'' = if bits' < 64 then 64 else bits' in
  let bytes = (bits'' + 7) / 8 in
  let bits = bytes * 8 in
  let init_size = Slice.length dst in
  Slice.add_string dst (String.make bytes '\000') ;
  Slice.add_char dst (Char.of_int_exn t.k) ;
  List.iter keys ~f:(fun k ->
      let h = bloom_hash k in
      let delta =
        Uint32.(logor (shift_right_logical h 17) (shift_left h 15))
      in
      let rec aux n h =
        if n < 1 then ()
        else
          let bitpos = Uint32.to_int h % bits in
          let pos = init_size + bitpos / 8 in
          let v = Slice.get_exn dst pos |> Char.to_int |> Uint8.of_int in
          Slice.set dst pos
            ( Uint8.(logor v (shift_left one (bitpos % 8)) |> to_int)
            |> Char.of_int_exn ) ;
          aux (n - 1) Uint32.(h + delta)
      in
      aux t.k h )


let key_may_match t key bloom_filter =
  let len = Slice.length bloom_filter in
  if len < 2 then false
  else
    let bits = (len - 1) * 8 in
    let k = Slice.get_exn bloom_filter (len - 1) |> Char.to_int in
    if k > 30 then true
    else
      let h = bloom_hash key in
      let delta =
        Uint32.(logor (shift_right_logical h 17) (shift_left h 15))
      in
      let rec aux n h =
        if n < 1 then true
        else
          let bitpos = Uint32.to_int h % bits in
          let v =
            Slice.get_exn bloom_filter (bitpos / 8) |> Char.to_int
            |> Uint8.of_int
          in
          let v' = Uint8.(shift_left one (bitpos % 8)) in
          if Uint8.(logand v' v = zero) then false
          else aux (n - 1) Uint32.(h + delta)
      in
      aux k h
