open Core
open Stdint
open Oleveldb_lib_util
open Coding

type config_option = {block_restart_interval: int}

type t =
  { buffer: Slice.t
  ; mutable restarts: int list
  ; option: config_option
  ; mutable counter: int
  ; mutable finished: bool
  ; mutable last_key: Slice.t }

let create option =
  { buffer= Slice.create 100
  ; restarts= [0]
  ; option
  ; counter= 0
  ; finished= false
  ; last_key= Slice.create 0 }


(** return shared length *)
let compute_shared_key last_k k limit =
  let rec aux n =
    let open Option in
    Slice.get last_k n
    >>= fun c ->
    Slice.get k n
    >>= fun c' -> if c' = c && n + 1 < limit then aux (n + 1) else return n
  in
  Option.value_exn (aux 0)


let add t k v =
  assert (not t.finished) ;
  assert (t.counter <= t.option.block_restart_interval) ;
  let shared =
    if t.counter = t.option.block_restart_interval then (
      t.counter <- 0 ;
      t.restarts <- Slice.length t.buffer :: t.restarts ;
      0 )
    else
      compute_shared_key t.last_key k
        (Int.min (Slice.length t.last_key) (Slice.length k))
  in
  let non_shared = Slice.length k - shared in
  append_varint32 t.buffer (Uint32.of_int shared) ;
  append_varint32 t.buffer (Uint32.of_int non_shared) ;
  append_varint32 t.buffer (Uint32.of_int (Slice.length v)) ;
  Slice.add_string t.buffer (Slice.to_substring k ~pos:shared ~len:non_shared) ;
  Slice.add_slice t.buffer v ;
  t.last_key <- k ;
  t.counter <- t.counter + 1


let finish t =
  List.iter t.restarts ~f:(fun i -> append_fix32 t.buffer (Uint32.of_int i)) ;
  append_fix32 t.buffer (Uint32.of_int (List.length t.restarts)) ;
  t.finished <- true ;
  t.buffer
