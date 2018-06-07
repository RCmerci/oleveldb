open Core
open Stdint
open Oleveldb_lib_util
open Coding

type config_option = {block_restart_interval: int}

let default_config_option = {block_restart_interval= 16}

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
    if n >= limit then return n
    else Slice.get last_k n
      >>= fun c ->
      Slice.get k n >>= fun c' -> if c' = c then aux (n + 1) else return n
  in
  Option.value (aux 0) ~default:0


let reset t = create t.option

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
  List.iter (List.rev t.restarts) ~f:(fun i ->
      append_fix32 t.buffer (Uint32.of_int i) ) ;
  append_fix32 t.buffer (Uint32.of_int (List.length t.restarts)) ;
  t.finished <- true ;
  t.buffer


let current_size_estimate t =
  Slice.length t.buffer + List.length t.restarts * 4 + 4


let is_empty t = Slice.length t.buffer = 0
