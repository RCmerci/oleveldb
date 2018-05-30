open Core
open Stdint
open Oleveldb_lib_util
open Coding

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

module type S = sig
  type t

  val policy_name : string

  val create : unit -> t

  val add_key : t -> Oleveldb_lib_util.Slice.t -> unit

  val start_block : t -> int -> unit

  val finish : t -> Oleveldb_lib_util.Slice.t
end

module Make (Policy : Filter_policy.S) : S = struct
  (*
     keys: key list
     result: filter data computed yet
  *)
  type t =
    { mutable keys: Slice.t list
    ; result: Slice.t
    ; mutable filter_offset: int list
    ; policy: Policy.t }

  let policy_name = Policy.name

  let filter_base_lg = 11

  (* 2kb *)
  let filter_base = 1 lsl filter_base_lg

  let create () =
    { keys= []
    ; result= Slice.create 1
    ; filter_offset= []
    ; policy= Policy.create 10 }


  let add_key t key = t.keys <- key :: t.keys

  let generate_filter t =
    let num_keys = List.length t.keys in
    t.filter_offset <- Slice.length t.result :: t.filter_offset ;
    if num_keys = 0 then ()
    else Policy.create_filter t.policy (List.rev t.keys) t.result ;
    t.keys <- []


  let start_block t block_offset =
    let filter_index = block_offset / filter_base in
    let rec aux () =
      if filter_index > List.length t.filter_offset then (
        generate_filter t ; aux () )
    in
    aux ()


  let finish t =
    if not (List.is_empty t.keys) then generate_filter t else () ;
    List.iter (List.rev t.filter_offset) ~f:(fun e ->
        append_fix32 t.result (Uint32.of_int e) ) ;
    append_fix32 t.result (Uint32.of_int (Slice.length t.result)) ;
    Slice.add_char t.result (Char.of_int_exn filter_base_lg) ;
    t.result

end

module BloomFilterBlock = Make (Bloomfilter)
