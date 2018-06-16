open Core
open Stdint
open Oleveldb_lib_util

type value_type = TYPE_DELETION | TYPE_SEEK

let value_type_to_int = function TYPE_DELETION -> 0 | TYPE_SEEK -> 1

let value_type_of_int = function
  | 0 -> TYPE_DELETION
  | 1 -> TYPE_SEEK
  | _ -> assert false


let max_sequence = Uint64.(shift_right_logical max_int 8)

let pack_sequence_and_type seq tp =
  let open Uint64 in
  let tp' = value_type_to_int tp |> of_int in
  logor (shift_left seq 8) tp'


module Internal_key = struct
  type parsed_t =
    {user_key: Slice.t; sequence: Uint64.t; value_type: value_type}

  type t = {mutable rep: Slice.t option; mutable parsed_t: parsed_t option}

  let of_parsed_t parsed_t =
    let r = Slice.create 20 in
    Slice.add_slice r parsed_t.user_key ;
    pack_sequence_and_type parsed_t.sequence parsed_t.value_type
    |> Coding.append_fix64 r ;
    r


  let to_parsed_t rep =
    let rep' = Slice.copy rep in
    let user_key =
      Slice.strip_head rep' (Slice.length rep' - 8) |> Slice.from_string
    in
    let seq_and_type = Coding.decode_fix64 rep' in
    assert (Option.is_some seq_and_type) ;
    let seq_and_type' = Option.value_exn seq_and_type in
    let value_type =
      Uint64.(logand (of_int 0xff) seq_and_type' |> to_int)
      |> value_type_of_int
    in
    let sequence = Uint64.(shift_right_logical seq_and_type' 8) in
    {user_key; sequence; value_type}


  let decode_from s = {rep= Some s; parsed_t= None}

  let decode_from_2 user_key sequence value_type =
    {rep= None; parsed_t= Some {user_key; sequence; value_type}}


  let to_slice t =
    match (t.rep, t.parsed_t) with
    | Some v, _ -> v
    | None, Some p_t ->
        let s = of_parsed_t p_t in
        t.rep <- Some s ;
        s
    | _ -> assert false


  let get_user_key t =
    match (t.rep, t.parsed_t) with
    | _, Some p_t -> p_t.user_key
    | Some s, None ->
        let parsed_t = to_parsed_t s in
        t.parsed_t <- Some parsed_t ;
        parsed_t.user_key
    | _ -> assert false


  let get_value_type t =
    match (t.rep, t.parsed_t) with
    | _, Some p_t -> p_t.value_type
    | Some s, None ->
        let parsed_t = to_parsed_t s in
        t.parsed_t <- Some parsed_t ;
        parsed_t.value_type
    | _ -> assert false


  let get_sequence t =
    match (t.rep, t.parsed_t) with
    | _, Some p_t -> p_t.sequence
    | Some s, None ->
        let parsed_t = to_parsed_t s in
        t.parsed_t <- Some parsed_t ;
        parsed_t.sequence
    | _ -> assert false

end
