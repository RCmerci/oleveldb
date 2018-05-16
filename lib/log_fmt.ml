open Core

(*
+-----------------+-------------+-----------------------+
| length (16bit)  | type (8bit) | data (length bytes)   |
+-----------------+-------------+-----------------------+
 *)

type record_type = ZeroType | FullType | FirstType | MiddleType | LastType

let record_to_int r =
  match r with
  | ZeroType -> 0
  | FullType -> 1
  | FirstType -> 2
  | MiddleType -> 3
  | LastType -> 4


let record_from_int i =
  match i with
  | 0 -> ZeroType
  | 1 -> FullType
  | 2 -> FirstType
  | 3 -> MiddleType
  | 4 -> LastType
  | _ -> assert false


let kBlockSize = 32768

let kHeaderSize = 2 + 1

let parse_header s : int * record_type =
  let len1 = s.[0] |> Char.to_int in
  let len2 = s.[1] |> Char.to_int in
  let len = len2 lsl 8 + len1 in
  let tp = s.[2] |> Char.to_int |> record_from_int in
  (len, tp)
