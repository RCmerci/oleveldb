open Core
open Stdint

let lsl32 = Uint32.shift_left

let lsr32 = Uint32.shift_right_logical

let land32 = Uint32.logand

let lor32 = Uint32.logor

let lsl64 = Uint64.shift_left

let lsr64 = Uint64.shift_right_logical

let land64 = Uint64.logand

let lor64 = Uint64.logor

let encode_varint32 dst (v: uint32) =
  let open Uint32 in
  if v < lsl32 one 7 then (
    Slice.set dst 0 (Char.of_int_exn (to_int v)) ;
    1 )
  else if v < lsl32 one 14 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor32 (land32 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn (to_int (land32 (lsr32 v 7) (of_int 0x7f)))) ;
    2 )
  else if v < lsl32 one 21 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor32 (land32 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor32 (land32 (lsr32 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn (to_int (land32 (lsr32 v 14) (of_int 0x7f)))) ;
    3 )
  else if v < lsl32 one 28 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor32 (land32 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor32 (land32 (lsr32 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn
         (to_int (lor32 (land32 (lsr32 v 14) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 3
      (Char.of_int_exn (to_int (land32 (lsr32 v 21) (of_int 0x7f)))) ;
    4 )
  else (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor32 (land32 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor32 (land32 (lsr32 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn
         (to_int (lor32 (land32 (lsr32 v 14) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 3
      (Char.of_int_exn
         (to_int (lor32 (land32 (lsr32 v 21) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 4
      (Char.of_int_exn (to_int (land32 (lsr32 v 28) (of_int 0x7f)))) ;
    5 )


(** this function use Slice.strip_head, so it has
    side effects on [src] *)
let decode_varint32 src : uint32 option =
  let open Uint32 in
  let rec aux r shift =
    let h = Slice.strip_head src 1 in
    if String.length h = 0 then None
    else
      let h' = Char.of_string h |> Char.to_int |> of_int in
      if land32 h' (of_int 128) <> zero then
        let r' = lor32 (lsl32 (land32 h' (of_int 127)) shift) r in
        aux r' Pervasives.(shift + 7)
      else Some (lor32 (lsl32 h' shift) r)
  in
  aux (of_int 0) 0


let decode_varint64 src : uint64 option =
  let open Uint64 in
  let rec aux r shift =
    let h = Slice.strip_head src 1 in
    if String.length h = 0 then None
    else
      let h' = Char.of_string h |> Char.to_int |> of_int in
      if land64 h' (of_int 128) <> zero then
        let r' = lor64 (lsl64 (land64 h' (of_int 127)) shift) r in
        aux r' Pervasives.(shift + 7)
      else Some (lor64 (lsl64 h' shift) r)
  in
  aux (of_int 0) 0


let encode_varint64 dst (v: uint64) =
  let open Uint64 in
  if v < lsl64 one 7 then (
    Slice.set dst 0 (Char.of_int_exn (to_int v)) ;
    1 )
  else if v < lsl64 one 14 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor64 (land64 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn (to_int (land64 (lsr64 v 7) (of_int 0x7f)))) ;
    2 )
  else if v < lsl64 one 21 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor64 (land64 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn (to_int (land64 (lsr64 v 14) (of_int 0x7f)))) ;
    3 )
  else if v < lsl64 one 28 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor64 (land64 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 14) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 3
      (Char.of_int_exn (to_int (land64 (lsr64 v 21) (of_int 0x7f)))) ;
    4 )
  else if v < lsl64 one 35 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor64 (land64 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 14) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 3
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 21) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 4
      (Char.of_int_exn (to_int (land64 (lsr64 v 28) (of_int 0x7f)))) ;
    5 )
  else if v < lsl64 one 42 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor64 (land64 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 14) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 3
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 21) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 4
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 28) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 5
      (Char.of_int_exn (to_int (land64 (lsr64 v 35) (of_int 0x7f)))) ;
    6 )
  else if v < lsl64 one 49 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor64 (land64 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 14) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 3
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 21) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 4
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 28) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 5
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 35) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 6
      (Char.of_int_exn (to_int (land64 (lsr64 v 42) (of_int 0x7f)))) ;
    7 )
  else if v < lsl64 one 56 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor64 (land64 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 14) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 3
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 21) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 4
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 28) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 5
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 35) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 6
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 42) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 7
      (Char.of_int_exn (to_int (land64 (lsr64 v 49) (of_int 0x7f)))) ;
    8 )
  else if v < lsl64 one 63 then (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor64 (land64 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 14) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 3
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 21) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 4
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 28) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 5
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 35) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 6
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 42) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 7
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 49) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 8
      (Char.of_int_exn (to_int (land64 (lsr64 v 56) (of_int 0x7f)))) ;
    9 )
  else (
    Slice.set dst 0
      (Char.of_int_exn (to_int (lor64 (land64 v (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 1
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 7) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 2
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 14) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 3
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 21) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 4
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 28) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 5
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 35) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 6
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 42) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 7
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 49) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 8
      (Char.of_int_exn
         (to_int (lor64 (land64 (lsr64 v 56) (of_int 0x7f)) (of_int 128)))) ;
    Slice.set dst 9
      (Char.of_int_exn (to_int (land64 (lsr64 v 63) (of_int 0x7f)))) ;
    10 )


let append_varint32 dst v =
  let buf = Slice.from_string (String.make 5 '\000') in
  let len = encode_varint32 buf v in
  Slice.to_substring buf 0 len |> Slice.add_string dst


let append_varint64 dst v =
  let buf = Slice.from_string (String.make 10 '\000') in
  let len = encode_varint64 buf v in
  Slice.to_substring buf 0 len |> Slice.add_string dst


let encode_fix32 dst v =
  let open Uint32 in
  Slice.set dst 0 (Char.of_int_exn (to_int (land32 v (of_int 0xff)))) ;
  Slice.set dst 1 (Char.of_int_exn (to_int (land32 (lsr32 v 8) (of_int 0xff)))) ;
  Slice.set dst 2
    (Char.of_int_exn (to_int (land32 (lsr32 v 16) (of_int 0xff)))) ;
  Slice.set dst 3
    (Char.of_int_exn (to_int (land32 (lsr32 v 24) (of_int 0xff))))


let append_fix32 dst v =
  let buf = Slice.from_string (String.make 4 '\000') in
  encode_fix32 buf v ; Slice.add_slice dst buf


let decode_fix32 src : uint32 option =
  let open Option in
  let open Uint32 in
  all [Slice.get src 0; Slice.get src 1; Slice.get src 2; Slice.get src 3]
  >>= fun l ->
  return (List.map l ~f:(fun e -> e |> Char.to_int |> of_int))
  >>= fun l ->
  match l with
  | [a; b; c; d] -> return (lsl32 d 24 + lsl32 c 16 + lsl32 b 8 + a)
  | _ -> None


let encode_fix64 dst v =
  let open Uint64 in
  Slice.set dst 0 (Char.of_int_exn (to_int (land64 v (of_int 0xff)))) ;
  Slice.set dst 1 (Char.of_int_exn (to_int (land64 (lsr64 v 8) (of_int 0xff)))) ;
  Slice.set dst 2
    (Char.of_int_exn (to_int (land64 (lsr64 v 16) (of_int 0xff)))) ;
  Slice.set dst 3
    (Char.of_int_exn (to_int (land64 (lsr64 v 24) (of_int 0xff)))) ;
  Slice.set dst 4
    (Char.of_int_exn (to_int (land64 (lsr64 v 32) (of_int 0xff)))) ;
  Slice.set dst 5
    (Char.of_int_exn (to_int (land64 (lsr64 v 40) (of_int 0xff)))) ;
  Slice.set dst 6
    (Char.of_int_exn (to_int (land64 (lsr64 v 48) (of_int 0xff)))) ;
  Slice.set dst 7
    (Char.of_int_exn (to_int (land64 (lsr64 v 56) (of_int 0xff))))


let append_fix64 dst v =
  let buf = Slice.from_string (String.make 8 '\000') in
  encode_fix64 buf v ; Slice.add_slice dst buf


let decode_fix64 src : uint64 option =
  let open Option in
  let open Uint64 in
  all
    [ Slice.get src 0
    ; Slice.get src 1
    ; Slice.get src 2
    ; Slice.get src 3
    ; Slice.get src 4
    ; Slice.get src 5
    ; Slice.get src 6
    ; Slice.get src 7 ]
  >>= fun l ->
  return (List.map l ~f:(fun a -> Char.to_int a |> of_int))
  >>= fun l ->
  match l with
  | [a1; a2; a3; a4; a5; a6; a7; a8] ->
      return
        ( lsr64 a8 56 + lsr64 a7 48 + lsr64 a6 40 + lsr64 a5 32 + lsr64 a4 24
        + lsr64 a3 16 + lsr64 a2 8 + a1 )
  | _ -> None
