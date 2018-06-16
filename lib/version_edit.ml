open Core
open Stdint
open Oleveldb_lib_util
open Dbfmt

type tag =
  | Comparator
  | Log_num
  | Next_file_num
  | Last_sequence
  | Compact_pointer
  | Deleted_file
  | New_file
  | Prev_log_num

let tag_to_int = function
  | Comparator -> 1
  | Log_num -> 2
  | Next_file_num -> 3
  | Last_sequence -> 4
  | Compact_pointer -> 5
  | Deleted_file -> 6
  | New_file -> 7
  | Prev_log_num -> 9


let tag_of_int = function
  | 1 -> Comparator
  | 2 -> Log_num
  | 3 -> Next_file_num
  | 4 -> Last_sequence
  | 5 -> Compact_pointer
  | 6 -> Deleted_file
  | 7 -> New_file
  | 9 -> Prev_log_num
  | _ -> assert false


type file_metadata =
  { allowed_seeks: int
  ; number: uint64
  ; size: uint64
  ; smallest: Internal_key.t
  ; largest: Internal_key.t }

type t =
  { comparator: Slice.t option
  ; log_num: uint64 option
  ; prev_log_num: uint64 option
  ; next_file_num: uint64 option
  ; last_sequence: uint64 option
  ; deleted_files: (int * uint64) list
  ; new_files: (int * file_metadata) list
  ; compact_pointers: (int * Internal_key.t) list }

let encode t =
  let open Option in
  let open Coding in
  let u32_of_int = Uint32.of_int in
  let tag_to_u32 tag = tag_to_int tag |> u32_of_int in
  let dst = Slice.create 100 in
  iter t.comparator (fun cmp ->
      append_varint32 dst (tag_to_u32 Comparator) ;
      append_varint32 dst (Slice.length cmp |> u32_of_int) ;
      Slice.add_slice dst cmp ) ;
  iter t.log_num (fun log_num ->
      append_varint32 dst (tag_to_u32 Log_num) ;
      append_varint64 dst log_num ) ;
  iter t.prev_log_num (fun prev_log_num ->
      append_varint32 dst (tag_to_u32 Prev_log_num) ;
      append_varint64 dst prev_log_num ) ;
  iter t.next_file_num (fun next_file_num ->
      append_varint32 dst (tag_to_u32 Next_file_num) ;
      append_varint64 dst next_file_num ) ;
  iter t.last_sequence (fun last_sequence ->
      append_varint32 dst (tag_to_u32 Last_sequence) ;
      append_varint64 dst last_sequence ) ;
  List.iter (List.rev t.compact_pointers) (fun (level, ikey) ->
      let ikey' = Internal_key.to_slice ikey in
      append_varint32 dst (tag_to_u32 Compact_pointer) ;
      append_varint32 dst (u32_of_int level) ;
      append_varint32 dst (Slice.length ikey' |> u32_of_int) ;
      Slice.add_slice dst ikey' ) ;
  List.iter (List.rev t.deleted_files) (fun (level, file_num) ->
      append_varint32 dst (tag_to_u32 Deleted_file) ;
      append_varint32 dst (u32_of_int level) ;
      append_varint64 dst file_num ) ;
  List.iter (List.rev t.new_files) (fun (level, file_meta) ->
      append_varint32 dst (tag_to_u32 New_file) ;
      append_varint32 dst (u32_of_int level) ;
      append_varint64 dst file_meta.number ;
      append_varint64 dst file_meta.size ;
      let smallest = Internal_key.to_slice file_meta.smallest in
      let largest = Internal_key.to_slice file_meta.largest in
      append_varint32 dst (Slice.length smallest |> u32_of_int) ;
      Slice.add_slice dst smallest ;
      append_varint32 dst (Slice.length largest |> u32_of_int) ;
      Slice.add_slice dst largest ) ;
  dst


let decode src =
  let open Option in
  let open Coding in
  let u32_to_int = Uint32.to_int in
  let s = Slice.copy src in
  let rec aux r =
    match decode_varint32 s with
    | None -> Some r
    | Some tag ->
      match tag_of_int (u32_to_int tag) with
      | Comparator ->
          decode_varint32 s
          >>= fun len ->
          let cmp = Slice.strip_head s (u32_to_int len) |> Slice.from_string in
          aux {r with comparator= Some cmp}
      | Log_num ->
          decode_varint64 s
          >>= fun log_num -> aux {r with log_num= Some log_num}
      | Next_file_num ->
          decode_varint64 s
          >>= fun next_file_num ->
          aux {r with next_file_num= Some next_file_num}
      | Last_sequence ->
          decode_varint64 s
          >>= fun last_sequence ->
          aux {r with last_sequence= Some last_sequence}
      | Compact_pointer ->
          decode_varint32 s
          >>= fun level ->
          decode_varint32 s
          >>= fun len ->
          let ikey =
            Slice.strip_head s (u32_to_int len) |> Slice.from_string
          in
          let level' = u32_to_int level in
          aux
            { r with
              compact_pointers=
                (level', Internal_key.decode_from ikey) :: r.compact_pointers }
      | Deleted_file ->
          decode_varint32 s
          >>= fun level ->
          decode_varint64 s
          >>= fun file_num ->
          aux
            { r with
              deleted_files= (u32_to_int level, file_num) :: r.deleted_files }
      | New_file ->
          decode_varint32 s
          >>= fun level ->
          decode_varint64 s
          >>= fun number ->
          decode_varint64 s
          >>= fun size ->
          decode_varint32 s
          >>= fun smallest_len ->
          let smallest =
            Slice.strip_head s (u32_to_int smallest_len) |> Slice.from_string
            |> Internal_key.decode_from
          in
          decode_varint32 s
          >>= fun largest_len ->
          let largest =
            Slice.strip_head s (u32_to_int largest_len) |> Slice.from_string
            |> Internal_key.decode_from
          in
          aux
            { r with
              new_files=
                ( u32_to_int level
                , {allowed_seeks= 0; number; size; smallest; largest} )
                :: r.new_files }
      | Prev_log_num ->
          decode_varint64 s
          >>= fun prev_log_num -> aux {r with prev_log_num= Some prev_log_num}
  in
  aux
    { comparator= None
    ; log_num= None
    ; prev_log_num= None
    ; next_file_num= None
    ; last_sequence= None
    ; deleted_files= []
    ; new_files= []
    ; compact_pointers= [] }
