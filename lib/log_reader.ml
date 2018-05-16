open Core
open Oleveldb_lib_util
open Log_fmt

type t = {src: In_channel.t; mutable cache: (record_type * Slice.t) list}

let create src = In_channel.seek src 0L ; {src; cache= []}

let read_physical_record t : (record_type * Slice.t) list * Status.t =
  let buf = Slice.create kBlockSize in
  let _ = Slice.input buf t.src kBlockSize in
  let rec aux r =
    let header = Slice.strip_head buf kHeaderSize in
    if String.length header < kHeaderSize then r
    else
      let length, tp = parse_header header in
      let data = Slice.strip_head buf length in
      aux ((tp, Slice.from_string data) :: r)
  in
  let r = aux [] |> List.rev in
  (r, Status.OK)


let get_part_record_from_cache t is_begin =
  let is_begin = ref is_begin in
  let is_end = ref false in
  let phy_records =
    List.take_while t.cache ~f:(fun (tp, data) ->
        if !is_end then false
        else
          match tp with
          | FirstType when !is_begin = true -> true
          | LastType ->
              is_end := true ;
              true
          | ZeroType ->
              is_end := true ;
              true
          | FullType ->
              is_end := true ;
              true
          | MiddleType -> true
          | _ -> false )
  in
  t.cache <- List.drop t.cache (List.length phy_records) ;
  (phy_records, !is_end)


let read_record t =
  let combine_record_data l =
    List.fold_left l ~init:(Slice.create 30) ~f:(fun r (tp, slice) ->
        Slice.add_slice r slice ; r )
  in
  let rec aux r is_begin =
    let phy_records, is_full = get_part_record_from_cache t is_begin in
    if is_full then
      (combine_record_data (List.concat [r; phy_records]), Status.OK)
    else
      let phy_records2, stat = read_physical_record t in
      if Status.isOK stat then (
        t.cache <- List.concat [t.cache; phy_records2] ;
        let r = List.concat [r; phy_records] in
        aux r (List.is_empty r) )
      else (Slice.create 0, stat)
  in
  aux [] true
