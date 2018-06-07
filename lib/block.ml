open Core
open Stdint
open Oleveldb_lib_util
open Coding

(* restart_offset: datablock restart part start offset *)
type t = {data: Slice.t; size: int; restart_offset: int}

let num_restarts t =
  let open Option in
  let s = Slice.to_substring t.data ~pos:(t.size - 4) ~len:4 in
  Option.value_exn (decode_fix32 (Slice.from_string s)) |> Uint32.to_int


let create data =
  let size = Slice.length data in
  if size < 4 then None
  else
    let t = {data; size; restart_offset= 0} in
    let n_restarts = num_restarts t in
    if n_restarts > (t.size - 4) / 4 then None
    else Some {t with restart_offset= t.size - (1 + n_restarts) * 4}


(*
+--------------------+----------------------+--------------------+----------------+-------+
| shared (varint32)  |non_shared (varint32) |value_len (varint32)| non_shared_key | value |
+--------------------+----------------------+--------------------+----------------+-------+
decode datablock entry
@return (shared_length, non_shared_key, value, entry_size)
  *)
let decode_entry data =
  let data' = Slice.copy data in
  let open Option in
  let open Uint32 in
  decode_varint32 data'
  >>= fun shared ->
  decode_varint32 data'
  >>= fun non_shared ->
  decode_varint32 data'
  >>= fun value_len ->
  let shared' = to_int shared in
  let non_shared' = to_int non_shared in
  let value_len' = to_int value_len in
  let non_shared_key = Slice.strip_head data' non_shared' in
  let value = Slice.strip_head data' value_len' in
  let entry_size = Pervasives.(Slice.length data - Slice.length data') in
  return (shared', non_shared_key, value, entry_size)


type iter_t =
  { data: Slice.t (* imut *)
  ; n_restarts: int (* imut *)
  ; restart_offset: int (* imut *)
  ; current_data_offset: int
  ; current_data_entry_size: int
  ; current_restart_index: int
  ; k: string
  ; v: string }

module Iter_make_raw (CMP : Cmp.S) = struct
  type param = t

  type t = iter_t

  type 'a monad = t -> ('a * t, string) result

  let create (param: param) : t =
    { data= param.data
    ; n_restarts= num_restarts param
    ; restart_offset= param.restart_offset
    ; current_data_offset= 0
    ; current_data_entry_size= 0
    ; current_restart_index= 0
    ; k= ""
    ; v= "" }


  let update_t t ~current_data_offset ~current_data_entry_size
      ~current_restart_index ~k ~v =
    { t with
      current_restart_index; current_data_entry_size; current_data_offset; k; v
    }


  let get_restart_offset t index = t.restart_offset + index * 4

  let get_next_entry_offset t =
    t.current_data_offset + t.current_data_entry_size


  let get_restart_point t index =
    let open Option in
    let data' = Slice.copy t.data in
    Slice.strip_head_2 data' (get_restart_offset t index) ;
    decode_fix32 data' >>= fun r -> return (Uint32.to_int r)


  let get_next_restart_index t =
    let open Option in
    let rec aux index =
      get_restart_point t (index + 1)
      >>= fun restart_point ->
      if index + 1 < t.n_restarts && restart_point < t.current_data_offset then
        aux (index + 1)
      else return index
    in
    aux t.current_restart_index


  (* when no more entry, return [Ok None]*)
  let parse_next t =
    let open Result in
    let data' = Slice.copy t.data in
    let next_entry_offset = get_next_entry_offset t in
    if next_entry_offset >= t.restart_offset then
      (* no more entry *)
      return None
    else (
      Slice.strip_head_2 data' next_entry_offset ;
      of_option (decode_entry data') ~error:"decode_entry"
      >>= fun (shared_length, non_shared_key, value, entry_size) ->
      let shared = String.sub t.k 0 shared_length in
      let key = shared ^ non_shared_key in
      of_option (get_next_restart_index t) ~error:"get_next_restart_index"
      >>= fun next_restart_index ->
      return
        (Some
           (update_t t ~current_data_offset:next_entry_offset
              ~current_data_entry_size:entry_size
              ~current_restart_index:next_restart_index ~k:key ~v:value)) )


  let seek_restart_point t index =
    let open Option in
    let current_restart_index = index in
    get_restart_point t index
    >>= fun current_data_offset ->
    let t' =
      update_t t ~current_data_offset ~current_restart_index
        ~current_data_entry_size:0 ~k:"" ~v:""
    in
    return t'


  let seek_to_first t =
    let open Result in
    Option.bind (seek_restart_point t 0) ~f:(fun t' ->
        Option.return
          ( parse_next t'
          >>= fun t_op ->
          assert (Option.is_some t_op) ;
          let t'' = Option.value_exn t_op in
          return
            (update_t t'' ~current_data_offset:t''.current_data_offset
               ~current_data_entry_size:0
               ~current_restart_index:t.current_restart_index ~k:"" ~v:"") ) )
    |> of_option ~error:"seek_restart_point" |> join >>= fun t -> return ((), t)


  let seek_to_last t =
    let open Result in
    let rec to_last t =
      parse_next t
      >>= fun t_op ->
      match t_op with
      | Some t' ->
          if get_next_entry_offset t' < t'.restart_offset then to_last t'
          else return t'
      | None -> return t
    in
    Option.bind (seek_restart_point t (t.n_restarts - 1)) ~f:(fun t' ->
        Option.return (to_last t') )
    |> of_option ~error:"seek_restart_point" |> join >>= fun t -> return ((), t)


  let search_restart_index t key =
    let open Option in
    let rec aux left right =
      if left >= right then return left
      else
        let mid = (left + right + 1) / 2 in
        get_restart_point t mid
        >>= fun restart_point ->
        let data' = Slice.copy t.data in
        Slice.strip_head_2 data' restart_point ;
        decode_entry data'
        >>= fun (shared_length, non_shared_key, value, entry_size) ->
        (* it's restart start point,so non_shared_key is complete  *)
        let mid_key = non_shared_key in
        match CMP.compare (Slice.from_string mid_key) key with
        | Cmp.LT -> aux mid right
        | Cmp.EQ | Cmp.GT -> aux left (mid - 1)
    in
    aux 0 (t.n_restarts - 1)


  (* maybe [key] is greater than any key in block,
     then [seek] will behave like [seek_to_last]
 *)
  let seek key t =
    let open Option in
    let rec seek_to_key t =
      let open Result in
      parse_next t
      >>= fun t_op ->
      match t_op with
      | Some t' -> (
        match CMP.compare (Slice.from_string t'.k) key with
        | Cmp.GT | Cmp.EQ -> return t'
        | Cmp.LT -> seek_to_key t' )
      | None -> return t
    in
    search_restart_index t key
    >>= (fun restart_index ->
          seek_restart_point t restart_index
          >>= fun t' -> return (seek_to_key t'))
    |> Result.of_option ~error:"search_restart_index or seek_restart_point"
    |> Result.join |> Result.bind ~f:(fun t -> Result.return ((), t))


  let next t =
    let open Result in
    parse_next t
    >>= fun t_op ->
    match t_op with Some t' -> return (true, t') | None -> return (false, t)


  let prev t =
    let open Result in
    let rec back_restart_index_ restart_index =
      let open Option in
      if restart_index = 0 then return None
      else get_restart_point t restart_index
        >>= fun restart_point ->
        if restart_point >= t.current_data_offset then
          back_restart_index_ (restart_index - 1)
        else return (return restart_index)
    in
    let back_restart_index restart_index =
      back_restart_index_ restart_index
      |> Result.of_option ~error:"get_restart_point"
    in
    let rec seek_to_next_key orig t =
      parse_next t
      >>= fun t_op ->
      assert (Option.is_some t_op) ;
      (* this entry cannot be last one *)
      let t' = Option.value_exn t_op in
      if get_next_entry_offset t' < orig.current_data_offset then
        seek_to_next_key orig t'
      else return t'
    in
    back_restart_index t.current_restart_index
    >>= fun prev_restart_index_op ->
    match prev_restart_index_op with
    | Some prev_restart_index ->
        Result.of_option
          (seek_restart_point t prev_restart_index)
          ~error:"seek_restart_point"
        >>= seek_to_next_key t >>= fun t -> return (true, t)
    | None -> return (false, t)


  let key t = Result.return (Slice.from_string t.k, t)

  let value t = Result.return (Slice.from_string t.v, t)

  let fold ~f ~init t =
    let open Result in
    let rec aux r t =
      let r' = f r (Slice.from_string t.k, Slice.from_string t.v) in
      next t >>= fun (exist, t') -> if exist then aux r' t' else return (r', t)
    in
    seek_to_first t >>= fun ((), t') -> aux init t'


  let return a t = Result.return (a, t)

  let bind (m: 'a monad) ~(f: 'a -> 'b monad) : 'b monad =
    fun t ->
      let r = m t in
      Result.bind r (fun (a, t') -> f a t')


  let ( >>= ) m f = bind m ~f

  let ( >> ) m f = m >>= fun a -> return (f a)

  let run m t = m t
end

module type Iter_make_S = functor (CMP : Cmp.S) -> Iterator.S
                                                   with type param = t

module type Iter_make_debug_S = functor (CMP : Cmp.S) -> Iterator.S
                                                         with type param = t
                                                          and type t = iter_t

module Iter_make_debug = Iter_make_raw

module Iter_make : Iter_make_S = Iter_make_raw

module Iter = Iter_make (Cmp.ByteWiseComparator)
module Iter_debug = Iter_make_debug (Cmp.ByteWiseComparator)

let create_iter = Iter.create

let create_iter_debug = Iter_debug.create
