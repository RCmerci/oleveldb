open Core
open Table_fmt
open Oleveldb_lib_util
module FilterBlockReader = Filter_block_reader.BloomFilterBlockReader

module V : Lru.Weighted with type t = Block.t = struct
  type t = Block.t

  let weight _ = 1
end

(** block lru cache
    index_handle -> Block *)
module Cache = Lru.M.Make (Slice) (V)

(** TODO: better default value? *)
let default_cap = 20

type t =
  { file: In_channel.t
  ; index_block: Block.t
  ; metaindex_handle: block_handle
  ; filter_block: FilterBlockReader.t
  ; block_cache: Cache.t }

let read_filter handle file =
  let filter_block_data = read_block handle file in
  FilterBlockReader.create filter_block_data


let read_index_block handle file =
  let index_block_data = read_block handle file in
  Result.of_option ~error:"read_index_block" (Block.create index_block_data)


(* return filter_block and index_block   *)
let read_meta file footer =
  let open Result in
  let metaindex_block_data =
    read_block (footer_get_metaindex_handle footer) file
  in
  of_option
    (Block.create metaindex_block_data)
    ~error:"Block.create metaindex_block"
  >>= fun metaindex_block ->
  let metaindex_iter = Block.create_iter metaindex_block in
  let policy_name =
    "filter." ^ Filter_block.BloomFilterBlock.policy_name |> Slice.from_string
  in
  let m =
    let open Block.Iter in
    seek policy_name
    >>= fun b ->
    match b with
    | false -> return (Error "seek policy_name failed")
    | true ->
        value
        >>= fun v ->
        return
          (Result.bind
             (of_option ~error:"read_meta" (block_handle_decode_from v))
             (fun handle -> read_filter handle file ))
  in
  let filter_block =
    match Block.Iter.run m metaindex_iter with
    | Ok (r, _) -> r
    | Error e -> Error e
  in
  let index_block_handle = footer_get_index_handle footer in
  let index_block = read_index_block index_block_handle file in
  filter_block
  >>= fun filter_block ->
  index_block >>= fun index_block -> return (filter_block, index_block)


let create file =
  let size = In_channel.length file in
  let footer_encoded_length_int = Int64.to_int_exn footer_encoded_length in
  if size < footer_encoded_length then
    Error "file is too short to be an sstable"
  else (
    In_channel.seek file Int64.O.(size - footer_encoded_length) ;
    let buf = Slice.create footer_encoded_length_int in
    let readn = Slice.input buf file footer_encoded_length_int in
    if readn < footer_encoded_length_int then Error "incomplete sstable footer"
    else
      match footer_decode_from buf with
      | None -> Error "decode sstable footer failed"
      | Some footer ->
          let metaindex_handle = footer_get_metaindex_handle footer in
          let open Result in
          read_meta file footer
          >>= fun (filter_block, index_block) ->
          return
            { file
            ; index_block
            ; metaindex_handle
            ; filter_block
            ; block_cache= Cache.create default_cap } )


(** used as two-level-iterator's block-function
    index_value: encoded block_handle *)
let block_reader t index_value =
  let open Option in
  ( match Cache.find index_value t.block_cache with
  | Some block -> return block
  | None ->
      block_handle_decode_from index_value
      >>= fun handle ->
      let block_data = read_block handle t.file in
      Block.create block_data
      >>= fun block ->
      Cache.add index_value block t.block_cache ;
      return block )
  >>= fun block ->
  let iter = Block.create_iter block in
  return iter


module A = struct
  type t' = t

  type t = t'
end

module Two_level_iter = Two_level_iterator.Make (A) (Block.Iter) (Block.Iter)

let create_iter t =
  let index_iter = Block.create_iter t.index_block in
  Two_level_iter.create {block_func= block_reader; arg= t; index_iter}


let get t k =
  let index_iter = Block.create_iter t.index_block in
  let m =
    let open Block.Iter in
    seek k >>= fun b -> match b with false -> fail "seek" | true -> value
  in
  let open Option in
  ( match Block.Iter.run m index_iter with
  | Ok (index_value, _) -> Some index_value
  | Error _ -> None )
  >>= fun index_value ->
  block_reader t index_value
  >>= fun block_iter ->
  match Block.Iter.run m block_iter with
  | Ok (block_value, _) -> Some block_value
  | Error _ -> None
