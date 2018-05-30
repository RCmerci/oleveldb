open Core
open Oleveldb_lib_util
module Filter_block' = Filter_block.BloomFilterBlock

type option = {data_block_size: int; file: Out_channel.t}

type t =
  { pending_index_entry: bool
  ; last_key: string
  ; data_block: Block_builder.t
  ; index_block: Block_builder.t
  ; filter_block: Filter_block'.t
  ; pending_handle: Table_fmt.block_handle
  ; offset: int
  ; option: option }

let create option =
  { pending_index_entry= false
  ; last_key= ""
  ; data_block= Block_builder.create {block_restart_interval= 16}
  ; index_block= Block_builder.create {block_restart_interval= 16}
  ; filter_block= Filter_block'.create ()
  ; pending_handle= Table_fmt.empty_block_handle
  ; offset= 0
  ; option }


let write_raw_block t block_contents handle =
  let size = Slice.length block_contents in
  let handle' = Table_fmt.block_handle_set_offset handle t.offset in
  let handle'' = Table_fmt.block_handle_set_size handle' size in
  Slice.output block_contents t.option.file ;
  (* TODO: add compressionType and crc *)
  ({t with offset= t.offset + size}, handle'')


let write_block t block handle =
  let raw = Block_builder.finish block in
  write_raw_block t raw handle


let flush t =
  if Block_builder.is_empty t.data_block then t
  else
    let t', pending_handle = write_block t t.data_block t.pending_handle in
    let data_block' = Block_builder.reset t'.data_block in
    Filter_block'.start_block t'.filter_block t'.offset ;
    Out_channel.flush t'.option.file ;
    {t' with pending_index_entry= true; pending_handle; data_block= data_block'}


let add t ~k ~v =
  let t' =
    if t.pending_index_entry then
      let handle_encoding =
        Table_fmt.block_handle_encode_to t.pending_handle
      in
      Block_builder.add t.index_block
        (Slice.from_string t.last_key)
        handle_encoding ;
      {t with pending_index_entry= false}
    else t
  in
  Filter_block'.add_key t'.filter_block k ;
  let t'' = {t' with last_key= Slice.to_string k} in
  Block_builder.add t''.data_block k v ;
  let estimate_data_block_size =
    Block_builder.current_size_estimate t''.data_block
  in
  if estimate_data_block_size >= t''.option.data_block_size then flush t''
  else t''


let finish t =
  let filter_block_handle = Table_fmt.empty_block_handle in
  let metaindex_block_handle = Table_fmt.empty_block_handle in
  let index_block_handle = Table_fmt.empty_block_handle in
  let t' = flush t in
  (* filter block *)
  let filter_block_content = Filter_block'.finish t'.filter_block in
  let t'', filter_block_handle' =
    write_raw_block t' filter_block_content filter_block_handle
  in
  (* metaindex block *)
  let meta_index_block = Block_builder.create {block_restart_interval= 16} in
  let filter_block_handle_encoding =
    Table_fmt.block_handle_encode_to filter_block_handle'
  in
  Block_builder.add meta_index_block
    (Slice.from_string ("filter." ^ Filter_block'.policy_name))
    filter_block_handle_encoding ;
  let t''', metaindex_block_handle' =
    write_block t'' meta_index_block metaindex_block_handle
  in
  (* index block *)
  let t'''' =
    if t'''.pending_index_entry then
      let pending_handle_encoding =
        Table_fmt.block_handle_encode_to t'''.pending_handle
      in
      Block_builder.add t'''.index_block
        (Slice.from_string t'''.last_key)
        pending_handle_encoding ;
      {t''' with pending_index_entry= false}
    else t'''
  in
  let t''''', index_block_handle' =
    write_block t'''' t''''.index_block index_block_handle
  in
  (* footer *)
  let footer = Table_fmt.empty_footer in
  let footer' =
    Table_fmt.footer_set_metaindex_handle footer metaindex_block_handle'
  in
  let footer'' =
    Table_fmt.footer_set_index_handle footer' index_block_handle'
  in
  let footer_encoding = Table_fmt.footer_encode_to footer'' in
  Slice.output footer_encoding t'''''.option.file ;
  Out_channel.flush t'''''.option.file ;
  {t''''' with offset= t'''''.offset + Slice.length footer_encoding}
