open Oleveldb_lib_util
module Filter_block' = Filter_block.BloomFilterBlock

type option = {data_block_size: int; file: Core.Out_channel.t}

type t =
  { pending_index_entry: bool
  ; last_key: string
  ; data_block: Block_builder.t
  ; index_block: Block_builder.t
  ; filter_block: Filter_block'.t
  ; pending_handle: Table_fmt.block_handle
  ; offset: int
  ; option: option }

val create : option -> t

val add : t -> k:Slice.t -> v:Slice.t -> t

val finish : t -> t
