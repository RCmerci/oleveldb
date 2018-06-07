open Core
open Oleveldb_lib
open Oleveldb_lib_util

module Block_util = struct
  let gen_block kv_list =
    let builder = Block_builder.create Block_builder.default_config_option in
    List.iter kv_list (fun (k, v) ->
        Block_builder.add builder (Slice.from_string k) (Slice.from_string v) ) ;
    let r = Block_builder.finish builder in
    r


  let data_block_1 =
    gen_block
      [ ("k0", "v0")
      ; ("k1", "v1")
      ; ("k2", "v2")
      ; ("k3", "v3")
      ; ("k4", "v4")
      ; ("k5", "v5")
      ; ("k6", "v6")
      ; ("k7", "v7")
      ; ("k8", "v8")
      ; ("k9", "v9")
      ; ("k10", "v10")
      ; ("k11", "v11")
      ; ("k12", "v12")
      ; ("k13", "v13")
      ; ("k14", "v14")
      ; ("k15", "v15")
      ; ("k16", "v16")
      ; ("k17", "v17")
      ; ("k18", "v18")
      ; ("k19", "v19")
      ; ("k20", "v20")
      ; ("k21", "v21") ]

end
