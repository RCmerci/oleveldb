open Core
open Oleveldb_lib
open Oleveldb_lib_util

let kv_1 =
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


let kv_2 =
  [ ("k0", "v0")
  ; ("k0", "v1")
  ; ("k0", "v2")
  ; ("k0", "v3")
  ; ("k0", "v4")
  ; ("k0", "v5")
  ; ("k0", "v6")
  ; ("k0", "v7")
  ; ("k0", "v8")
  ; ("k0", "v9")
  ; ("k0", "v10")
  ; ("k0", "v11")
  ; ("k0", "v12")
  ; ("k0", "v13")
  ; ("k0", "v14")
  ; ("k0", "v15")
  ; ("k0", "v16")
  ; ("k0", "v17")
  ; ("k0", "v18")
  ; ("k0", "v19")
  ; ("k0", "v20")
  ; ("k0", "v21")
  ; ("k0", "v22")
  ; ("k0", "v23")
  ; ("k0", "v24")
  ; ("k0", "v25")
  ; ("k0", "v26")
  ; ("k0", "v27")
  ; ("k0", "v28")
  ; ("k1", "v0")
  ; ("k1", "v1")
  ; ("k1", "v2")
  ; ("k1", "v3")
  ; ("k1", "v4")
  ; ("k3", "v0")
  ; ("k3", "v1")
  ; ("k3", "v2")
  ; ("k3", "v3")
  ; ("k3", "v4")
  ; ("k3", "v5")
  ; ("k3", "v6") ]


module Block_util = struct
  let gen_block kv_list =
    let builder = Block_builder.create Block_builder.default_config_option in
    List.iter kv_list (fun (k, v) ->
        Block_builder.add builder (Slice.from_string k) (Slice.from_string v) ) ;
    let r = Block_builder.finish builder in
    r


  let data_block_1 = gen_block kv_1

  let data_block_2 = gen_block kv_2
end

module Table_util = struct
  let gen_table_file kv_list name =
    let t =
      Table_builder.create
        {data_block_size= 4 * 1024; file= Out_channel.create name}
    in
    let add t k v =
      Table_builder.add t (Slice.from_string k) (Slice.from_string v)
    in
    let t = List.fold kv_list ~init:t ~f:(fun r (k, v) -> add r k v) in
    let _ = Table_builder.finish t in
    name


  let table_1 =
    let file =
      try In_channel.create "table_1" with Sys_error _ ->
        let _ = gen_table_file kv_1 "table_1" in
        In_channel.create "table_1"
    in
    file


  let table_2 =
    let file =
      try In_channel.create "table_2" with Sys_error _ ->
        let _ = gen_table_file kv_2 "table_2" in
        In_channel.create "table_2"
    in
    file

end
