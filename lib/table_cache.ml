open Core
open Oleveldb_lib_util

module Make (Internal_cmp : Cmp.S) = struct
  module Table = Table.Make (Internal_cmp)

  module V : Lru.Weighted with type t = Table.t = struct
    type t = Table.t

    let weight _ = 1
  end

  (** file_num -> Table *)
  module Cache = Lru.F.Make (Int) (V)

  let default_cap = 20

  type t = {dbname: string; cache: Cache.t}

  let create dbname = {dbname; cache= Cache.empty default_cap}

  let find_table t file_num =
    match Cache.find file_num t.cache with
    | Some (table, cache) -> Ok (table, {t with cache})
    | None ->
        let table_name = Names.table_file_name t.dbname file_num in
        let file = In_channel.create table_name in
        match Table.create file with
        | Ok table ->
            let cache = Cache.add file_num table t.cache in
            Ok (table, {t with cache})
        | Error e -> Error e


  let create_iter t file_num =
    let open Result in
    find_table t file_num
    >>= fun (table, t) ->
    let iter = Table.create_iter table in
    return iter


  let get t file_num k =
    let open Result in
    find_table t file_num
    >>= fun (table, t) -> of_option ~error:"Table.get" (Table.get table k)

end

module Table_cache_test = Make (Cmp.ByteWiseComparator)
