open Core
open OUnit2
open Oleveldb_lib
open Oleveldb_lib_util

let aux_log_write slice filename =
  Out_channel.with_file filename ~f:(fun f ->
      let log_w = Log_writer.create f in
      let s = Log_writer.add_record log_w slice in
      s )


let test_log_writer_normal _ =
  let stat =
    aux_log_write
      (Slice.from_string "test_log_writer_normal")
      "test_log_writer_normal"
  in
  assert_bool "" (Status.isOK stat)


let test_log_writer_multi_part _ =
  let stat =
    aux_log_write
      (Slice.from_string (String.make Log_fmt.kBlockSize 'a'))
      "test_log_writer_multi_part"
  in
  assert_bool "" (Status.isOK stat)


let test_log_reader_normal _ =
  let _ =
    aux_log_write
      (Slice.from_string "test_log_reader_normal")
      "test_log_reader_normal"
  in
  let s, stat =
    In_channel.with_file "test_log_reader_normal" ~f:(fun f ->
        let log_r = Log_reader.create f in
        let s, stat = Log_reader.read_record log_r in
        (s, stat) )
  in
  assert_equal (Slice.to_string s) "test_log_reader_normal"


let test_log_reader_multi_part_record _ =
  let _ =
    aux_log_write
      (Slice.from_string (String.make (Log_fmt.kBlockSize + 1) 'a'))
      "test_log_reader_multi_part_record"
  in
  let s, stat =
    In_channel.with_file "test_log_reader_multi_part_record" ~f:(fun f ->
        let log_r = Log_reader.create f in
        let s, stat = Log_reader.read_record log_r in
        (s, stat) )
  in
  assert_equal (Slice.to_string s) (String.make (Log_fmt.kBlockSize + 1) 'a')


let test_log_reader_multi_part_record2 _ =
  let _ =
    aux_log_write
      (Slice.from_string (String.make (2 * Log_fmt.kBlockSize + 1) 'a'))
      "test_log_reader_multi_part_record2"
  in
  let s, stat =
    In_channel.with_file "test_log_reader_multi_part_record2" ~f:(fun f ->
        let log_r = Log_reader.create f in
        let s, stat = Log_reader.read_record log_r in
        (s, stat) )
  in
  assert_equal (Slice.to_string s)
    (String.make (2 * Log_fmt.kBlockSize + 1) 'a')


let test_bloomfilter_1 _ =
  let t = Bloomfilter.create 10 in
  let dst = Slice.create 10 in
  Bloomfilter.create_filter t
    (List.map ["key1"; "key2"; "key3"; "keykey4"] ~f:Slice.from_string)
    dst ;
  assert_equal [true; true; true; false; true]
    [ Bloomfilter.key_may_match t (Slice.from_string "key1") dst
    ; Bloomfilter.key_may_match t (Slice.from_string "key2") dst
    ; Bloomfilter.key_may_match t (Slice.from_string "key3") dst
    ; Bloomfilter.key_may_match t (Slice.from_string "key4") dst
    ; Bloomfilter.key_may_match t (Slice.from_string "keykey4") dst ]


let test_bloomfilter_2 _ =
  let t = Bloomfilter.create 10 in
  let dst = Slice.from_string "233" in
  Bloomfilter.create_filter t
    (List.map ["key1"; "key2"; "key3"; "keykey4"] ~f:Slice.from_string)
    dst ;
  let _ = Slice.strip_head dst 3 in
  assert_equal [true; true; true; false; true]
    [ Bloomfilter.key_may_match t (Slice.from_string "key1") dst
    ; Bloomfilter.key_may_match t (Slice.from_string "key2") dst
    ; Bloomfilter.key_may_match t (Slice.from_string "key3") dst
    ; Bloomfilter.key_may_match t (Slice.from_string "key4") dst
    ; Bloomfilter.key_may_match t (Slice.from_string "keykey4") dst ]


let test_table_builder_1 _ =
  let t =
    Table_builder.create
      { data_block_size= 4 * 1024
      ; file= Out_channel.create "test_table_builder_1" }
  in
  let add t ~k ~v =
    Table_builder.add t (Slice.from_string k) (Slice.from_string v)
  in
  let t' =
    t |> add ~k:"k1" ~v:"v1" |> add ~k:"k2" ~v:"v2" |> add ~k:"k3" ~v:"v3"
    |> add ~k:"k4" ~v:"v4" |> add ~k:"k5" ~v:"v5" |> add ~k:"k6" ~v:"v6"
  in
  let _ = Table_builder.finish t' in
  ()


let test_block_iter_1 _ =
  let block_data = Util.Block_util.data_block_1 in
  let block = Option.value_exn (Block.create block_data) in
  let open Block.Iter_debug in
  let iter' = create block in
  let m =
    seek_to_first
    >>= fun _ ->
    prev
    >>= fun b0 ->
    next
    >>= fun b1 ->
    key
    >>= fun k1 ->
    value
    >>= fun v1 ->
    seek_to_last
    >>= fun _ ->
    prev
    >>= fun b2 ->
    key
    >>= fun k_last ->
    value
    >>= fun v_last ->
    return
      ( b0
      , b1
      , Slice.to_string k1
      , Slice.to_string v1
      , b2
      , Slice.to_string k_last
      , Slice.to_string v_last )
  in
  let open Result in
  match run m iter' with
  | Ok (v, t) -> assert_equal (false, true, "k1", "v1", true, "k20", "v20") v
  | Result.Error s -> Printf.printf "%s\n" s ; assert_equal s ""


let test_block_iter_2 _ =
  let block_data = Util.Block_util.data_block_1 in
  let block = Option.value_exn (Block.create block_data) in
  let open Block.Iter_debug in
  let iter' = Block.create_iter_debug block in
  let m =
    seek_to_last
    >>= fun _ ->
    next
    >>= fun b0 ->
    key
    >>= fun k0 ->
    value >>= fun v0 -> return (b0, Slice.to_string k0, Slice.to_string v0)
  in
  match run m iter' with
  | Ok (v, t) -> assert_equal (false, "k21", "v21") v
  | Result.Error s -> assert_equal s ""


let test_block_iter_3 _ =
  let block_data = Util.Block_util.data_block_1 in
  let block = Option.value_exn (Block.create block_data) in
  let open Block.Iter_debug in
  let iter' = Block.create_iter_debug block in
  let m =
    seek (Slice.from_string "k17")
    >>= fun b0 ->
    key
    >>= fun k0 ->
    value
    >>= fun v0 ->
    next
    >>= fun b1 ->
    key
    >>= fun k1 ->
    value
    >>= fun v1 ->
    prev
    >>= fun b2 ->
    key
    >>= fun k2 ->
    value
    >>= fun v2 ->
    return
      ( b0
      , Slice.to_string k0
      , Slice.to_string v0
      , b1
      , Slice.to_string k1
      , Slice.to_string v1
      , b2
      , Slice.to_string k2
      , Slice.to_string v2 )
  in
  match run m iter' with
  | Ok (v, t) ->
      assert_equal (true, "k17", "v17", true, "k18", "v18", true, "k17", "v17")
        v
  | Result.Error s -> assert_equal s ""


let test_block_iter_4 _ =
  let block_data = Util.Block_util.gen_block [] in
  let block = Option.value_exn (Block.create block_data) in
  let open Block.Iter_debug in
  let iter' = Block.create_iter_debug block in
  let m =
    seek_to_first
    >>= fun b0 ->
    seek_to_last
    >>= fun b1 -> key >>= fun k -> return (b0, b1, Slice.to_string k)
  in
  match run m iter' with
  | Ok (v, t) -> assert_equal (false, false, "") v
  | Result.Error s -> print_endline s ; assert_equal "" "1"


let test_block_iter_5 _ =
  let block_data = Util.Block_util.gen_block [("k0", "v0"); ("k2", "v2")] in
  let block = Option.value_exn (Block.create block_data) in
  let open Block.Iter_debug in
  let iter' = Block.create_iter_debug block in
  let m =
    seek (Slice.from_string "k1")
    >>= fun b ->
    key
    >>= fun k ->
    value >>= fun v -> return (b, Slice.to_string k, Slice.to_string v)
  in
  match run m iter' with
  | Ok (v, t) -> assert_equal (true, "k2", "v2") v
  | Error s -> print_endline s ; assert_equal "" "1"


let test_block_iter_6 _ =
  let block_data = Util.Block_util.data_block_1 in
  let block = Option.value_exn (Block.create block_data) in
  let open Block.Iter_debug in
  let iter' = Block.create_iter_debug block in
  let m =
    next
    >>= fun b ->
    key
    >>= fun k ->
    value >>= fun v -> return (b, Slice.to_string k, Slice.to_string v)
  in
  match run m iter' with
  | Ok (v, t) -> assert_equal (true, "k0", "v0") v
  | Error s -> print_endline s ; assert_equal "" "1"


let test_block_iter_7 _ =
  let block_data = Util.Block_util.data_block_1 in
  let block = Option.value_exn (Block.create block_data) in
  let open Block.Iter_debug in
  let iter' = Block.create_iter_debug block in
  let m = prev >>= fun b -> return b in
  match run m iter' with
  | Ok (v, t) -> assert_equal false v
  | Error s -> print_endline s ; assert_equal "" "1"


let tmp_test _ = assert_bool "" true

let suite =
  "suite"
  >::: [ "test_log_writer_normal" >:: test_log_writer_normal
       ; "test_log_writer_multi_part" >:: test_log_writer_multi_part
       ; "test_log_reader_normal" >:: test_log_reader_normal
       ; "test_log_reader_multi_part_record"
         >:: test_log_reader_multi_part_record
       ; "test_log_reader_multi_part_record2"
         >:: test_log_reader_multi_part_record2
       ; "test_bloomfilter_1" >:: test_bloomfilter_1
       ; "test_bloomfilter_2" >:: test_bloomfilter_2
       ; "test_table_builder_1" >:: test_table_builder_1
       ; "test_block_iter_1" >:: test_block_iter_1
       ; "test_block_iter_2" >:: test_block_iter_2
       ; "test_block_iter_3" >:: test_block_iter_3
       ; "test_block_iter_4" >:: test_block_iter_4
       ; "test_block_iter_5" >:: test_block_iter_5
       ; "test_block_iter_6" >:: test_block_iter_6
       ; "test_block_iter_7" >:: test_block_iter_7
       ; "tmp_test" >:: tmp_test ]


let () = run_test_tt_main suite
