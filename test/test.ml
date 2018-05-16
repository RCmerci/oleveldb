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
       ; "tmp_test" >:: tmp_test ]


let () = run_test_tt_main suite
