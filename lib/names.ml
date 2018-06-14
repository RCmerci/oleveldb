let make_filename name num suffix = Printf.sprintf "%s%06d.%s" name num suffix

let table_file_name name num = make_filename name num "ldb"

let log_file_name name num = make_filename name num "log"
