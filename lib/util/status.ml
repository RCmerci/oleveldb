open Core

type t = OK | Err of string

let isOK t = phys_equal t OK

let to_string t = match t with OK -> "OK" | Err s -> "Err: " ^ s
