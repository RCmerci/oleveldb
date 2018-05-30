open Core

type t = {mutable buf: string; mutable pos: int; mutable len: int}

let create n =
  let n = if n < 1 then 1 else n in
  let s = String.create n in
  {buf= s; pos= 0; len= n}


let from_string s =
  let len = String.length s in
  {buf= s; pos= len; len}


let from_substring s ~pos ~len =
  let sub_s = String.sub s pos len in
  let len = String.length sub_s in
  {buf= sub_s; pos= len; len}


let length t = t.pos

let resize t n_more =
  let new_buf = String.create (t.len + n_more) in
  String.blit t.buf 0 new_buf 0 t.pos ;
  t.buf <- new_buf ;
  t.len <- t.len + n_more


let add_string t s =
  let s_len = String.length s in
  if s_len + t.pos > t.len then resize t (2 * s_len) ;
  String.blit s 0 t.buf t.pos s_len ;
  t.pos <- t.pos + s_len


let add_char t c =
  if t.pos + 1 > t.len then resize t 1 ;
  t.buf.[t.pos] <- c ;
  t.pos <- t.pos + 1


let output t f =
  let s = String.sub t.buf 0 t.pos in
  Out_channel.output_string f s


let input t f len =
  if t.pos + len > t.len then resize t len ;
  let len_ = In_channel.input f t.buf t.pos len in
  t.pos <- t.pos + len_ ;
  len_


let clear t = t.pos <- 0

let to_string t = String.sub t.buf 0 t.pos

let to_substring t ~pos ~len = to_string t |> String.sub ~pos ~len

let add_slice t1 t2 = add_string t1 (to_string t2)

let strip_head t n =
  if n >= t.pos then
    let r = to_string t in
    clear t ; r
  else
    let r = String.sub t.buf 0 n in
    let new_buf = String.sub t.buf n (t.pos - n) in
    t.buf <- new_buf ;
    t.pos <- t.pos - n ;
    t.len <- String.length new_buf ;
    r


let get t n : char Option.t = if n < t.pos then Some (t.buf).[n] else None

let get_exn t n = Option.value_exn (get t n)

let set t n c =
  assert (n < t.pos) ;
  t.buf.[n] <- c


let copy t = {buf= t.buf; pos= t.pos; len= t.len}
