open Core
open Stdint
open Oleveldb_lib_util

let max_bytes_for_level level =
  let r = 1024 * 1024 * 10 in
  let rec aux r n = if n <= 1 then r else aux (10 * r) (n - 1) in
  aux r level

(** 2M  *)
let max_file_size = 1024 * 1024 * 2

let total_file_size (files: Version_edit.file_metadata array) =
  Uint64.(Array.fold files ~init:(of_int 0) ~f:(fun r a -> a.size + r))

module Make (Internal_key_cmp : Dbfmt.Internal_key_comparator_S) = struct
  open Dbfmt

  let find_file (files: Version_edit.file_metadata array) key =
    let left = 0 in
    let right = Array.length files in
    let rec aux l r =
      if l >= r then r
      else
        let mid = (l + r) / 2 in
        let f = files.(mid) in
        match
          Internal_key_cmp.compare (Internal_key.to_slice f.largest) key
        with
        | Cmp.EQ | Cmp.GT -> aux l mid
        | Cmp.LT -> aux (mid + 1) r
    in
    let r = aux left right in
    if r = right then None else Some r

  let before_file user_key (f: Version_edit.file_metadata) =
    match
      Internal_key_cmp.User_comparator.compare user_key
        (Internal_key.get_user_key f.smallest)
    with
    | Cmp.LT -> true
    | _ -> false

  let after_file user_key (f: Version_edit.file_metadata) =
    match
      Internal_key_cmp.User_comparator.compare user_key
        (Internal_key.get_user_key f.largest)
    with
    | Cmp.GT -> true
    | _ -> false

  let some_file_overlaps_range disjoint_sorted_files
      (files: Version_edit.file_metadata array) smallest_user_key
      largest_user_key =
    if not disjoint_sorted_files then
      (* like level-0 *)
      Array.exists files (fun f ->
          not (after_file smallest_user_key f || before_file largest_user_key f)
      )
    else
      match find_file files smallest_user_key with
      | None -> false
      | Some i -> not (before_file largest_user_key files.(i))

  let get_range (files: Version_edit.file_metadata array) =
    Array.fold files ~init:None ~f:(fun r e ->
        match r with
        | None -> Some (e.smallest, e.largest)
        | Some (s, l) ->
          match
            ( Internal_key_cmp.compare
                (Internal_key.to_slice e.smallest)
                (Internal_key.to_slice s)
            , Internal_key_cmp.compare
                (Internal_key.to_slice e.largest)
                (Internal_key.to_slice l) )
          with
          | Cmp.LT, Cmp.GT -> Some (e.smallest, e.largest)
          | _, Cmp.GT -> Some (s, e.largest)
          | Cmp.LT, _ -> Some (e.smallest, l)
          | _ -> r )

  type version = {files: Version_edit.file_metadata array}

  let get_overlap_inputs version level (smallest: Internal_key.t option)
      (largest: Internal_key.t option) =
    let open Option in
    let open Internal_key in
    let user_begin = map smallest get_user_key in
    let user_end = map largest get_user_key in
    let compare = Internal_key_cmp.User_comparator.compare in
    let rec aux nth r user_begin user_end =
      if Array.length version.files <= nth then r
      else
        let f = (version.files).(nth) in
        let before =
          value_map user_begin ~default:false ~f:(fun user_begin ->
              Cmp.GT = compare user_begin (to_slice f.largest) )
        in
        let after =
          value_map user_end ~default:false ~f:(fun user_end ->
              Cmp.LT = compare user_end (to_slice f.smallest) )
        in
        if before && after then aux (nth + 1) r user_begin user_end
        else if level = 0 then
          let restart = ref false in
          let new_user_begin =
            value_map user_begin ~default:user_begin ~f:(fun user_begin ->
                if Cmp.GT = compare user_begin (to_slice f.smallest) then (
                  restart := true ;
                  Some (to_slice f.smallest) )
                else Some user_begin )
          in
          let new_user_end =
            value_map user_end ~default:user_end ~f:(fun user_end ->
                if Cmp.LT = compare user_end (to_slice f.largest) then
                  Some (to_slice f.largest)
                else Some user_end )
          in
          if !restart then aux 0 [] new_user_begin new_user_end
          else aux (nth + 1) (f :: r) user_begin user_end
        else aux (nth + 1) (f :: r) user_begin user_end
    in
    aux 0 [] user_begin user_end |> Array.of_list

  module Level_file_num_iterator : Iterator.S = struct
    type param = Version_edit.file_metadata array

    type t = {flist: Version_edit.file_metadata array; index: int}

    type 'a monad = t -> 'a * t

    let create param = {flist= param; index= 0}

    let seek_to_first t =
      if Array.length t.flist = 0 then (false, t) else (true, {t with index= 0})

    let seek_to_last t =
      let len = Array.length t.flist in
      if len = 0 then (false, t) else (true, {t with index= len - 1})

    let seek key t =
      let open Option in
      let r =
        find_file t.flist key >>= fun index -> return (true, {t with index})
      in
      Option.value r ~default:(false, t)

    let next t =
      if t.index + 1 >= Array.length t.flist then (false, t)
      else (true, {t with index= t.index + 1})

    let prev t =
      if t.index <= 0 then (false, t) else (true, {t with index= t.index - 1})

    let key t =
      let meta = (t.flist).(t.index) in
      (Internal_key.to_slice meta.largest, t)

    let value t =
      let meta = (t.flist).(t.index) in
      let buf = Slice.create 16 in
      Coding.append_fix64 buf meta.number ;
      Coding.append_fix64 buf meta.size ;
      (buf, t)

    let fold ~f ~init t =
      let rec aux r t =
        let k, _ = key t in
        let v, _ = value t in
        let r' = f r (k, v) in
        let exist, t' = next t in
        if exist then aux r' t' else (r', t)
      in
      let b, t' = seek_to_first t in
      match b with true -> aux init t' | false -> (init, t')

    let return a t = (a, t)

    let bind m ~f t =
      let a, t' = m t in
      f a t'

    let ( >>= ) m f = bind m ~f

    let ( >> ) m f = m >>= fun a -> return (f a)

    let fail s t = failwith s

    let run m t = Ok (m t)
  end
end
