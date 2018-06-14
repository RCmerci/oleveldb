open Core
open Oleveldb_lib_util

module type Arg_S = sig
  type t
end

type ('a, 'b, 'c) param_ = {block_func: 'a; arg: 'b; index_iter: 'c}

module Two_level_iter_Make
    (Arg_ : Arg_S)
    (Index_iter : Iterator.S)
    (Data_iter : Iterator.S) =
struct
  type block_func_arg = Arg_.t

  type block_function = Arg_.t -> Slice.t -> Data_iter.t option

  type param =
    (Arg_.t -> Slice.t -> Data_iter.t option, Arg_.t, Index_iter.t) param_

  type t =
    { block_func: block_function
    ; arg: block_func_arg
    ; index_iter: Index_iter.t
    ; data_iter: Data_iter.t option
    ; data_block_handle: Slice.t }

  type 'a monad = t -> ('a * t, string) result

  let init_data_block t =
    let open Index_iter in
    value
    >>= fun handle ->
    if Option.is_none t.data_iter
       || Slice.to_string handle <> Slice.to_string t.data_block_handle
    then
      let data_iter_op = t.block_func t.arg handle in
      match data_iter_op with
      | Some _ ->
          return {t with data_iter= data_iter_op; data_block_handle= handle}
      | None -> fail "init_data_block"
    else return t


  let rec skip_empty_data_block_forward t =
    let open Index_iter in
    init_data_block t
    >>= fun t' ->
    assert (Option.is_some t'.data_iter) ;
    return Data_iter.(run seek_to_first (Option.value_exn t'.data_iter))
    >>= fun r ->
    match r with
    (* fine case *)
    | Ok (true, data_t) -> return (true, {t' with data_iter= Some data_t})
    (* empty data block, skip this data block *)
    | Ok (false, _) -> (
        next
        >>= fun b ->
        match b with
        (* go next index entry *)
        | true -> skip_empty_data_block_forward t'
        (* no more index entry *)
        | false -> return (false, t') )
    (* Data_iter inner Error, throw it *)
    | Error e -> fail ("skip_empty_data_block_forward, " ^ e)


  let rec skip_empty_data_block_backward t =
    let open Index_iter in
    init_data_block t
    >>= fun t' ->
    assert (Option.is_some t'.data_iter) ;
    return Data_iter.(run seek_to_first (Option.value_exn t'.data_iter))
    >>= fun r ->
    match r with
    | Ok (true, data_t) -> return (true, {t with data_iter= Some data_t})
    | Ok (false, _) -> (
        prev
        >>= fun b ->
        match b with
        | true -> skip_empty_data_block_backward t'
        | false -> return (false, t') )
    | Error e -> fail ("skip_empty_data_block_backward, " ^ e)


  let seek_to_first t =
    let m =
      let open Index_iter in
      seek_to_first
      >>= fun b0 ->
      match b0 with
      | true -> skip_empty_data_block_forward t
      | false -> return (false, t)
    in
    match Index_iter.run m t.index_iter with
    | Ok ((true, t), index_t) -> Ok (true, {t with index_iter= index_t})
    | Ok ((false, t), index_t) -> Ok (false, {t with index_iter= index_t})
    | Error e -> Error e


  let seek_to_last t =
    let m =
      let open Index_iter in
      seek_to_last
      >>= fun b0 ->
      match b0 with
      | false -> return (false, t)
      | true -> skip_empty_data_block_backward t
    in
    match Index_iter.run m t.index_iter with
    | Ok ((true, t), index_t) -> (
      match Data_iter.(run seek_to_last (Option.value_exn t.data_iter)) with
      | Ok (true, data_t) ->
          Ok (true, {t with index_iter= index_t; data_iter= Some data_t})
      | Ok (false, data_t) ->
          Ok (false, {t with index_iter= index_t; data_iter= Some data_t})
      | Error e -> Error e )
    | Ok ((false, t), index_t) -> Ok (false, t)
    | Error e -> Error e


  let seek k t =
    let open Index_iter in
    let m =
      seek k
      >>= fun b0 ->
      match b0 with
      | false -> return (false, t)
      | true ->
          init_data_block t
          >>= fun t ->
          return
            Data_iter.(
              let m = seek k in
              run m (Option.value_exn t.data_iter))
          >>= fun r ->
          match r with
          | Ok (true, data_t) -> return (true, {t with data_iter= Some data_t})
          | Ok (false, _) -> (
              next
              >>= fun b1 ->
              match b1 with
              | false -> return (false, t)
              | true -> skip_empty_data_block_forward t )
          | Error e -> fail ("seek, " ^ e)
    in
    match run m t.index_iter with
    | Ok ((true, t), index_t) -> Ok (true, {t with index_iter= index_t})
    | Ok ((false, t), index_t) -> Ok (false, t)
    | Error e -> Error e


  let next t =
    match t.data_iter with
    (* not init data block yet *)
    | None -> seek_to_first t
    | Some data_t ->
        let r = Data_iter.(run next data_t) in
        match r with
        | Ok (true, data_t) -> Ok (true, {t with data_iter= Some data_t})
        | Ok (false, _) -> (
            let m =
              let open Index_iter in
              next
              >>= fun b ->
              match b with
              | true -> skip_empty_data_block_forward t
              | false -> return (false, t)
            in
            match Index_iter.run m t.index_iter with
            | Ok ((true, t), index_t) -> Ok (true, {t with index_iter= index_t})
            | Ok ((false, t), index_t) ->
                Ok (false, {t with index_iter= index_t})
            | Error e -> Error e )
        | Error e -> Error e


  let prev t =
    match t.data_iter with
    | None -> Ok (false, t)
    | Some data_t ->
        let r = Data_iter.(run prev data_t) in
        match r with
        | Ok (true, data_t) -> Ok (true, {t with data_iter= Some data_t})
        | Ok (false, _) -> (
            let m =
              let open Index_iter in
              prev
              >>= fun b ->
              match b with
              | true -> skip_empty_data_block_backward t
              | false -> return (false, t)
            in
            match Index_iter.run m t.index_iter with
            | Ok ((true, t), index_t) -> Ok (true, {t with index_iter= index_t})
            | Ok ((false, t), index_t) ->
                Ok (false, {t with index_iter= index_t})
            | Error e -> Error e )
        | Error e -> Error e


  let key t =
    match t.data_iter with
    | None -> Ok (Slice.from_string "", t)
    | Some data_t ->
      match Data_iter.(run key data_t) with
      | Ok (k, data_t) -> Ok (k, {t with data_iter= Some data_t})
      | Error e -> Error e


  let value t =
    match t.data_iter with
    | None -> Ok (Slice.from_string "", t)
    | Some data_t ->
      match Data_iter.(run value data_t) with
      | Ok (v, data_t) -> Ok (v, {t with data_iter= Some data_t})
      | Error e -> Error e


  let fold ~f ~init t =
    let open Result in
    let rec aux r t =
      key t
      >>= fun (k, t) ->
      value t
      >>= fun (v, t) ->
      let r' = f r (k, v) in
      next t >>= fun (exist, t) -> if exist then aux r' t else return (r', t)
    in
    seek_to_first t
    >>= fun (b, t') ->
    match b with true -> aux init t' | false -> return (init, t')


  let return a t = Result.return (a, t)

  let bind m ~f t =
    let r = m t in
    Result.bind r (fun (a, t') -> f a t')


  let ( >>= ) m f = bind m ~f

  let ( >> ) m f = m >>= fun a -> return (f a)

  let fail s t = Error s

  let run m t = m t

  let create (p: param) =
    { block_func= p.block_func
    ; arg= p.arg
    ; index_iter= p.index_iter
    ; data_iter= None
    ; data_block_handle= Slice.from_string "" }

end

module type Make_S = functor (Arg_ :
  Arg_S) -> functor (Index_iter :
  Iterator.S) -> functor (Data_iter :
  Iterator.S) -> Iterator.S
                 with type param =
                             ( Arg_.t -> Slice.t -> Data_iter.t option
                             , Arg_.t
                             , Index_iter.t )
                             param_

module Make : Make_S = Two_level_iter_Make
