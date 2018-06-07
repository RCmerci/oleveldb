type compare_result = EQ | GT | LT

module type S = sig
  val name : string

  val compare : Slice.t -> Slice.t -> compare_result
end

module ByteWiseComparator : S = struct
  let name = "bytewise_comparator"

  let compare a b =
    let a' = Slice.to_string a in
    let b' = Slice.to_string b in
    let r = String.compare a' b' in
    match r with 0 -> EQ | _ when r < 0 -> LT | _ -> GT

end
