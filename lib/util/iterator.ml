module type S = sig
  type t

  type 'a monad

  val seek_to_first : unit monad

  val seek_to_last : unit monad

  val seek : Slice.t -> unit monad

  val next : bool monad
  (** if current entry is the last one, bool=false  *)

  val prev : bool monad
  (** if current entry is the first one, bool=false  *)

  val key : Slice.t monad

  val value : Slice.t monad

  val fold : f:('r -> 'a -> 'r) -> init:'r -> 'r monad
  (** [fold ~f ~init] , first seek to first, then iter from first to last *)

  val return : 'a -> 'a monad

  val bind : 'a monad -> ('a -> 'b monad) -> 'b monad

  val run : 'a monad -> t -> 'a
end
