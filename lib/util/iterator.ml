module type S = sig
  type param

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

  val fold : f:('r -> Slice.t * Slice.t -> 'r) -> init:'r -> 'r monad
  (** [fold ~f ~init] , first seek to first, then iter from first to last.
      f: result -> (k, v) -> result'
*)

  val return : 'a -> 'a monad

  val bind : 'a monad -> f:('a -> 'b monad) -> 'b monad

  val ( >>= ) : 'a monad -> ('a -> 'b monad) -> 'b monad

  val ( >> ) : 'a monad -> ('a -> 'b) -> 'b monad

  val run : 'a monad -> t -> ('a * t, string) result

  val create : param -> t
end
