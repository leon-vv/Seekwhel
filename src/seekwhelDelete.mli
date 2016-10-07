
module Make : functor(C : SeekwhelConnection.S) -> sig

	type t

	val q : table:string -> t
	val where : bool SeekwhelSelect.Make(C).expr -> t -> t

	val to_string : t -> string
	val exec : t -> unit
end

