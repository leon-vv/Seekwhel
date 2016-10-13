
module Make : functor(C : SeekwhelConnection.S)(SS : SeekwhelSelect.S)
-> sig

	type t

	val q : table:string -> t
	val where : bool SS.expr -> t -> t

	val to_string : t -> string
	val exec : t -> unit
end

