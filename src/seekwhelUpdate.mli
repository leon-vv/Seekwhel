
module Make : functor(C : SeekwhelConnection.S)(SS : SeekwhelSelect.S)
-> sig

	type t
	type result = unit

	val to_string : t -> string
	val exec : t -> result

	type target = SS.column_value array

	val q : table:string -> target -> t

	val where : bool SS.expr -> t -> t
end

