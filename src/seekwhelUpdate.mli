
module Make : functor(C : SeekwhelConnection.S)
-> sig

	type t
	type result = unit

	val to_string : t -> string
	val exec : t -> result

	type target = SeekwhelSelect.Make(C).column_and_value array

	val q : table:string -> target -> t

	val where : bool SeekwhelSelect.Make(C).expr -> t -> t
end

