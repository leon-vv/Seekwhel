
module Make : functor(C : module type of SeekwhelConnection) -> sig

	type t
	type result = unit

	val to_string : t -> string
	val exec : t -> result

	type target = SeekwhelSelect.Make(C).column_eq_default array 

	val q : table:string -> target -> t
end

