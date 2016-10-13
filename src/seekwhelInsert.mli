
module Make : functor(C : SeekwhelConnection.S)(SS : SeekwhelSelect.S)
-> sig

	type t
	type result = unit

	val to_string : t -> string
	val exec : t -> result

	type target =
		| ColumnValue : SS.column_value array -> target
		| Select : SeekwhelColumn.any_column array * SS.t -> target


	val q : table:string -> target -> t
end

