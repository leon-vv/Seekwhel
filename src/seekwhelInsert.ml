
module Make(C : SeekwhelConnection.S) = struct

	module SS = SeekwhelSelect.Make(C)
	module SC = SeekwhelColumn
	module SI = SeekwhelInner

	type target = SS.column_eq_default array
	type result = unit

	type t = {
		target : target ;
		table: string
	} 

	let q ~table target = {target; table}

	let to_string {target; table} =
		let aux = fun indent ->
		let (columns, values) =
			List.split (SS.stringify_column_and_opt_expr_array ~indent target)
		in let column_part = String.concat "," columns
		and values_part = String.concat "," values
		in "INSERT INTO " ^ SC.safely_quote_identifier table ^
			" ( " ^ column_part ^ " ) " ^
			" VALUES ( " ^ values_part ^ " ) "
		in aux 0

	let exec ins = SI.exec_ignore C.conn (to_string ins)

end

