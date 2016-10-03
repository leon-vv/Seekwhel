
module Make(C : module type of SeekwhelConnection) = struct

	module SS = SeekwhelSelect.Make(C)
	module SC = SeekwhelColumn
	module SI = SeekwhelInner

	type t = {
		table: string ;
		where : bool SS.expr option
	}

	let q ~table = {table; where = None}

	let where expr query =
		{query with where = (SS.combine_optional_expr query.where expr)}

	let to_string {table; where} =
		let aux indent =
		" DELETE " ^ " FROM " ^ SC.safely_quote_identifier table
		^ SS.where_clause_of_optional_expr ~indent where
		in aux 0

	let exec del = SI.exec_ignore C.c (to_string  del)
end
