
module Make(C : SeekwhelConnection.S) = struct

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
		let indent = 0
		in "DELETE " ^ "FROM " ^ SC.quoted_string_of_identifier table
		^ SS.where_clause_of_optional_expr ~indent where ^ "\n"

	let exec del = SI.exec_ignore C.conn (to_string  del)
end
