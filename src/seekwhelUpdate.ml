
module Make(C : SeekwhelConnection.S) = struct

	module SS = SeekwhelSelect.Make(C)
	module SC = SeekwhelColumn
	module SI = SeekwhelInner

	type target = SS.column_and_value array 

	type t = {
		target : target ;
		table : string ;
		where : bool SS.expr option
	}

	type result = unit

	let q ~table target = {target; table; where = None}

	let where expr query =
		{query with where = (SS.combine_optional_expr query.where expr)}

	let to_string {target; table; where} =
		let target_s = SS.stringify_column_and_opt_expr_array ~indent:2 target
		in let equals = List.map (fun (col, v) -> "\n\t" ^ col ^ " =\n\t\t" ^ v) target_s
		in "UPDATE " ^ (SC.quoted_string_of_identifier table)
		^ "\nSET" ^ String.concat "," equals
		^ SS.where_clause_of_optional_expr ~indent:0 where
		^ "\n"

	let exec upd = SI.exec_ignore C.conn (to_string upd)
end

