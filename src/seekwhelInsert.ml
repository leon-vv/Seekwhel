
module Make(C : SeekwhelConnection.S) = struct

	module SS = SeekwhelSelect.Make(C)
	module SC = SeekwhelColumn
	module SI = SeekwhelInner

	type target = SS.column_and_value array
	type result = unit

	type t = {
		target : target ;
		table: string
	} 

	let q ~table target = {target; table}

	let to_string {target; table} =
		let indent = 0 
		and indent_list lst = lst
			|> List.map (fun s -> "\t" ^ s)
			|> String.concat ",\n"
		in let (columns, values) =
			List.split
				(SS.strings_of_column_and_value_arr ~indent target)
		in let column_part = indent_list columns
		and values_part = indent_list values
		in "INSERT INTO " ^ SC.quoted_string_of_identifier table ^ " (\n"
			^ column_part ^ "\n)\n"
			^ "VALUES (\n"
			^ values_part ^ "\n)\n"

	let exec ins = SI.exec_ignore C.conn (to_string ins)

end

