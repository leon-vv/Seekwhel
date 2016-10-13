
module Make(C : SeekwhelConnection.S)(SS : SeekwhelSelect.S) =
struct

	module SC = SeekwhelColumn
	module SI = SeekwhelInner

(*
	Possiblities for update:
		column with a value, array of these
	Possibilities for insert:
		either a select
		of a column wit a value, array of these
*)

	type target =
		| ColumnValue : SS.column_value array -> target
		| Select : SC.any_column array * SS.t -> target

	type result = unit

	type t = {
		target : target ;
		table: string
	} 

	let q ~table target = {target; table}

	let indent_list lst = lst
		|> List.map (fun s -> "\t" ^ s)
		|> String.concat ",\n"

	let string_of_target = function
		| ColumnValue col_val_arr -> 
			let (columns, values) =
				List.split
					(SS.strings_of_column_value_arr ~indent:0 col_val_arr)
			in let (column_part, values_part) =
				(indent_list columns, indent_list values)
			in " (\n" ^ column_part ^ "\n)\n"
				^ "VALUES (\n"
				^ values_part ^ "\n)\n"
		| Select (cols, sel_query) ->
			let column_part =
				SC.(
					cols
					|> Array.map
							(fun (AnyCol c) ->
							quoted_string_of_column c)
					|> Array.to_list)
			and sel_part = SS.to_string sel_query
			in " (\n" ^ indent_list column_part ^ "\n)\n"
				^ sel_part



	let to_string {target; table} =
		let target_string = string_of_target target
		in "INSERT INTO " ^ SC.quoted_string_of_identifier table
			^ target_string

	let exec ins = SI.exec_ignore C.conn (to_string ins)

end

