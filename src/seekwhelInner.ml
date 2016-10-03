open CalendarLib
exception Seekwhel_error of string	

let seekwhel_fail s = raise (Seekwhel_error ("Seekwhel: " ^ s))

let exec_ignore (c:Postgresql.connection) st = ignore (c#exec
		~expect:[Postgresql.Command_ok]
		st)

let rec get_index_where ?idx:(idx=0) pred arr =
	if idx > Array.length arr - 1 then None
	else (
		let element = (Array.get arr idx) 
		in if pred element then (Some idx)
		else get_index_where ~idx:(idx+1) pred arr)

(* of_string function for each type *)
(* int_of_string, float_of_string, bool_of_string *)
let date_of_string = Printer.Calendar.from_string

let int_null_of_string s = Some (int_of_string s)
let float_null_of_string s = Some (float_of_string s)
let string_null_of_string s = Some ((fun s -> s) s)
let date_null_of_string s = Some (date_of_string s)
let bool_null_of_string s = Some (bool_of_string s)

(* Retrieve the substring of s between
i1 and i2 (excluding i1 and i2) *)
let sub_between s i1 i2 =
	let diff = i2 - i1
	in if diff < 2 then ""
		else String.sub s (i1+1) (diff - 1) ;;

let split_string_around_dots str =
	let rec inner s idx previous_idx sub_list = 
		if idx = -1 then (sub_between s (-1) previous_idx) :: sub_list
		else (
		if String.get s idx = '.'
		then 
			let new_sub = sub_between s idx previous_idx
			in inner s (idx-1) idx (new_sub::sub_list)

		else inner s (idx-1) previous_idx sub_list )

		in let length_minus = String.length str - 1
		in inner str length_minus (String.length str) []


