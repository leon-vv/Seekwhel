open CalendarLib
module SI = SeekwhelInner

type 'a custom_column = {
	name : string ;
	of_psql_string : string -> 'a ;
	to_psql_string : 'a -> string
} ;;

type 'a column =
	| Columni : string -> int column
	| Columnr : string -> float column
	| Columnt : string -> string column
	| Columnd : string -> Calendar.t column
	| Columnb : string -> bool column
	| Column_custom : 'a custom_column -> 'a column

	(* Nullable *)
	| ColumniNull : string -> int option column
	| ColumnrNull : string -> float option column
	| ColumntNull : string -> string option column
	| ColumndNull : string -> Calendar.t option column
	| ColumnbNull : string -> bool option column
	| ColumnCustomNull : 'a custom_column -> 'a option column

(* Rename the table name in the column 'column'.
We assume the column consists of three or two parts
separated by dots (schema name, table name, column name) *)
let rename_table_in_string column table_name =
	let parts = SI.split_string_around_dots column
	in let new_parts = 
		match parts with
		| [p1;p2;p3] -> [p1;table_name;p3]
		| [p1;p2] -> [table_name; p2]
		| _ -> SI.seekwhel_fail ("function 'rename_table', expected a column
			name of the format schema.tablename.column or tablename.column." ^
			" Column " ^ column ^ " was not of the expected format.")
		in String.concat "." new_parts

let rec rename_table (type a) (c:a column) new_name :a column =
	let rtis = rename_table_in_string
		in match c with
			| Columni cn -> Columni (rtis cn new_name)
			| Columnr cn -> Columnr (rtis cn new_name)
			| Columnt cn -> Columnt (rtis cn new_name)
			| Columnd cn -> Columnd (rtis cn new_name)
			| Columnb cn -> Columnb (rtis cn new_name)
			| Column_custom ({name} as cust) -> Column_custom {cust with name = rtis name new_name}

			| ColumniNull cn -> ColumniNull (rtis cn new_name)
			| ColumnrNull cn -> ColumnrNull (rtis cn new_name)
			| ColumntNull cn -> ColumntNull (rtis cn new_name)
			| ColumndNull cn -> ColumndNull (rtis cn new_name)
			| ColumnbNull cn -> ColumnbNull (rtis cn new_name)
			| ColumnCustomNull ({name} as cust) -> ColumnCustomNull {cust with name = rtis name new_name}

type parser_state = 
	| Begin (* Start *)
	| Read (* Read one or more characters (no quotes) *)
	| Quote_begin (* Read one quote at the start,
			and maybe some characters thereafter *)
	| Quote_end (* Read a quote at start,
			maybe some characters and then another quote *)
	| Invalid (* The string is not valid *)

(* The state transition the parser makes if in state 'state'
and reads a character 'c' *)
let parser_state_transition state c =
	match state with
		| Begin  when c != '"' -> Read
		| Begin -> Quote_begin

		| Read when c != '"' -> Read
		| Read -> Invalid

		| Quote_begin when c != '"' -> Quote_begin
		| Quote_begin -> Quote_end

		| Quote_end
		| Invalid -> Invalid

let fold_string_left f state str =
	let len = String.length str
	in let rec inner state idx =
		if idx >= len then state
		else inner (f state (String.get str idx)) (idx + 1)
	in inner state 0

let is_valid_char ch =
	let c = Char.code ch
	in 
		(65 <= c && c <= 90) (* Uppercase letter *)
		|| (97 <= c && c <= 122) (* Lowercase letter *)
		|| (48 <= c && c <= 57) (* Number *)
		|| ch = '_'
	
let string_no_invalid_chars s =
	let rec inner s idx =
		if String.length s == idx then true
		else if not (is_valid_char (String.get s idx)) then false
		else inner s (idx + 1)
	in inner s 0

let quoted_string_of_identifier ident =
	if string_no_invalid_chars ident
		&& (not (SeekwhelKeywords.is_keyword ident))
		then ident
		else (* Check for quotes and quotify *)
			(let st = fold_string_left
			parser_state_transition
			Begin
			ident
		and quotify s = "\"" ^ s ^ "\""
		in match st with
			| Begin -> "\"\"" (* Empty string *)
			| Read -> quotify ident
			| Quote_end -> ident
			| Quote_begin
			| Invalid -> SI.seekwhel_fail "could not quote identifier " ^ ident
			^ " because it contains invalid characters")

(* This check shouldn't be neccessary. Identifiers (table
and column names) should be static and thus not
vulnerable to injections.  Still, somewhere, someone 
will plug in dynamic values or even values supplied
from an user interface. To keep the library safe
we will make sure that all identifiers
are quoted (and that no identifier includes a quote). *)
let safely_quote_column s =
	let parts = SI.split_string_around_dots s
		in let new_parts = List.map quoted_string_of_identifier parts
		in String.concat "." new_parts


let string_of_column (type a) (c:a column) =
	match c with
		| Columni s -> s
		| Columnr s -> s
		| Columnt s -> s
		| Columnd s -> s
		| Columnb s -> s
		| Column_custom {name} -> name

		| ColumniNull s -> s
		| ColumnrNull s -> s
		| ColumntNull s -> s
		| ColumndNull s -> s
		| ColumnbNull s -> s
		| ColumnCustomNull {name} -> name


let quoted_string_of_column c = 
	c |> string_of_column |> safely_quote_column

let column_value_of_string (type a) (c:a column) (v:string): a =
	match c with
		| Columni _ -> int_of_string v
		| Columnr _ -> float_of_string v
		| Columnt _ -> v
		| Columnd _ -> SI.date_of_string v
		| Columnb _ -> bool_of_string v
		| Column_custom { of_psql_string } -> of_psql_string v

		| ColumniNull _ -> SI.int_null_of_string v
		| ColumnrNull _ -> SI.float_null_of_string v
		| ColumntNull _ -> SI.string_null_of_string v
		| ColumndNull _ -> SI.date_null_of_string v
		| ColumnbNull _ -> SI.bool_null_of_string v
		| ColumnCustomNull { of_psql_string } -> Some (of_psql_string v)

type ('t, 'a) column_mapping = 
	('a column) * ('t -> 'a -> 't) * ('t -> 'a)

type 't any_column_mapping =
	| AnyMapping : ('t, 'a) column_mapping -> 't any_column_mapping

