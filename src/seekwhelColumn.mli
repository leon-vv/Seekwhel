open CalendarLib

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

val rename_table : 'a column -> string -> 'a column

val safely_quote_identifier : string -> string

val string_of_column : 'a column -> string

val quoted_string_of_column : 'a column -> string

val column_value_of_string : 'a column -> string -> 'a

type ('t, 'a) column_mapping = 
	('a column) * ('t -> 'a -> 't) * ('t -> 'a)

type 't any_column_mapping =
	| AnyMapping : ('t, 'a) column_mapping -> 't any_column_mapping


