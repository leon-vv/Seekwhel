(*
    Seekwhel OCaml library
    Copyright (C) 2016  Léon van Velzen

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*)

open CalendarLib 

module type Connection = sig
    val connection : Postgresql.connection
end

module Make (C : Connection) = struct
    exception Seekwhel_error of string	

    let seekwhel_fail s = raise (Seekwhel_error s)

    let exec_ignore st = ignore (C.connection#exec
			~expect:[Postgresql.Command_ok]
			st)

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
   
    (* Rename the table name in the column 'column'.
    We assume the column consists of three or two parts
    separated by dots (schema name, table name, column name) *)
    let rename_table_in_string column table_name =
	let parts = split_string_around_dots column
	in let new_parts = 
	    match parts with
		| [p1;p2;p3] -> [p1;table_name;p3]
		| [p1;p2] -> [table_name; p2]
		| _ -> seekwhel_fail ("Seekwhel: function 'rename_table', expected a column
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

    let ( <|| ) = rename_table

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
	
    let is_alphanum c =
	let c = Char.code c
	in 
	    (65 <= c && c <= 90) (* Uppercase letter *)
	    || (97 <= c && c <= 122) (* Lowercase letter *)
	    || (48 <= c && c <= 57) (* Number *)
	 
    let string_is_alphanum s =
	let rec inner s idx =
	    if String.length s == idx then true
	    else if not (is_alphanum (String.get s idx)) then false
	    else inner s (idx + 1)
	in inner s 0

    let safely_quote_identifier ident =
	if string_is_alphanum ident && (not (Keywords.is_keyword ident))
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
		    | Invalid -> seekwhel_fail "Could not quote identifier " ^ ident
			^ " because it contains invalid characters")

    (* This check shouldn't be neccessary. Identifiers (table
    and column names) should be static and thus not
    vulnerable to injections.  Still, somewhere, someone 
    will plug in dynamic values or even values supplied
    from an user interface. To keep the library safe
    we will make sure that all identifiers
    are quoted (and that no identifier includes a quote). *)
    let safely_quote_column s =
	let parts = split_string_around_dots s
	in let new_parts = List.map safely_quote_identifier parts
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
	    | Columnd _ -> date_of_string v
	    | Columnb _ -> bool_of_string v
	    | Column_custom { of_psql_string } -> of_psql_string v
    
	    | ColumniNull _ -> int_null_of_string v
	    | ColumnrNull _ -> float_null_of_string v
	    | ColumntNull _ -> string_null_of_string v
	    | ColumndNull _ -> date_null_of_string v
	    | ColumnbNull _ -> bool_null_of_string v
	    | ColumnCustomNull { of_psql_string } -> Some (of_psql_string v)

    module type Query = sig
	type t
	type result
	
	val to_string : t -> string
	val exec : t -> result
    end

    module Select = struct

	type join_direction =
	    | Left | Inner | Right

	let string_of_direction = function
	    | Left -> "LEFT"
	    | Inner -> "INNER"
	    | Right -> "RIGHT"

	type join = {
	    direction : join_direction ;
	    (* Table name and table abbreviation
		SQL: <direction> JOIN <name> <abbr>
		ON <left_column> = <right_column>
		see string_of_join *)
	    table_name : string * (string option) ;
	    left_column : string ;
	    right_column : string
	}

	let string_of_join {direction; table_name; left_column; right_column} =
	    let name = safely_quote_identifier (fst table_name)
	    and abbr = match (snd table_name) with
		| Some abbr -> " " ^ safely_quote_identifier abbr
		| None -> ""
	    in let name_part = name ^ " " ^ abbr
	    in string_of_direction direction ^ " JOIN " ^ name_part
		^ " ON " ^ left_column ^ " = " ^ right_column

	type 'a custom_expr = {
	    value : 'a ;
	    to_psql_string : 'a -> string ;
	    of_psql_string : string -> 'a
	}
    
	type order_dir =
	    | ASC
	    | DESC

	type 'a expr =
	    (* Column *)
	    | Column : 'a column -> 'a expr

	    (* Values *)
	    | Int : int -> int expr
	    | Real : float -> float expr
	    | Text : string -> string expr
	    | Date : Calendar.t -> Calendar.t expr
	    | Bool : bool -> bool expr
	    | Custom : 'a custom_expr -> 'a expr

	    (* Nullable values *)
	    | Null : ('a option) expr
	    | IntNull : int -> int option expr
	    | RealNull : float -> float option expr
	    | TextNull : string -> string option expr
	    | DateNull : Calendar.t -> Calendar.t option expr
	    | BoolNull : bool -> bool option expr
	    | CustomNull : 'a custom_expr -> 'a option expr

	    (* Math functions *)
	    | Random : float expr
	    | Sqrti : int expr -> float expr
	    | Sqrtr : float expr -> float expr
	    | Addi : int expr * int expr -> int expr
	    | Addr : float expr * float expr -> float expr
	    | Multi : int expr * int expr -> int expr
	    | Multr : float expr * float expr -> float expr
	    | Divi : int expr * int expr -> int expr
	    | Divr : float expr * float expr -> float expr
	    | Mod : int expr * int expr -> int expr
	    | Expr : float expr * float expr -> float expr
	    | Absi : int expr -> int expr
	    | Absr : float expr -> float expr
	    | Round : float expr * int -> float expr
	    | Ceil : float expr -> int expr
	    | Trunc : float expr -> int expr

	    (* String functions *)
	    | Concat : string expr * string expr -> string expr
	    | CharLength : string expr -> int expr
	    | Lower : string expr -> string expr
	    | Upper : string expr -> string expr
	    | Regex : string expr * string * bool -> bool expr (* bool: case sensitive *)

	    (* Date functions *)
	    | LocalTimeStamp : Calendar.t expr

	    (* Boolean *)
	    | IsNull : ('a option) expr -> bool expr
	    | Eq : 'a expr * 'a expr -> bool expr
	    | GT : 'a expr * 'a expr -> bool expr
	    | LT : 'a expr * 'a expr -> bool expr
	    | Not : bool expr -> bool expr
	    | And : bool expr * bool expr -> bool expr
	    | Or : bool expr * bool expr -> bool expr
	    | In : 'a expr * 'a expr list -> bool expr

	    (* Conditionals *)
	    | Case : bool expr * 'a expr * 'a expr -> 'a expr
	    | Max : 'a expr * 'a expr -> 'a expr
	    | Min : 'a expr * 'a expr -> 'a expr

	    (* Subqueries *)
	    | Exists : t -> bool expr

	    | AnyEq1 : 'a expr * t -> bool expr
	    | AnyEq2 : 'a expr * 'a expr * t -> bool expr
	    | AnyEqN : any_expr list * t -> bool expr

	    | AnyGt : 'a expr * t -> bool expr
	    | AnyLt : 'a expr * t -> bool expr

	    | AllEq1 : 'a expr * t -> bool expr
	    | AllEq2 : 'a expr * 'a expr * t -> bool expr
	    | AllEqN : any_expr list * t -> bool expr

	    | AllGt : 'a expr * t -> bool expr
	    | AllLt : 'a expr * t -> bool expr

	    (* Other *)
	    | Coalesce : ('a option) expr * 'a expr -> 'a expr
	    | Casti : 'a expr -> int expr
	    | Castr : 'a expr -> float expr
	    | Castt : 'a expr -> string expr
	    | Castd : 'a expr -> Calendar.t expr
	and any_expr = 
	    | AnyExpr : 'a expr -> any_expr
	and order_by = {
	    dir : order_dir ;
	    column : string 
	}
	and target = any_expr array
	and t = {
	    target : target ;
	    table: string ;
	    where : bool expr option ;
	    join : join list ;
	    limit : int option ;
	    order_by : order_by list ;
	    offset : int option
	}

	type result = Postgresql.result * target

	let string_of_limit = function
	    | None -> ""
	    | Some n -> " LIMIT " ^ (string_of_int n)

	let string_of_offset = function
	    | None -> ""
	    | Some n -> "OFFSET " ^ (string_of_int n)

	let string_of_order_dir = function
	    | DESC -> "DESC"
	    | ASC -> "ASC"

	let string_of_order_by_list lst =
	    let column_part = lst
		|> List.map (fun {dir; column = col} -> safely_quote_column col ^ " " ^ string_of_order_dir dir)
		|> String.concat ", "
	    in if column_part = "" then ""
		else "ORDER BY " ^ column_part
	   		

	let is_simple_value_constructor (type a) (x: a expr) =
	    match x with
	    | Column _ -> true
	    | Int _ -> true
	    | Real _ -> true
	    | Text _ -> true
	    | Date _ -> true
	    | Null -> true
	    | IntNull _ -> true
	    | RealNull _ -> true
	    | TextNull _ -> true
	    | DateNull _ -> true
	    | LocalTimeStamp -> true
	    | _ -> false

	let is_and = function
	    | And _ -> true
	    | _ -> false

	let is_or = function
	    | Or _ -> true
	    | _ -> false

	let within_paren s = "(" ^ s ^ ")"

	let rec nsim_soe : type a. a expr -> string = fun x ->
	    if is_simple_value_constructor x then string_of_expr x
	    else x |> string_of_expr |> within_paren

	and string_of_case : type a. a expr -> string = fun c ->
	    let rec when_of_case = function
		| Case (cond1, then1, else1) ->
		    (let w = "WHEN " ^ nsim_soe cond1 ^ " THEN " ^ nsim_soe then1 ^ " "
		    in match else1 with
			| Case _ ->
			    w ^ when_of_case else1
			| _ ->
			    w ^ "ELSE " ^ nsim_soe else1)
		| _ -> seekwhel_fail "Seekwhel: string_of_case called with a non-Case expression"
	    in 
		"CASE " ^ when_of_case c ^ " END"

			
	and string_of_expr : type a. a expr -> string =
	    fun expr ->
		let wp = within_paren

		(* not simple, expr around separator,
		see explanation above *)
		in let nsim_eas x1 x2 sep = nsim_soe x1 ^ sep ^ nsim_soe x2

		(* not simple, expr within func *)
		and nsim_ewf x f = f ^ wp (nsim_soe x)

		and concat_any_expr xs = 
		    xs
			|> List.map (fun (AnyExpr x) -> nsim_soe x)
			|> String.concat ", "
			|> wp

		(* select within parentheses *)
		and select_wp s = s |> to_string |> wp

		(* Separator between expression and select
		subquery *)
		in let sep_between_x_sel x sep sel =
		    nsim_soe x ^ sep ^ select_wp sel

		and escaped_string s = "'" ^ (C.connection#escape_string s) ^ "'"

		in match expr with
		    | Column c -> quoted_string_of_column c

		    | Int i -> string_of_int i
		    | Real f -> string_of_float f
		    | Text s -> escaped_string s
		    | Date d -> "'" ^ (Printer.Calendar.to_string d) ^ "'"
		    | Bool b -> string_of_bool b
		    | Custom {value; to_psql_string} -> to_psql_string value

		    | Null -> "NULL"
		    | IntNull i -> string_of_int i
		    | RealNull f -> string_of_float f
		    | TextNull s -> escaped_string s
		    | DateNull d -> "'" ^ (Printer.Calendar.to_string d) ^ "'"
		    | BoolNull b -> string_of_bool b
		    | CustomNull {value; to_psql_string} -> to_psql_string value

		    | Coalesce (x1, x2) -> "COALESCE(" ^ nsim_soe x1 ^ ", " ^ nsim_soe x2 ^ ")"
		    | Random -> "RANDOM()"
		    | Sqrti x -> "|/ " ^ nsim_soe x
		    | Sqrtr x -> "|/ " ^ nsim_soe x
		    | Addi (x1, x2) -> nsim_eas x1 x2 " + "
		    | Addr (x1, x2) -> nsim_eas x1 x2 " + "
		    | Multi (x1, x2) -> nsim_eas x1 x2 " * "
		    | Multr (x1, x2) -> nsim_eas x1 x2 " * "
		    | Divi (x1, x2) -> nsim_eas x1 x2 " / "
		    | Divr (x1, x2) -> nsim_eas x1 x2 " / "
		    | Mod (x1, x2) -> nsim_eas x1 x2 " % "
		    | Expr (x1, x2) -> nsim_eas x1 x2 " ^ "
		    | Absi x -> "@ " ^ nsim_soe x
		    | Absr x -> "@ " ^ nsim_soe x
		    | Round (x, i) -> "round(" ^ nsim_soe x ^ ", " ^ string_of_int i ^ ")"
		    | Ceil x -> "ceil(" ^ nsim_soe x ^ ")"
		    | Trunc x -> "trunc(" ^ nsim_soe x ^ ")"

		    | Concat (s1, s2) -> nsim_soe s1 ^ " || " ^ nsim_soe s2
		    | CharLength x -> nsim_ewf x "char_length"
		    | Lower x -> nsim_ewf x "lower"
		    | Upper x -> nsim_ewf x "upper"
		    | Regex (x, r, sensitive) ->
			let op = if sensitive then " ~ " else " ~* "
			in
			    nsim_soe x ^ op ^ escaped_string r

		    | LocalTimeStamp -> "LOCALTIMESTAMP"

		    | IsNull x -> nsim_soe x ^ " IS NULL"
		    | Eq (x1, x2) -> nsim_eas x1 x2 " = "
		    | GT (x1, x2) -> nsim_eas x1 x2 " > "
		    | LT (x1, x2) -> nsim_eas x1 x2 " < "
		    | Not x -> "NOT " ^ nsim_soe x

		    (* Some special checks to reduce parentheses
		    in ouput. This makes the query easier to read for humans,
		    which are the target specicies for this library. *)
		    | And (x1, x2) ->
			let left = if is_and x1 then string_of_expr x1 else nsim_soe x1
			and right = if is_and x2 then string_of_expr x2 else nsim_soe x2
			in left ^ " AND " ^ right

		    | Or (x1, x2) ->
			let left = if is_or x1 then string_of_expr x1 else nsim_soe x1
			and right = if is_or x2 then string_of_expr x2 else nsim_soe x2
			in left ^ " OR " ^ right

		    | In (x1, xs) ->
			nsim_soe x1 ^ " IN " ^ 
			wp (String.concat ", " (List.map nsim_soe xs))

		    | Case (cond, then_, else_) as c ->
			string_of_case c

		    | Max (left1, right1) ->
			let rec comma_sep_expressions = function
			    | Max (left2, right2) ->
				comma_sep_expressions left2
				    ^ ", " ^ comma_sep_expressions right2
			    | x -> nsim_soe x
			in "GREATEST(" ^ comma_sep_expressions left1 ^
			    ", " ^ comma_sep_expressions right1 ^ ")"
		    | Min (left1, right1) ->
			let rec comma_sep_expressions = function
			    | Min (left2, right2) ->
				comma_sep_expressions left2
				    ^ ", " ^ comma_sep_expressions right2
			    | x -> nsim_soe x
			in "LEAST(" ^ comma_sep_expressions left1 ^
			    ", " ^ comma_sep_expressions right1 ^ ")"

		    | Exists sel -> to_string sel

		    | AnyEq1 (x, select) ->
			sep_between_x_sel x " = ANY " select
		    | AnyEq2 (x1, x2, select) ->
			wp (nsim_soe x1 ^ "," ^ nsim_soe x2) ^ " = ANY " ^ select_wp select
		    | AnyEqN (lst, select) ->
			concat_any_expr lst ^ " = ANY " ^ select_wp select

		    | AnyGt (x, select) -> sep_between_x_sel x " > ANY " select
		    | AnyLt (x, select) -> sep_between_x_sel x " < ANY " select

		    | AllEq1 (x, select) ->
			sep_between_x_sel x " = ALL" select
		    | AllEq2 (x1, x2, select) ->
			wp (nsim_soe x1 ^ "," ^ nsim_soe x2) ^ " = ALL " ^ select_wp select
		    | AllEqN (lst, select) ->
			concat_any_expr lst ^ " = ALL " ^ select_wp select

		    | AllGt (x, select) -> sep_between_x_sel x " > ALL " select
		    | AllLt (x, select) -> sep_between_x_sel x " < ALL " select
		    
		    | Casti expr -> "CAST(" ^ nsim_soe expr ^ " AS INT)"
		    | Castr expr -> "CAST(" ^ nsim_soe expr ^ " AS DOUBLE PRECISION)"
		    | Castt expr -> "CAST(" ^ nsim_soe expr ^ " AS TEXT)"
		    | Castd expr -> "CAST(" ^ nsim_soe expr ^ " AS TIMESTAMP)"

	and where_clause_of_optional_expr = function
	    | None -> ""
	    | Some expr -> " WHERE " ^ string_of_expr expr
	and string_of_target target =
	    Array.(
		map (fun (AnyExpr x) -> string_of_expr x) target
		|> to_list
		|> String.concat ", ")

	and to_string {target; table; where; join; limit; order_by; offset} =
	    let target_part = string_of_target target
	    and sqi = safely_quote_identifier
	    in let first_part = " SELECT " ^ target_part ^ " FROM " ^ sqi table
	    and join_s = String.concat "\n" (List.map string_of_join join)
	    in let first_and_joined = first_part ^ "\n" ^ (if join_s = "" then "" else join_s ^ "\n")
	    in first_and_joined 
	    ^ (where_clause_of_optional_expr where)
	    ^ "\n" ^ string_of_order_by_list order_by
	    ^ "\n" ^ string_of_limit limit
	    ^ "\n" ^ string_of_offset offset


	let expr_of_value : type a. a -> a column -> a expr
	    = fun val_ col -> 
	    let maybe_null val_ f = match val_ with
		| None -> Null
		| Some v -> f v
	    in match col with
	    | Columni _ -> Int val_
	    | Columnr _ -> Real val_
	    | Columnt _ -> Text val_
	    | Columnd _ -> Date val_
	    | Columnb _ -> Bool val_
	    | Column_custom {to_psql_string; of_psql_string } ->
		Custom { value = val_ ; to_psql_string ; of_psql_string }

	    | ColumniNull _ -> maybe_null val_ (fun v -> IntNull v)
	    | ColumnrNull _ -> maybe_null val_ (fun v -> RealNull v)
	    | ColumntNull _ -> maybe_null val_ (fun v -> TextNull v)
	    | ColumndNull _ -> maybe_null val_ (fun v -> DateNull v)
	    | ColumnbNull _ -> maybe_null val_ (fun v -> BoolNull v)
	    | ColumnCustomNull {to_psql_string ; of_psql_string } ->
		maybe_null val_ (fun v ->
		    CustomNull { value = v ; to_psql_string ; of_psql_string })

	    
	type 'a expr_or_default =
	    | Expr of 'a expr
	    | Default

	type column_eq =
	    | ColumnEq : 'a column * 'a expr -> column_eq

	type column_eq_default =
	    | ColumnEqDefault : 'a column * 'a expr_or_default -> column_eq_default

	let string_of_column_value (type a) (c:a column) (v:a): string =
	    let maybe_null f v = match v with
		| None -> "NULL"
		| Some v -> f v
	    and soe = string_of_expr
	    in match c with
		| Columni _ -> soe (Int v)
		| Columnr _ -> soe (Real v)
		| Columnt _ -> soe (Text v)
		| Columnd _ -> soe (Date v)
		| Columnb _ -> soe (Bool v)
		| Column_custom {to_psql_string} -> to_psql_string v

		| ColumniNull _ -> maybe_null (fun i -> soe (IntNull i)) v
		| ColumnrNull _ -> maybe_null (fun f -> soe (RealNull f)) v
		| ColumntNull _ -> maybe_null (fun s -> soe (TextNull s)) v
		| ColumndNull _ -> maybe_null (fun d -> soe (DateNull d)) v
		| ColumnbNull _ -> maybe_null (fun b -> soe (BoolNull b)) v
		| ColumnCustomNull {to_psql_string} -> maybe_null (fun c ->  to_psql_string c) v

	let string_of_column_and_opt_expr (ColumnEqDefault (col, opt_expr)) = 
	    (string_of_column col, match opt_expr with
		| Expr x -> string_of_expr x
		| Default -> "DEFAULT")
	    
	let stringify_column_and_opt_expr_array arr =
	    List.map
		string_of_column_and_opt_expr
		(Array.to_list arr) 
	
	let expr_of_expr_list = function
	    | x1::x2::rest ->
		Some (List.fold_left
		    (fun and_x new_x -> And (and_x, new_x))
		    (And (x1, x2))
		    rest)
	    | [x1] -> Some x1
	    | _ -> None (* Can't create an expression from an empty list *)

	let q ~table target = {
	    target ;
	    table ;
	    where = None ;
	    join = [] ;
	    limit = None ;
	    order_by = [] ;
	    offset = None
	}

	let limit count query = { query with limit = Some count }

	let offset count query = { query with offset = Some count }

	let combine_optional_expr expr new_expr =
	    Some (match expr with
	    | None -> new_expr
	    | Some x -> And (x, new_expr))

	let where expr sel = {sel with where = (combine_optional_expr sel.where expr)}

	let order_by dir col sel = {sel with order_by = sel.order_by @ [{dir; column = col}]}

	let abbr_join : ?abbr:(string option)
	    -> string
	    -> join_direction
	    -> on:('a column*'a column)
	    -> t -> t =

	    fun ?abbr:(abbr=None) table_name direction ~on query ->
		    { query with join = query.join @ [{
			direction; 
			table_name = (table_name, abbr) ;
			left_column = quoted_string_of_column (fst on) ;
			right_column = quoted_string_of_column (snd on) 
		    }]}

	let join table_name dir ~on sel =
	    abbr_join table_name dir ~on sel

	(* Perform a join on the same table *)
	(* 'abbr' stand for 'abbreviation' *)
	let self_join table_name as_abbr dir ~on sel = 
	    let (`As abbr) = as_abbr
	    in abbr_join ~abbr:(Some abbr) table_name dir ~on sel

	let rec get_index_where ?idx:(idx=0) pred arr =
	    if idx > Array.length arr - 1 then None

	    else (
		let element = (Array.get arr idx) 
		in if pred element then (Some idx)
		else get_index_where ~idx:(idx+1) pred arr)

	let index_of_expr target expr =
	    get_index_where (fun x -> AnyExpr expr = x) target
 
	let rec expression_value_of_string : type a. a expr -> bool -> string -> a = fun e is_null s ->
	    let maybe_null f =
		if is_null then None
		else f s
	    in match e with
	    | Column c -> column_value_of_string c s
	    | Int _ -> int_of_string s 
	    | Real _ -> float_of_string s
	    | Text _ -> s
	    | Date _ -> date_of_string s
	    | Bool _ -> bool_of_string s
	    | Custom {of_psql_string} -> of_psql_string s

	    | Null -> None
	    | IntNull _ -> maybe_null int_null_of_string
	    | RealNull _ -> maybe_null float_null_of_string
	    | TextNull _ -> maybe_null string_null_of_string
	    | DateNull _ -> maybe_null date_null_of_string
	    | BoolNull _ -> maybe_null bool_null_of_string
	    | CustomNull {of_psql_string} ->
		maybe_null (fun s -> Some (of_psql_string s))

	    | Coalesce (_, x) -> expression_value_of_string x is_null s
	    | Random -> float_of_string s
	    | Sqrti _ -> float_of_string s
	    | Sqrtr _ -> float_of_string s
	    | Addi _ -> int_of_string s
	    | Addr _ -> float_of_string s
	    | Multi _ -> int_of_string s
	    | Multr _ -> float_of_string s
	    | Divi _ -> int_of_string s
	    | Divr _ -> float_of_string s
	    | Mod _ -> int_of_string s
	    | Expr _ -> float_of_string s
	    | Absi _ -> int_of_string s
	    | Absr _ -> float_of_string s
	    | Round _ -> float_of_string s
	    | Ceil _ -> int_of_string s
	    | Trunc _ -> int_of_string s

	    | Concat _ -> s
	    | CharLength _ -> int_of_string s
	    | Lower _ -> s
	    | Upper _ -> s
	    | Regex _ -> bool_of_string s

	    | LocalTimeStamp -> date_of_string s

	    | IsNull _ -> bool_of_string s
	    | Eq _ -> bool_of_string s
	    | GT _ -> bool_of_string s
	    | LT _ -> bool_of_string s
	    | Not _ -> bool_of_string s
	    | And _ -> bool_of_string s
	    | Or _ -> bool_of_string s
	    | In _ -> bool_of_string s

	    | Case (_, x1, _) -> expression_value_of_string x1 is_null s
	    | Max (x1, _) -> expression_value_of_string x1 is_null s
	    | Min (x1, _) -> expression_value_of_string x1 is_null s


	    | Exists _ -> bool_of_string s
	    | AnyEq1 _ -> bool_of_string s
	    | AnyEq2 _ -> bool_of_string s
	    | AnyEqN _ -> bool_of_string s
	    | AnyGt _ -> bool_of_string s
	    | AnyLt _ -> bool_of_string s
	    | AllEq1 _ -> bool_of_string s
	    | AllEq2 _ -> bool_of_string s
	    | AllEqN _ -> bool_of_string s
	    | AllGt _ -> bool_of_string s
	    | AllLt _ -> bool_of_string s

	    | Casti x -> int_of_string s
	    | Castr x -> float_of_string s
	    | Castt x -> s
	    | Castd x -> date_of_string s


	let expr_value_opt (qres, target) row expr =
	     match index_of_expr target expr with
		| Some idx -> Some (expression_value_of_string
				expr
				(qres#getisnull row idx)
				(qres#getvalue row idx))
		| None -> None


	let expr_value res row expr =
	    match expr_value_opt res row expr with
		| Some v -> v
		| None -> raise Not_found

	let is_null_opt (qres, target) row expr =
	    match index_of_expr target expr with
		| Some idx -> Some (qres#getisnull row idx)
		| None -> None
	
	let is_null res row expr =
	    match is_null_opt res row expr with
		| Some v -> v
		| None -> raise Not_found

	type 'a row_callback = {
	    get_value_opt : 'a. ('a expr -> 'a option ) ;
	    is_null_opt : 'a. ('a expr -> bool option) ;

	    get_value : 'a. ('a expr -> 'a) ;
	    is_null : 'a. ('a expr -> bool)
	} 

	let n_rows (r:result) = (fst r)#ntuples

	let column_callback_of_index (res:result) idx =
	    {
	    	get_value_opt = (fun x -> expr_value_opt res idx x) ;
	    	is_null_opt = (fun x -> is_null_opt res idx x) ;

		get_value = (fun x -> expr_value res idx x) ;
		is_null = (fun x -> is_null res idx x)
	    }

	let get_all callback (res:result) =
	    Array.init (fst res)#ntuples (fun idx ->
		    callback (column_callback_of_index res idx)) 


	let get_first callback (res:result) =
	    match (fst res)#ntuples with
		| 0 -> None
		| _ -> Some (callback (column_callback_of_index res 0))
	
	let get_unique callback (res:result) =
	    match (fst res)#ntuples with
		| 0 -> None
		| 1 -> Some (callback (column_callback_of_index res 0))
		| _ -> seekwhel_fail "Seekwhel: in function get_unique; select query
		resulted in more than one row" 

	let exec sel =
	    let res = C.connection#exec (to_string sel)
	    in let st = res#status
	    in if st != Postgresql.Command_ok && st != Postgresql.Tuples_ok
		then seekwhel_fail res#error
		else (res, sel.target)
    end

    module type WhereQuery = sig
	include Query
	val where : bool Select.expr -> t -> t ;;
    end

    module Insert = struct
	type target = Select.column_eq_default array
	type result = unit

	type t = {
	    target : target ;
	    table: string
	} 

	let q ~table target = {target; table}

	let to_string {target; table} =
	    let (columns, values) =
		List.split (Select.stringify_column_and_opt_expr_array target)
	    in let column_part = String.concat "," columns
	    and values_part = String.concat "," values
	    in "INSERT INTO " ^ safely_quote_identifier table ^
		" ( " ^ column_part ^ " ) " ^
		" VALUES ( " ^ values_part ^ " ) "
	
	let exec ins = exec_ignore (to_string ins)
    end

    module Update = struct
	type target = Select.column_eq_default array
	type t = {
	    target : target ;
	    table : string ;
	    where : bool Select.expr option
	}
	type result = unit

	let q ~table target = {target; table; where = None}

	let where expr query =
	    {query with where = (Select.combine_optional_expr query.where expr)}

	let to_string {target; table; where} =
	    let target_s = Select.stringify_column_and_opt_expr_array target
	    in let equals = List.map (fun (col, v) -> col ^ " = " ^ v) target_s
	    in " UPDATE " ^ (safely_quote_identifier table) ^ " SET "
	    ^ String.concat "," equals
	    ^ Select.where_clause_of_optional_expr where

	let exec upd = exec_ignore (to_string upd)
    end


    module Delete = struct
	type t = {
	    table: string ;
	    where : bool Select.expr option
	}
	let q ~table = {table; where = None}

	let where expr query =
	    {query with where = (Select.combine_optional_expr query.where expr)}
	
	let to_string {table; where} =
	    " DELETE " ^ " FROM " ^ safely_quote_identifier table
	    ^ Select.where_clause_of_optional_expr where

	let exec del = exec_ignore (to_string del)
    end


    type ('t, 'a) column_mapping = 
	('a column) * ('t -> 'a -> 't) * ('t -> 'a)

    type 't any_column_mapping =
	| AnyMapping : ('t, 'a) column_mapping -> 't any_column_mapping

    module type Table = sig
	val name : string 
	type t
    
	val empty : t
	val primary_key : string array

	val default_columns : string array

	val column_mappings : t any_column_mapping array
    end

    module Queryable (T : Table) = struct
	
	let select_q = Select.q ~table:T.name
	let update_q = Update.q ~table:T.name
	let insert_q = Insert.q ~table:T.name
	let delete_q = Delete.q ~table:T.name

	let t_of_callback cb =
	    Array.fold_left
		(fun t (AnyMapping (c, apply, _)) ->
		    let v = Select.(cb.get_value (Column c))
		    in apply t v)
		T.empty
		T.column_mappings ;;
	
	let columns = Array.map (fun (AnyMapping (col, _, _))
	    -> Select.AnyExpr (Select.Column col)) T.column_mappings

	let select_query_of_expr expr =
	    select_q columns
		|> Select.where expr

	let select expr =
	    select_query_of_expr expr
	    |> Select.exec
	    |> Select.get_all t_of_callback

	let result_of_expr_and_limit expr n =
	    select_query_of_expr expr
	    |> Select.limit n
	    |> Select.exec

	let select_first expr =
	    result_of_expr_and_limit expr 1
	    |> Select.get_first t_of_callback
	
	let select_unique expr =
	    (* If a limit of 1 would be used, this function
	    would never throw an exception, which it's supposed to
	    do when the expr does not uniquely reference a row *)
	    result_of_expr_and_limit expr 2
	    |> Select.get_unique t_of_callback
	    
	
	let column_value_array_of_t t =
	    Array.map
		(fun (AnyMapping (col, _, get)) ->
		    let val_ = get t
		    in Select.(if Array.mem (quoted_string_of_column col) T.default_columns
			then ColumnEqDefault (col, Default)
			else ColumnEqDefault (col, Expr (expr_of_value val_ col))))
		T.column_mappings ;;

	let insert ts =
	    Array.iter (fun t ->
	    	insert_q (column_value_array_of_t t)
			|> Insert.exec)
		ts
	
	let equal_expr col v =
	    Select.(Eq (Column col, expr_of_value v col))

	let primary_mappings = 
	    let lst = match Array.length T.primary_key with
		| 0 -> seekwhel_fail ("Seekwhel: primary key array cannot be empty
		    (table name: " ^ T.name ^ ")")
		| _ -> let lst = Array.to_list T.column_mappings
		    in List.filter (fun (AnyMapping (col, _, _)) ->
			Array.mem (string_of_column col) T.primary_key) lst
	    in if List.length lst > 0 then lst
		else seekwhel_fail ("Seekwhel: a column mapping of the primary keys
		    could not be found (table name: " ^ T.name ^ ")")

	let where_of_primary t =
	    let equals = List.map (fun (AnyMapping (col, _, get)) ->
		equal_expr col (get t)) primary_mappings
	    in let expr = Select.expr_of_expr_list equals
	    in match expr with
		| Some x -> x
		| None -> seekwhel_fail "Trying to build where clause of primary keys; but 
		    no primary keys where given" (* This exception can't happen
		    because of the assertion made when assigning 'primary_mappings' *)
	
	let update ts =
	    Array.iter (fun t ->
		let expr = where_of_primary t
		in update_q (column_value_array_of_t t)
		    |> Update.where expr
		    |> Update.exec)
		ts

	let delete ts =
	    Array.iter (fun t ->
		let expr = where_of_primary t
		in delete_q
		    |> Delete.where expr
		    |> Delete.exec)
		ts
	
	    
    end

    module Join2 (T1 : Table)(T2 : Table) = struct
	
	module Q1 = Queryable(T1)
	module Q2 = Queryable(T2)
 
	type ('a, 'b) join_result =
	    | Left of 'a
	    | Right of 'b
	    | Both of ('a * 'b)

	let expr_of_col col = Select.Column col

	(* Todo: handle joining on the same table *)
	(* Todo: handle multiple joins *)
	let join dir ~on expr =
	    let columns = Array.append Q1.columns Q2.columns
	    in let result = Select.q
		    ~table:T1.name
		    columns
		|> Select.where expr
		|> Select.join T2.name dir ~on
		|> Select.exec
	    and expr_of_fst (c, _) = expr_of_col c
	    and expr_of_snd (_, c) = expr_of_col c

	    in let ordered_on = if Array.exists
		(fun e ->
		    Select.(
			e = AnyExpr (expr_of_fst on)))
		Q1.columns then on (* Correct order *)

		else (snd on, fst on) (* Swap *)
	    in 
		Select.get_all (fun cb ->
		    Select.(match (cb.is_null (expr_of_fst ordered_on),
				cb.is_null (expr_of_snd ordered_on)) with
			| (false, false) -> Both (Q1.t_of_callback cb, Q2.t_of_callback cb)
			| (true, false) -> Right (Q2.t_of_callback cb)
			| (false, true) -> Left (Q1.t_of_callback cb)
			| (true, true) -> seekwhel_fail "Seekwhel: 'on' columns in join query both returned NULL"))
		    result
	

	let inner_join ~on expr =
	    let rows = join Select.Inner ~on expr
	    in
		Array.map (function
		    | Both res -> res
		    | _ -> seekwhel_fail "Seekwhel: 'on' columns in INNER join cannot be NULL; unexpected result")
		    rows
	
    end
end

