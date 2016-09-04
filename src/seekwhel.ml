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
 
    (* Begin helpful functions for testing *)

    let rec test1 f = function
	| (input, output)::rest ->
	    assert (f input = output) ;
	    test1 f rest 
	| [] -> ()

    let rec test2 f = function
	| (i1, i2, output)::rest ->
	    assert (f i1 i2 = output) ;
	    test2 f rest
	| [] -> ()

    let rec expect_exception f = function
	| input::is ->
	    let ex = (try f input; false
		with _ -> true)
	    in if ex then expect_exception f is
	    else failwith "No exception thrown"
	| [] -> ()
	

    (* End helpful functions for testing *)

    let exec_ignore st = ignore (C.connection#exec
			~expect:[Postgresql.Command_ok]
			st)

    type 'a column =
	| Columni : string -> int column
	| Columnf : string -> float column
	| Columnt : string -> string column
	| Columnd : string -> Calendar.t column
	(* Nullable *)
	| Columni_null : string -> (int option) column
	| Columnf_null : string -> (float option) column
	| Columnt_null : string -> (string option) column
	| Columnd_null : string -> (Calendar.t option) column

    (* Retrieve the substring of s between
    i1 and i2 (excluding i1 and i2) *)
    let sub_between s i1 i2 =
	let diff = i2 - i1
	in if diff < 2 then ""
	    else String.sub s (i1+1) (diff - 1) ;;

    let sub_between_test () =
	assert (sub_between "" 0 0 = "") ;
	assert (sub_between "a" 0 0 = "") ;
	assert (sub_between "ab" 0 0 = "") ;
	assert (sub_between "ab" 0 1 = "") ;
	assert (sub_between "abc" 0 1 = "") ;
	assert (sub_between "abc" 0 2 = "b") ;
	assert (sub_between "abcd" 1 3 = "c") ;
	assert (sub_between "abcdefg" 1 5 = "cde") ;
	assert (sub_between "abc" 0 3 = "bc") ;
	assert (sub_between "abc" (-1) 3 = "abc")

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
   	
    let split_string_around_dots_test () =
	test1 split_string_around_dots
	    [	("", [""]) ;
		("adf", ["adf"]) ;
		("#$%^", ["#$%^"]) ;
		("asé¶»«³", ["asé¶»«³"]) ;
		(".", [""; ""]) ;
		("...", [""; ""; ""; ""]) ;
		("a.b..d", ["a"; "b"; ""; "d"]) ;
		("asdf adsf.ads ..", ["asdf adsf"; "ads "; ""; ""]) ;
		(".a.b", [""; "a"; "b"]) ;
		("a.b.c", ["a"; "b"; "c"]) ] ;;


    (* Rename the table name in the column 'column'.
    We assume the column consists of three or two parts
    separated by dots (schema name, table name, column name) *)
    let rename_table_in_string column table_name =
	let parts = split_string_around_dots column
	in let new_parts = 
	    match parts with
		| [p1;p2;p3] -> [p1;table_name;p3]
		| [p1;p2] -> [table_name; p2]
		| _ -> failwith ("Seekwhel: function 'rename_table', expected a column
	    	name of the format schema.tablename.column or tablename.column." ^
	    	" Column " ^ column ^ " was not of the expected format.")
	in String.concat "." new_parts

    let rename_table_in_string_test () =
	let flip f = (fun x y -> f y x)
	in (expect_exception
	    ((flip rename_table_in_string) "table")
	    [""; "a"; "..."; "a.b.c.d"] ;
	test2 rename_table_in_string [
	    ("a.b", "def", "def.b") ;
	    ("a.b.c", "d", "a.d.c") ;
	    ("..", "a", ".a.") ;
	    (".", "a", "a.")
	])
	


    let rec rename_table (type a) (c:a column) new_name :a column =
	let rtis = rename_table_in_string
	in match c with
	    | Columni cn -> Columni (rtis cn new_name)
	    | Columnf cn -> Columnf (rtis cn new_name)
	    | Columnt cn -> Columnt (rtis cn new_name)
	    | Columnd cn -> Columnd (rtis cn new_name)

	    | Columni_null cn -> Columni_null (rtis cn new_name)
	    | Columnf_null cn -> Columnf_null (rtis cn new_name)
	    | Columnt_null cn -> Columnt_null (rtis cn new_name)
	    | Columnd_null cn -> Columnd_null (rtis cn new_name)

    let ( <|| ) = rename_table

    let rename_table_test () = assert (
	(Columni "products.stock" <|| "p1")
	    = Columni "p1.stock")
    
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
	
    let safely_quote_identifier ident =
	let st = fold_string_left
	    parser_state_transition
	    Begin
	    ident
	and quotify s = "\"" ^ s ^ "\""
	in match st with
	    | Begin -> "\"\"" (* Empty string *)
	    | Read -> quotify ident
	    | Quote_end -> ident
	    | Quote_begin
	    | Invalid -> failwith "Could not quote identifier " ^ ident
		^ " because it contains invalid characters"
	
    let quote_identifier_test () =
	expect_exception safely_quote_identifier
	    ["asdf\""; "\"sfd"; "\""; "aasf\"asdfasdf";
	    "\"\"a"; "a\"\""; "\"\"asdfad\"\""] ;
	test1 safely_quote_identifier [
	    ("", "\"\"");
	    ("a", "\"a\"");
	    ("\"\"", "\"\"");
	    ("\"a\"", "\"a\"")
	]
	
	    
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

    let safely_quote_string_test () =
	expect_exception safely_quote_column
	    ["\""; "adc.d\"d"; "a\""] ;
	test1 safely_quote_column
	    [("a.b.c", "\"a\".\"b\".\"c\"");
	    ("..", "\"\".\"\".\"\"");
	    ("\"a\".b", "\"a\".\"b\"");
	    ("a.d.\"k\"", "\"a\".\"d\".\"k\"");
	    ("\"\"", "\"\"")]
	

    let string_of_column (type a) (c:a column) =
	let s = match c with
	    | Columni s -> s
	    | Columnf s -> s
	    | Columnt s -> s
	    | Columnd s -> s

	    | Columni_null s -> s
	    | Columnf_null s -> s
	    | Columnt_null s -> s
	    | Columnd_null s -> s
	in safely_quote_column s
	
    let column_value_of_string (type a) (c:a column) (v:string): a =
	match c with
	    | Columni _ -> int_of_string v
	    | Columnf _ -> float_of_string v
	    | Columnt _ -> v
	    | Columnd _ -> Printer.Calendar.from_string v
    
	    | Columni_null _ -> Some (int_of_string v)
	    | Columnf_null _ -> Some (float_of_string v)
	    | Columnt_null _ -> Some v
	    | Columnd_null _ -> Some (Printer.Calendar.from_string v)

    let string_of_column_value (type a) (c:a column) (v:a): string =
	let maybe_null f v = match v with
	    | None -> "NULL"
	    | Some v -> f v
	in match c with
	    | Columni _ -> string_of_int v
	    | Columnf _ -> string_of_float v
	    | Columnt _ -> C.connection#escape_string v
	    | Columnd _ -> Printer.Calendar.to_string v

	    | Columni_null _ -> maybe_null string_of_int v
	    | Columnf_null _ -> maybe_null string_of_float v
	    | Columnt_null _ -> maybe_null C.connection#escape_string v
	    | Columnd_null _ -> maybe_null Printer.Calendar.to_string v
	    	
    type any_column =
	| AnyColumn : 'a column -> any_column


    let string_of_any_column = function
	| AnyColumn c -> string_of_column c

    type 'a value =
	| Value of 'a
	| Default

    type column_and_value =
	| ColumnValue : 'a column * 'a value -> column_and_value

    let string_of_column_and_value (ColumnValue (col, v)) = 
	(string_of_column col, match v with
	    | Value v -> string_of_column_value col v
	    | Default -> "DEFAULT")


    let stringify_any_tagged_value_array arr =
	List.map
	    string_of_column_and_value
	    (Array.to_list arr) 

    type 'a slot =
	(* Column *)
	| Column : 'a column -> 'a slot
	(* Values *)
	| Int : int -> int slot
	| Float : float -> float slot
	| Text : string -> string slot
	| Date : Calendar.t -> Calendar.t slot
	(* Nullable values *)
	| Null : ('a option) slot
	| Int_null : int -> int option slot
	| Float_null : float -> float option slot
	| Text_null : string -> string option slot
	| Date_null : Calendar.t -> Calendar.t option slot

    let string_of_slot (type a) (expr:a slot) : string =
	match expr with
	    | Column c -> string_of_column c

	    | Int i -> string_of_int i
	    | Float f -> string_of_float f
	    | Text s -> "'" ^ (C.connection#escape_string s) ^ "'"
	    | Date d -> "'" ^ (Printer.Calendar.to_string d) ^ "'"

	    | Null -> "NULL"
	    | Int_null i -> string_of_int i
	    | Float_null f -> string_of_float f
	    | Text_null s -> "'" ^ (C.connection#escape_string s) ^ "'"
	    | Date_null d -> "'" ^ (Printer.Calendar.to_string d) ^ "'"

    type bool_expr =
	| Eq : 'a slot * 'a slot -> bool_expr
	| Gt : 'a slot * 'a slot -> bool_expr
	| Lt : 'a slot * 'a slot -> bool_expr
	| Not : bool_expr -> bool_expr
	| And : bool_expr list -> bool_expr
	| Or : bool_expr list -> bool_expr

    let rec string_of_expr expr =
	let s = string_of_slot
	and map_join xs sep = 
	    let strings = List.map (fun x -> "(" ^ string_of_expr x ^ ")") xs
	    in String.concat sep strings
	in match expr with
	    | Eq (x1, x2) -> s x1 ^ " = " ^ s x2
	    | Gt (x1, x2) -> s x1 ^ " > " ^ s x2
	    | Lt (x1, x2) -> s x1 ^ " < " ^ s x2
	    | Not x -> " not (" ^ (string_of_expr x) ^ ")"
	    | And xs -> map_join xs " and "
	    | Or xs -> map_join xs " or " ;;

    let where_clause_of_optional_expr = function
	| None -> ""
	| Some expr -> " WHERE " ^ string_of_expr expr

    module type Query = sig
	type t
	type target
	type result
	
	val q : table:string -> target -> t
	val to_string : t -> string
	val exec : t -> result
    end

    module type WhereQuery = sig
	include Query
	val where : bool_expr -> t -> t ;;
    end

    module Insert = struct
	type target = column_and_value array
	type result = unit

	type t = {
	    target : target ;
	    table: string
	} 

	let q ~table target = {target; table}

	let to_string {target; table} =
	    let (columns, values) = List.split (stringify_any_tagged_value_array target)
	    in let column_part = String.concat "," columns
	    and values_part = String.concat "," values
	    in "INSERT INTO " ^ safely_quote_identifier table ^
		" ( " ^ column_part ^ " ) " ^
		" VALUES ( " ^ values_part ^ " ) "
	
	let exec ins = exec_ignore (to_string ins)
    end

    (* Add new_expr to the expression expr *)
    let combine_expr expr new_expr =
	match expr with
	    | (And xs) -> And (xs @ [new_expr])
	    | x -> And [x ; new_expr]
	    
    let combine_optional_expr expr new_expr =
	Some (match expr with
	| None -> new_expr
	| Some x -> combine_expr x new_expr )

    module Select = struct
	type target = string array
	type result = Postgresql.result * target

	type join_direction =
	    | Left | Inner | Right

	let string_of_direction = function
	    | Left -> "LEFT"
	    | Inner -> "INNER"
	    | Right -> "RIGHT"

	type join = {
	    direction : join_direction ;
	    (* Table name and table abbreviation *)
	    table_name : string * (string option) ;
	    left_column : string ;
	    right_column : string
	}

	type t = {
	    target : target ;
	    table: string ;
	    where : bool_expr option ;
	    join : join option
	}

	let q ~table target = {target; table; where = None; join = None}

	let where expr query = {query with where = (combine_optional_expr query.where expr)}

	let abbr_join : ?abbr:(string option)
	    -> string
	    -> join_direction
	    -> on:('a column*'a column)
	    -> t -> t =

	    fun ?abbr:(abbr=None) table_name direction ~on query ->
		let join = match query.join with
		    | None -> { direction; table_name = (table_name, abbr);
			left_column = string_of_column (fst on); 
			right_column = string_of_column (snd on);
		    }
		    | Some _ -> failwith "Select already contains join"
		in {query with join = Some join } 

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

	let index_of_column target col =
	    let column_equal c = (string_of_column col = c)
	    in get_index_where column_equal target

	let column_value (qres, target) row col =
	    match index_of_column target col with
		| Some idx -> let v = (qres#getvalue row idx) in 
				column_value_of_string col v
		| None -> raise Not_found
	
	let is_null (qres, target) row col =
	    match index_of_column target col with
		| Some idx -> qres#getisnull row idx
		| None -> raise Not_found

	type 'a column_callback = {
		get_value : 'a. ('a column -> 'a) ;
		is_null : 'a. ('a column -> bool)
	} 

	type ('a, 'b) row_callback = 'a column_callback -> 'b

	let n_rows (r:result) = (fst r)#ntuples

	let get_all callback (res:result) =
	    Array.init (fst res)#ntuples (fun idx ->
		    callback {
			get_value = (fun col -> column_value res idx col) ;
			is_null = (fun col -> is_null res idx col )
		    })

	let to_string {target; table; where; join} =
	    let columns = String.concat "," (Array.to_list target)
	    and sqi = safely_quote_identifier
	    in let first_part = " SELECT " ^ columns ^ " FROM " ^ sqi table
	    and join_s = match join with
		| None -> ""
		| Some {table_name; direction; left_column; right_column} ->
		    let name_part = match table_name with
			| (t, None) -> sqi t
			| (t, Some abbr) -> sqi t ^ " " ^ sqi abbr
		    in string_of_direction direction ^ " JOIN " ^ name_part
		    ^ " ON " ^ left_column ^ " = " ^ right_column
	    in let first_and_joined = first_part ^ "\n" ^ (if join_s = "" then "" else join_s ^ "\n")
	    in first_and_joined ^ (where_clause_of_optional_expr where)
	
	let exec sel =
	    let res = C.connection#exec (to_string sel)
	    in let st = res#status
	    in if st != Postgresql.Command_ok && st != Postgresql.Tuples_ok
		then failwith res#error
		else (res, sel.target)
    end

    module Update = struct
	type target = column_and_value array
	type t = {
	    target : target ;
	    table : string ;
	    where : bool_expr option
	}
	type result = unit

	let q ~table target = {target; table; where = None}
	let where expr query =
	    {query with where = (combine_optional_expr query.where expr)}

	let to_string {target; table; where} =
	    let target_s = stringify_any_tagged_value_array target
	    in let equals = List.map (fun (col, v) -> col ^ " = " ^ v) target_s
	    in " UPDATE " ^ (safely_quote_identifier table) ^ " SET "
	    ^ " ( " ^ (String.concat "," equals) ^ " ) "
	    ^ where_clause_of_optional_expr where

	let exec upd = exec_ignore (to_string upd)
    end


    module Delete = struct
	type t = {
	    table: string ;
	    where : bool_expr option
	}
	let q ~table = {table; where = None}

	let where expr query =
	    {query with where = (combine_optional_expr query.where expr)}
	
	let to_string {table; where} =
	    " DELETE " ^ " FROM " ^ safely_quote_identifier table
	    ^ where_clause_of_optional_expr where

	let exec del = exec_ignore (to_string del)
    end


    type ('t, 'a) column_mapping = 
	('a column) * ('t -> 'a -> 't) * ('t -> 'a)

    type 't any_column_mapping =
	| AnyMapping : ('t, 'a) column_mapping -> 't any_column_mapping

    module type Table = sig
	type t
    
	val empty : t
	val name : string 
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
		    let v = Select.(cb.get_value) c
		    in apply t v)
		T.empty
		T.column_mappings ;;
	
	let columns = Array.map (fun (AnyMapping (col, _, _))
	    -> string_of_column col) T.column_mappings

	let select expr =
	    select_q columns
	    |> Select.where expr
	    |> Select.exec
	    |> Select.get_all t_of_callback
	
	let tagged_value_array_of_t t =
	    Array.map
		(fun (AnyMapping (col, _, get)) ->
		    if Array.mem (string_of_column col) T.default_columns
			then ColumnValue (col, Default)
			else ColumnValue (col, Value (get t)))
		T.column_mappings ;;

	let insert ts =
	    Array.iter (fun t ->
	    	insert_q (tagged_value_array_of_t t)
			|> Insert.exec)
		ts
	
	let equal_expr (type a) (col:a column) (v:a) : bool_expr =
	    let maybe_null v f =
		match v with
		| None -> Null
		| Some v -> f v
	    in match col with
		| Columni _ -> Eq (Column col, Int v)
		| Columnf _ -> Eq (Column col, Float v)
		| Columnt _ -> Eq (Column col, Text v)
		| Columnd _ -> Eq (Column col, Date v)

		| Columni_null _ -> Eq (Column col, maybe_null v (fun v -> Int_null v))
		| Columnf_null _ -> Eq (Column col, maybe_null v (fun v -> Float_null v))
		| Columnt_null _ -> Eq (Column col, maybe_null v (fun v -> Text_null v))
		| Columnd_null _ -> Eq (Column col, maybe_null v (fun v -> Date_null v))

	let primary_mappings = 
	    assert (Array.length T.primary_key != 0) ;
	    let lst = Array.to_list T.column_mappings
	    in List.filter (fun (AnyMapping (col, _, _)) ->
		    Array.mem (string_of_column col) T.primary_key) lst

	let where_of_primary t =
	    let equals = List.map (fun (AnyMapping (col, _, get)) ->
		equal_expr col (get t)) primary_mappings
	    in And equals
	
	let update ts =
	    Array.iter (fun t ->
		let expr = where_of_primary t
		in update_q (tagged_value_array_of_t t)
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
	    and ordered_on = if Array.exists
		(fun c ->
		    c = string_of_column (fst on))
		Q1.columns then on (* Correct order *)

		else (snd on, fst on) (* Swap *)
	    in
		Select.get_all (fun cb ->
		    match Select.(cb.is_null (fst ordered_on), cb.is_null (snd ordered_on)) with
			| (false, false) -> Both (Q1.t_of_callback cb, Q2.t_of_callback cb)
			| (true, false) -> Right (Q2.t_of_callback cb)
			| (false, true) -> Left (Q1.t_of_callback cb)
			| (true, true) -> failwith "Seekwhel: 'on' columns in join query both returned NULL")
		    result
	

	let inner_join ~on expr =
	    let rows = join Select.Inner ~on expr
	    in
		Array.map (function
		    | Both res -> res
		    | _ -> failwith "Seekwhel: 'on' columns in INNER join cannot be NULL; unexpected result")
		    rows
	
    end
end

