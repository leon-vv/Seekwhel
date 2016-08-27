module type Connection = sig
    val connection : Postgresql.connection
end

module Make (C : Connection) = struct

    let exec_ignore st = ignore (C.connection#exec
			~expect:[Postgresql.Command_ok]
			st)

    type 'a column =
	| Columni : string -> int column
	| Columnf : string -> float column
	| Columnt : string -> string column


    (* Todo: test the shit out of this function *)	
    let split_string_around_dots str =
	let rec inner s idx previous_idx sub_list = 
	    if idx = -1 then (String.sub s 0 (previous_idx)) :: sub_list
	    else (
		if String.get s idx = '.'
		then 
		    let idx_plus = idx + 1
		    in let new_sub = String.sub s idx_plus (previous_idx - idx_plus) 
		    in inner s (idx-1) idx (new_sub::sub_list)

		else inner s (idx-1) previous_idx sub_list )

	in inner str 0 (String.length str - 1) []

    (* Rename the table name in the column 'column'.
    We assume the column consists of three or two parts
    separated by dots (schema name, table name, column name) *)
    let rename_table_in_string column table_name =
	let parts = split_string_around_dots table_name
	in let new_parts = 
	    match parts with
		| [p1;p2;p3] -> [p1;table_name;p3]
		| [p1;p2] -> [table_name; p2]
		| _ -> failwith ("Seekwhel: function 'rename_table', expected a column name
	    	of the format schema.tablename.column or tablename.column. Column "
	    	^ column ^ " was not of the expected format.")
	in String.concat "." new_parts


    let rename_table (type a) (c:a column) new_name :a column =
	let rtis = rename_table_in_string
	in match c with
	    | Columni cn -> Columni (rtis cn new_name)
	    | Columnf cn -> Columnf (rtis cn new_name)
	    | Columnt cn -> Columnt (rtis cn new_name)

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
	
    let quote_identifier ident =
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
	
    (* This check shouldn't be neccessary. Identifiers (table
    and column names) should be static and thus not
    vulnerable to injections.  Still, somewhere, someone 
    will plug in dynamic values or even values supplied
    from an user interface. To keep the library safe
    we will make sure that all identifiers
    are quoted (and that no identifier includes a quote). *)
    let safely_quote_string s =
	let parts = split_string_around_dots  s
	in let new_parts = List.map quote_identifier parts
	in String.concat "." new_parts

	
    let string_of_column (type a) (c:a column) =
	let s = match c with
	    | Columni s -> s
	    | Columnf s -> s
	    | Columnt s -> s
	in safely_quote_string s
	
    let column_value_of_string (type a) (c:a column) (v:string): a =
	match c with
	    | Columni _ -> int_of_string v
	    | Columnf _ -> float_of_string v
	    | Columnt _ -> C.connection#escape_string v
	
    type any_column =
	| AnyColumn : 'a column -> any_column


    let string_of_any_column = function
	| AnyColumn c -> string_of_column c


    type 'a tagged_value = 'a column * 'a;;

    let string_of_tagged_value (type a) (t:a tagged_value) =
	let soc = string_of_column in
	    match (fst t) with
		| Columni s as c -> (soc c, string_of_int (snd t))
		| Columnf s as c -> (soc c, string_of_float (snd t))
		| Columnt s as c -> (soc c, C.connection#escape_string (snd t))

    type any_tagged_value =
	| AnyTaggedValue : 'a tagged_value -> any_tagged_value ;;

    let stringify_any_tagged_value_array arr =
	List.map
	    (fun (AnyTaggedValue t) -> string_of_tagged_value t)
	    (Array.to_list arr) 

    type 'a slot =
	| Column : 'a column -> 'a slot
	| Int : int -> int slot
	| Float : float -> float slot
	| Text : string -> string slot

    let string_of_slot (type a) (expr:a slot) : string =
	match expr with
	    | Column c -> string_of_column c
	    | Int i -> string_of_int i
	    | Float f -> string_of_float f
	    | Text s -> "'" ^ (C.connection#escape_string s) ^ "'"

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
	type target = any_tagged_value array
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
	    in "INSERT INTO " ^ safely_quote_string table ^
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
	type target = any_column array 
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
	    let column_equal c = (
		string_of_column col = string_of_any_column c
	    )
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
	    let columns = String.concat "," Array.(to_list (map string_of_any_column target))
	    in let first_part = " SELECT " ^ columns ^ " FROM " ^ safely_quote_string table
	    and join_s = match join with
		| None -> ""
		| Some {table_name; direction; left_column; right_column} ->
		    let name_part = match table_name with
			| (t, None) -> safely_quote_string t
			| (t, Some abbr) -> safely_quote_string t ^ " " ^ safely_quote_string abbr

		    in string_of_direction direction ^ " JOIN " ^ name_part
		    ^ " ON " ^ left_column ^ " = " ^ right_column
	    in let first_and_joined = first_part ^ "\n" ^ (if join_s = "" then "" else join_s ^ "\n")
	    in match where with
		| Some (expr) -> first_and_joined ^ " WHERE " ^ string_of_expr expr
		| None -> first_and_joined
	
	let exec sel =
	    let res = C.connection#exec (to_string sel)
	    in let st = res#status
	    in if st != Postgresql.Command_ok && st != Postgresql.Tuples_ok
		then failwith res#error
		else (res, sel.target)
    end

    module Update = struct
	type target = any_tagged_value array
	type t = {
	    target : target ;
	    table : string ;
	    where : bool_expr
	}
	type result = unit

	let q ~table target where = {target; table; where }
	let where expr query = {query with where = (combine_expr query.where expr)}

	let to_string {target; table; where} =
	    let target_s = stringify_any_tagged_value_array target
	    in let equals = List.map (fun (col, v) -> col ^ " = " ^ v) target_s
	    in " UPDATE " ^ (safely_quote_string table) ^ " SET "
	    ^ " ( " ^ (String.concat "," equals) ^ " ) "
	    ^ " WHERE " ^ string_of_expr where

	let exec upd = exec_ignore (to_string upd)
    end


    module Delete = struct
	type t = {
	    table: string ;
	    where : bool_expr
	}
	let q ~table where = {table; where = where}
	let where expr query = {query with where = (combine_expr query.where expr)}
	
	let to_string {table; where} =
	    " DELETE " ^ " FROM " ^ safely_quote_string table
	    ^ " WHERE " ^ string_of_expr where

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
	val columns : any_column array 
	
	val column_mappings : t any_column_mapping array
    end

    module Queryable (T : Table) = struct
	
	let t_of_callback cb =
	    Array.fold_left
		(fun t (AnyMapping (c, apply, _)) ->
		    let v = Select.(cb.get_value) c
		    in apply t v)
		T.empty
		T.column_mappings ;;
	    
	let select expr =
	    Select.q
		~table:T.name
		T.columns
	    |> Select.where expr
	    |> Select.exec
	    |> Select.get_all t_of_callback
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
	    let columns = Array.append T1.columns T2.columns
	    in let result = Select.q
		    ~table:T1.name
		    columns
		|> Select.where expr
		|> Select.join T2.name dir ~on
		|> Select.exec
	    and ordered_on = if Array.exists
		(fun c ->
		    string_of_any_column c = string_of_column (fst on))
		T1.columns then on (* Correct order *)

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

