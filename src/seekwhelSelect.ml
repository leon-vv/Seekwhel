open CalendarLib
open SeekwhelColumn

module SI = SeekwhelInner

module type S = sig

	type 'a custom_expr = {
		value : 'a ;
		to_psql_string : 'a -> string ;
		of_psql_string : string -> 'a
	}

	type order_dir =
		| ASC
		| DESC


	type t
	type result

	val to_string : t -> string
	val exec : t -> result

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
		| Subtracti : int expr * int expr -> int expr
		| Subtractr : float expr * float expr -> float expr
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

		(* Aggregate functions *)
		| Avgi : int expr -> float option expr
		| Avgr : float expr -> float option expr
		| BoolAnd : bool expr -> bool option expr
		| BoolOr : bool expr -> bool option expr
		| Count : int expr
		| CountExpr : 'a expr -> int expr

		| Maxi : int expr -> int option expr
		| Maxr : float expr -> float option expr
		| Maxd : Calendar.t expr -> Calendar.t option expr
		| Maxt : string expr -> string option expr

		| Mini : int expr -> int option expr
		| Minr : float expr -> float option expr
		| Mind : Calendar.t expr -> Calendar.t option expr
		| Mint : string expr -> string option expr

		| StringAgg : string expr * string -> string option expr

		| Sumi : int expr -> int option expr
		| Sumr : float expr -> float option expr

		(* Boolean *)
		| IsNull : ('a option) expr -> bool expr
		| Eq : 'a expr * 'a expr -> bool expr
		| GT : 'a expr * 'a expr -> bool expr
		| GTE : 'a expr * 'a expr -> bool expr
		| LT : 'a expr * 'a expr -> bool expr
		| LTE : 'a expr * 'a expr -> bool expr
		| Not : bool expr -> bool expr
		| And : bool expr * bool expr -> bool expr
		| Or : bool expr * bool expr -> bool expr
		| In : 'a expr * 'a expr list -> bool expr

		(* Conditionals *)
		| Case : bool expr * 'a expr * 'a expr -> 'a expr

		| Greatesti : int expr list -> int expr
		| Greatestr : float expr list -> float expr
		| Greatestd : Calendar.t expr list -> Calendar.t expr
		| Greatestt : string expr list -> string expr

		| Leasti : int expr list -> int expr
		| Leastr : float expr list -> float expr
		| Leastd : Calendar.t expr list -> Calendar.t expr
		| Leastt : string expr list -> string expr

		(* Subqueries *)
		| Subqueryi : t -> int option expr (* WARNING *)
		| Subqueryr : t -> float option expr (* WARNING *)
		| Subqueryt : t -> string option expr (* WARNING *)
		| Subqueryd : t -> Calendar.t option expr (* WARNING *)

		| Exists : t -> bool expr

		| AnyEq1 : 'a expr * t -> bool expr
		| AnyEq2 : 'a expr * 'a expr * t -> bool expr
		| AnyEqN : any_expr list * t -> bool expr

		| AnyGT : 'a expr * t -> bool expr
		| AnyGTE : 'a expr * t -> bool expr
		| AnyLT : 'a expr * t -> bool expr
		| AnyLTE : 'a expr * t -> bool expr

		| AllEq1 : 'a expr * t -> bool expr
		| AllEq2 : 'a expr * 'a expr * t -> bool expr
		| AllEqN : any_expr list * t -> bool expr

		| AllGT : 'a expr * t -> bool expr
		| AllGTE : 'a expr * t -> bool expr
		| AllLT : 'a expr * t -> bool expr
		| AllLTE : 'a expr * t -> bool expr

		(* Other *)
		| Coalesce : ('a option) expr * 'a expr -> 'a expr
		| Casti : 'a expr -> int expr
		| Castr : 'a expr -> float expr
		| Castt : 'a expr -> string expr
		| Castd : 'a expr -> Calendar.t expr

	and any_expr = 
		| AnyExpr : 'a expr -> any_expr

	and target = any_expr array


	val q : from:string -> target -> t

	val string_of_expr : ?indent:int -> 'a expr -> string

	val expr_of_value : 'a -> 'a column -> 'a expr
				
	(* The value of a column in a update or insert query *)
	type 'a value =
		| Default : 'a value
		| Expr : 'a expr -> 'a value
		| OptExpr : 'a expr -> 'a option value

	type column_value =
		| ColumnValue : 'a column * 'a value -> column_value
	
	val null : 'a option value

	val strings_of_column_value_arr
		: indent:int -> column_value array -> (string * string) list

	val distinct : any_expr list -> t -> t
	val where : bool expr-> t -> t
	val limit : int -> t -> t
	val offset : int -> t -> t
	val order_by : 'a expr -> order_dir -> t -> t
	val having : bool expr -> t -> t 

	val combine_optional_expr : bool expr option -> bool expr -> bool expr option
	val where_clause_of_optional_expr : indent:int -> bool expr option -> string

	(* Cross join is equal to INNER JOIN ON (TRUE) *)
	type join_direction =
		| Inner
		| Left
		| LeftOuter
		| Right
		| RightOuter
		| FullOuter

	val join : string
		-> join_direction
		-> on:bool expr
		-> t
		-> t

	type 'a row_callback = {
		get_value_opt : 'a. ('a expr -> 'a option ) ;
		is_null_opt : 'a. ('a expr -> bool option) ;

		get_value : 'a. ('a expr -> 'a) ;
		is_null : 'a. ('a expr -> bool)
	}

	val get_all : ('a row_callback -> 'b)
		-> result
		-> 'b array

	val get_first : ('a row_callback -> 'b)
		-> result
		-> 'b option

	val get_unique : ('a row_callback -> 'b)
		-> result
		-> 'b option


	val any : 'a expr -> any_expr array
	val col : 'a column -> 'a expr
	val any_col : 'a column -> any_expr array
	val (@||) : any_expr array -> 'a expr -> any_expr array
	val (|||) : any_expr array -> 'a column -> any_expr array

	val (&&||) : bool expr -> bool expr -> bool expr
	val (||||) : bool expr -> bool expr -> bool expr

	val (=||) : 'a expr -> 'a expr -> bool expr
	val (!||) : bool expr -> bool expr
	val (>||) : 'a expr -> 'a expr -> bool expr
	val (>=||) : 'a expr -> 'a expr -> bool expr
	val (<||) : 'a expr -> 'a expr -> bool expr
	val (<=||) : 'a expr -> 'a expr -> bool expr
	val (<>||) : 'a expr -> 'a expr -> bool expr

	val (+||) : int expr -> int expr -> int expr
	val (-||) : int expr -> int expr -> int expr
	val (+.||) : float expr -> float expr -> float expr
	val (-.||) : float expr -> float expr -> float expr
end

module Make (C : SeekwhelConnection.S) = struct
	(* Cross join is equal to INNER JOIN ON (TRUE) *)

	type join_direction =
		| Inner
		| Left
		| LeftOuter
		| Right
		| RightOuter
		| FullOuter

	let string_of_direction = function
		| Inner -> "INNER"
		| Left -> "LEFT"
		| LeftOuter -> "LEFT OUTER"
		| Right -> "RIGHT"
		| RightOuter -> "RIGHT OUTER"
		| FullOuter -> "FULL OUTER"

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
		| Subtracti : int expr * int expr -> int expr
		| Subtractr : float expr * float expr -> float expr
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

		(* Aggregate functions *)
		| Avgi : int expr -> float option expr
		| Avgr : float expr -> float option expr
		| BoolAnd : bool expr -> bool option expr
		| BoolOr : bool expr -> bool option expr
		| Count : int expr
		| CountExpr : 'a expr -> int expr

		| Maxi : int expr -> int option expr
		| Maxr : float expr -> float option expr
		| Maxd : Calendar.t expr -> Calendar.t option expr
		| Maxt : string expr -> string option expr

		| Mini : int expr -> int option expr
		| Minr : float expr -> float option expr
		| Mind : Calendar.t expr -> Calendar.t option expr
		| Mint : string expr -> string option expr

		| StringAgg : string expr * string -> string option expr

		| Sumi : int expr -> int option expr
		| Sumr : float expr -> float option expr

		(* Boolean *)
		| IsNull : ('a option) expr -> bool expr
		| Eq : 'a expr * 'a expr -> bool expr
		| GT : 'a expr * 'a expr -> bool expr
		| GTE : 'a expr * 'a expr -> bool expr
		| LT : 'a expr * 'a expr -> bool expr
		| LTE : 'a expr * 'a expr -> bool expr
		| Not : bool expr -> bool expr
		| And : bool expr * bool expr -> bool expr
		| Or : bool expr * bool expr -> bool expr
		| In : 'a expr * 'a expr list -> bool expr

		(* Conditionals *)
		| Case : bool expr * 'a expr * 'a expr -> 'a expr

		| Greatesti : int expr list -> int expr
		| Greatestr : float expr list -> float expr
		| Greatestd : Calendar.t expr list -> Calendar.t expr
		| Greatestt : string expr list -> string expr

		| Leasti : int expr list -> int expr
		| Leastr : float expr list -> float expr
		| Leastd : Calendar.t expr list -> Calendar.t expr
		| Leastt : string expr list -> string expr

		(* Subqueries *)
		| Subqueryi : t -> int option expr (* WARNING *)
		| Subqueryr : t -> float option expr (* WARNING *)
		| Subqueryt : t -> string option expr (* WARNING *)
		| Subqueryd : t -> Calendar.t option expr (* WARNING *)

		| Exists : t -> bool expr

		| AnyEq1 : 'a expr * t -> bool expr
		| AnyEq2 : 'a expr * 'a expr * t -> bool expr
		| AnyEqN : any_expr list * t -> bool expr

		| AnyGT : 'a expr * t -> bool expr
		| AnyGTE : 'a expr * t -> bool expr
		| AnyLT : 'a expr * t -> bool expr
		| AnyLTE : 'a expr * t -> bool expr

		| AllEq1 : 'a expr * t -> bool expr
		| AllEq2 : 'a expr * 'a expr * t -> bool expr
		| AllEqN : any_expr list * t -> bool expr

		| AllGT : 'a expr * t -> bool expr
		| AllGTE : 'a expr * t -> bool expr
		| AllLT : 'a expr * t -> bool expr
		| AllLTE : 'a expr * t -> bool expr

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
		distinct : any_expr list option ;
		target : target ;
		from: string ;
		where : bool expr option ;
		join : join list ;
		limit : int option ;
		order_by : (any_expr * order_dir) list ;
		group_by : any_expr list ;
		having : bool expr option ;
		offset : int option
	}
	and join = {
		direction : join_direction ;
		table_name : string * (string option) ;
		on : bool expr
	}

	type result = Postgresql.result * target

	let rec whitespace_of_indent = function
		| 0 -> ""
		| n -> "\t" ^ whitespace_of_indent (n-1)

	let string_of_limit ~indent limit =
		let w = whitespace_of_indent indent
		in match limit with
			| None -> ""
			| Some n -> "\n" ^ w ^ "LIMIT " ^ (string_of_int n)

	let string_of_offset ~indent offset =
		let w = whitespace_of_indent indent
		in match offset with
			| None -> ""
			| Some n -> "\n" ^ w ^ "OFFSET " ^ (string_of_int n)

	let string_of_order_dir = function
			| DESC -> "DESC"
			| ASC -> "ASC"


	(*
		Expressions for which the surrounding
		expression will never yield different
		results depending on whether the expression is
		surrounded by parentheses. For example, Addi
		is not such an expression, since 2 * 5 + 2
		is different from 2 * (5 + 2). Basically, any
		value constructor or function (including casts)
		does not need surrounding parentheses.
	*)
	let no_paren_expr (type a) (x: a expr) =
		match x with
		| Column _ -> true

		| Int _ -> true
		| Real _ -> true
		| Text _ -> true
		| Date _ -> true
		| Bool _ -> true

		| Null -> true
		| IntNull _ -> true
		| RealNull _ -> true
		| TextNull _ -> true
		| DateNull _ -> true
		| BoolNull _ -> true

		| Coalesce _ -> true
		| Random -> true
		| Round _ -> true
		| Ceil _ -> true
		| Trunc _ -> true

		| CharLength _ -> true
		| Lower _ -> true
		| Upper _ -> true

		| LocalTimeStamp -> true

		| Avgi _ -> true
		| Avgr _ -> true
		| BoolAnd _ -> true
		| BoolOr _ -> true

		| Count -> true
		| CountExpr _ -> true
		| Maxi _ -> true
		| Maxr _ -> true
		| Maxd _ -> true
		| Maxt _ -> true

		| Mini _ -> true
		| Minr _ -> true
		| Mind _ -> true
		| Mint _ -> true

		| StringAgg _ -> true

		| Sumi _ -> true
		| Sumr _ -> true

		| Greatesti _ -> true
		| Greatestr _ -> true
		| Greatestd _ -> true
		| Greatestt _ -> true

		| Leasti _ -> true
		| Leastr _ -> true
		| Leastd _ -> true
		| Leastt _ -> true

		| Casti _ -> true
		| Castr _ -> true
		| Castt _ -> true
		| Castd _ -> true

		| _ -> false


	let is_and = function
		| And _ -> true
		| _ -> false

	let is_or = function
		| Or _ -> true
		| _ -> false

	let within_paren s = "(" ^ s ^ ")"

	(* Parentheses, string of expression.
	Converts the expression to string, and puts
	parentheses around it if needed (see no_paren_expr) *)
	let rec p_soe : type a. a expr -> indent:int -> string = fun x ~indent ->
		let s = string_of_expr ~indent x
		in if no_paren_expr x then s else within_paren s

	and string_of_case : type a. a expr -> indent:int -> string = fun c ~indent ->
		let rec when_of_case = function
			| Case (cond1, then1, else1) ->
				(let w = "WHEN " ^ p_soe ~indent cond1 ^ " THEN " ^ p_soe ~indent then1 ^ " "
				in match else1 with
				| Case _ ->
					w ^ when_of_case else1
				| _ ->
					w ^ "ELSE " ^ p_soe ~indent else1)
			| _ -> SI.seekwhel_fail "string_of_case called with a non-Case expression"
			in 
		"CASE " ^ when_of_case c ^ " END"

			
	and string_of_expr : type a. ?indent:int -> a expr ->  string =
		fun ?indent:(indent=0) expr ->
		let wp = within_paren

		(* Parenthses, expr around separator.
		Puts parentheses around the expressions if needed
		(see p_soe). *)
		in let p_eas x1 x2 sep = p_soe ~indent x1 ^ sep ^ p_soe ~indent x2

		(* parentheses, expr within func
		Puts parentheses around the expression if needed
		(see p_soe). *)
		and p_ewf x f = f ^ wp (string_of_expr ~indent x)

		and concat_any_expr xs = 
			xs
			|> List.map (fun (AnyExpr x) -> p_soe ~indent x)
			|> String.concat ", "
			|> wp

		and concat_expr xs =
			xs
			|> List.map (p_soe ~indent)
			|> String.concat ", "
			|> wp

		(* select within parentheses *)
		and subselect s =
			let w = whitespace_of_indent (indent+1)
			in "\n" ^ w ^ "(\n"
			^ to_string_indent ~indent:(indent+2) s
			^ w ^ ")"

		(* Separator between expression and select
		subquery *)
		in let sep_between_x_sel x sep sel =
			p_soe x ~indent ^ sep ^ subselect sel

		and escaped_string s = "'" ^ (C.conn#escape_string s) ^ "'"

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

			| Coalesce (x1, x2) -> "COALESCE(" ^ p_soe ~indent x1 ^ ", " ^ p_soe ~indent x2 ^ ")"
			| Random -> "RANDOM()"
			| Sqrti x -> "|/ " ^ p_soe ~indent x
			| Sqrtr x -> "|/ " ^ p_soe ~indent x
			| Addi (x1, x2) -> p_eas x1 x2 " + "
			| Addr (x1, x2) -> p_eas x1 x2 " + "
			| Subtracti (x1, x2) -> p_eas x1 x2 " - "
			| Subtractr (x1, x2) -> p_eas x1 x2 " - "
			| Multi (x1, x2) -> p_eas x1 x2 " * "
			| Multr (x1, x2) -> p_eas x1 x2 " * "
			| Divi (x1, x2) -> p_eas x1 x2 " / "
			| Divr (x1, x2) -> p_eas x1 x2 " / "
			| Mod (x1, x2) -> p_eas x1 x2 " % "
			| Expr (x1, x2) -> p_eas x1 x2 " ^ "
			| Absi x -> "@ " ^ p_soe ~indent x
			| Absr x -> "@ " ^ p_soe ~indent x
			| Round (x, i) -> "ROUND(" ^ p_soe ~indent x ^ ", " ^ string_of_int i ^ ")"
			| Ceil x -> "CEIL(" ^ p_soe ~indent x ^ ")"
			| Trunc x -> "TRUNC(" ^ p_soe ~indent x ^ ")"

			| Concat (s1, s2) -> p_soe ~indent s1 ^ " || " ^ p_soe ~indent s2
			| CharLength x -> p_ewf x "CHAR_LENGTH"
			| Lower x -> p_ewf x "LOWER"
			| Upper x -> p_ewf x "UPPER"
			| Regex (x, r, sensitive) ->
			let op = if sensitive then " ~ " else " ~* "
			in
				p_soe ~indent x ^ op ^ escaped_string r

			| LocalTimeStamp -> "LOCALTIMESTAMP"

			| Avgi x -> p_ewf x "AVG"
			| Avgr x -> p_ewf x "AVG"
			| BoolAnd x -> p_ewf x "BOOL_AND"
			| BoolOr x -> p_ewf x "BOOL_OR"

			| Count -> "COUNT(*)"
			| CountExpr x -> p_ewf x "COUNT"
			| Maxi x -> p_ewf x "MAX"
			| Maxr x -> p_ewf x "MAX"
			| Maxd x -> p_ewf x "MAX"
			| Maxt x -> p_ewf x "MAX"

			| Mini x -> p_ewf x "MIN"
			| Minr x -> p_ewf x "MIN"
			| Mind x -> p_ewf x "MIN"
			| Mint x -> p_ewf x "MIN"

			| StringAgg (x, del) ->
			"STRING_AGG" ^ wp (p_soe ~indent x ^ ", " ^ escaped_string del)

			| Sumi x -> p_ewf x "SUM"
			| Sumr x -> p_ewf x "SUM"

			| IsNull x -> p_soe ~indent x ^ " IS NULL"
			| Eq (x1, x2) -> p_eas x1 x2 " = "
			| GT (x1, x2) -> p_eas x1 x2 " > "
			| GTE (x1, x2) -> p_eas x1 x2 " >= "
			| LT (x1, x2) -> p_eas x1 x2 " < "
			| LTE (x1, x2) -> p_eas x1 x2 " <= "
			| Not x -> "NOT " ^ p_soe ~indent x

			(* Some special checks to reduce parentheses
			in ouput. This makes the query easier to read for humans,
			which are the target species for this library. *)
			| And (x1, x2) ->
			let w = whitespace_of_indent (indent+1)

			and left = if is_and x1
				then string_of_expr ~indent:(indent) x1
				else p_soe ~indent x1

			and right = if is_and x2
				then string_of_expr x2 ~indent:(indent)
				else p_soe ~indent x2

			in left ^ "\n" ^ w ^ "AND " ^ right

			| Or (x1, x2) ->
			let w = whitespace_of_indent (indent+1)

			and left = if is_or x1 
				then string_of_expr ~indent:(indent) x1
				else p_soe ~indent x1

			and right = if is_or x2
				then string_of_expr ~indent:(indent) x2
				else p_soe ~indent x2

			in left ^ "\n" ^ w ^ "OR " ^ right

			| In (x1, xs) ->
			p_soe ~indent x1 ^ " IN " ^ 
			wp (String.concat ", " (List.map (p_soe ~indent) xs))

			| Case (cond, then_, else_) as c ->
			string_of_case ~indent c

			| Greatesti xs -> "GREATEST" ^ concat_expr xs
			| Greatestr xs -> "GREATEST" ^ concat_expr xs
			| Greatestd xs -> "GREATEST" ^ concat_expr xs
			| Greatestt xs -> "GREATEST" ^ concat_expr xs

			| Leasti xs -> "LEAST" ^ concat_expr xs
			| Leastr xs -> "LEAST" ^ concat_expr xs
			| Leastd xs -> "LEAST" ^ concat_expr xs
			| Leastt xs -> "LEAST" ^ concat_expr xs


			| Subqueryi sel -> subselect sel
			| Subqueryr sel -> subselect sel
			| Subqueryt sel -> subselect sel
			| Subqueryd sel -> subselect sel

			| Exists sel -> "EXISTS " ^ subselect sel

			| AnyEq1 (x, select) ->
			sep_between_x_sel x " = ANY" select
			| AnyEq2 (x1, x2, select) ->
			wp (p_soe ~indent x1 ^ "," ^ p_soe ~indent x2) ^ " = ANY" ^ subselect select
			| AnyEqN (lst, select) ->
			concat_any_expr lst ^ " = ANY" ^ subselect select

			| AnyGT (x, select) -> sep_between_x_sel x " > ANY" select
			| AnyGTE (x, select) -> sep_between_x_sel x " >= ANY" select
			| AnyLT (x, select) -> sep_between_x_sel x " < ANY" select
			| AnyLTE (x, select) -> sep_between_x_sel x " <= ANY" select

			| AllEq1 (x, select) ->
			sep_between_x_sel x " = ALL" select
			| AllEq2 (x1, x2, select) ->
			wp (p_soe ~indent x1 ^ "," ^ p_soe ~indent x2) ^ " = ALL" ^ subselect select
			| AllEqN (lst, select) ->
			concat_any_expr lst ^ " = ALL" ^ subselect select

			| AllGT (x, select) -> sep_between_x_sel x " > ALL" select
			| AllGTE (x, select) -> sep_between_x_sel x " >= ALL" select
			| AllLT (x, select) -> sep_between_x_sel x " < ALL" select
			| AllLTE (x, select) -> sep_between_x_sel x " <= ALL" select
			
			| Casti expr -> "CAST(" ^ p_soe ~indent expr ^ " AS INT)"
			| Castr expr -> "CAST(" ^ p_soe ~indent expr ^ " AS DOUBLE PRECISION)"
			| Castt expr -> "CAST(" ^ p_soe ~indent expr ^ " AS TEXT)"
			| Castd expr -> "CAST(" ^ p_soe ~indent expr ^ " AS TIMESTAMP)"

	and where_clause_of_optional_expr ~indent opt_expr = 
		let w = whitespace_of_indent indent
		and wplus = whitespace_of_indent (indent + 1)
		in match opt_expr with
		| None -> ""
		| Some expr -> "\n" ^ w ^ "WHERE\n" ^ wplus
			^ string_of_expr ~indent expr

	and string_of_target ~indent target =
		let w = whitespace_of_indent (indent + 1)
		in Array.(
		map (fun (AnyExpr x) -> string_of_expr ~indent x) target
		|> to_list
		|> String.concat ("\n" ^ w ^ ", "))

	and string_of_from ~indent table =
		let w = whitespace_of_indent indent
		in 
		"\n" ^ w ^ "FROM " ^
		quoted_string_of_identifier table

	and string_of_where ~indent where = 
		(where_clause_of_optional_expr ~indent where)

	and string_of_distinct ~indent = function
		| None -> ""
		| Some [] -> "DISTINCT "
		| Some xs -> xs
		|> List.map (fun (AnyExpr x) -> string_of_expr ~indent x)
		|> String.concat ", "
		|> within_paren
		|> (fun s -> "DISTINCT ON " ^ s)

	and string_of_having ~indent having =
		let w = whitespace_of_indent indent
		in match having with
		| None -> ""
		| Some x -> "\n" ^ w ^ "HAVING " ^ string_of_expr ~indent x

	and string_of_order_by ~indent lst =
		let w = whitespace_of_indent indent
		and string_of_single ((AnyExpr x), dir) =
		(x |> p_soe ~indent)
		^ " " ^ string_of_order_dir dir
		in 
		match lst with
			| x::_ as xs -> "\n" ^ w ^ "ORDER BY " ^
			String.concat
				", "
				(List.map string_of_single xs)
			| _ -> ""

		and string_of_join ~indent {direction; table_name; on} =
		let w = whitespace_of_indent indent
		in let name = quoted_string_of_identifier (fst table_name)
		and abbr = match (snd table_name) with
		| Some abbr -> " " ^ quoted_string_of_identifier abbr
		| None -> ""
		in let name_part = name ^ abbr
		in "\n" ^ w ^ string_of_direction direction ^ " JOIN " ^ name_part
		^ " ON " ^ string_of_expr ~indent on

		and string_of_join_list ~indent joins =
		String.concat "\n" (List.map (string_of_join ~indent) joins)

	and string_of_group_by ~indent lst =
		let w = whitespace_of_indent indent
		in 
		match lst with
			| x::_ as xs -> "\n" ^ w ^ "GROUP BY " ^ (xs
			|> List.map (fun (AnyExpr x) -> x
				|> string_of_expr ~indent 
				|> within_paren)
				|> String.concat ", ")

			| _ -> ""

	and to_string_indent ~indent s =
		(* whitespace *)
		let w = whitespace_of_indent indent
		in w ^ "SELECT " ^ string_of_distinct ~indent s.distinct
		^ string_of_target ~indent s.target
		^ string_of_from ~indent s.from
		^ string_of_join_list ~indent s.join
		^ string_of_where ~indent s.where
		^ string_of_group_by ~indent s.group_by
		^ string_of_having ~indent s.having
		^ string_of_order_by ~indent s.order_by
		^ string_of_limit ~indent s.limit
		^ string_of_offset ~indent s.offset
		^ "\n"


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


	let to_string = to_string_indent ~indent:0
		
	(* The value of a column in a update or insert query *)
	type 'a value =
		| Default : 'a value
		| Expr : 'a expr -> 'a value
		(* A nullable column can be set to the value
		of a non-nullable expression *)
		| OptExpr : 'a expr -> 'a option value

	type column_value =
		| ColumnValue : 'a column * 'a value -> column_value

	let null = Expr Null

	let strings_of_column_value ~indent (ColumnValue (col, v)) =
		(quoted_string_of_column col, 
			match v with
				| Default -> "DEFAULT"
				| Expr e -> string_of_expr ~indent e
				| OptExpr e -> string_of_expr ~indent e)
	
	let strings_of_column_value_arr ~indent arr =
		List.map
			(strings_of_column_value ~indent)
			(Array.to_list arr) 

	let string_of_column_value (type a) (c:a column) (v:a) ~indent : string =
		let maybe_null f v = match v with
		| None -> "NULL"
		| Some v -> f v
		and soe = string_of_expr
		in match c with
		| Columni _ -> soe ~indent (Int v)
		| Columnr _ -> soe ~indent (Real v)
		| Columnt _ -> soe ~indent (Text v)
		| Columnd _ -> soe ~indent (Date v)
		| Columnb _ -> soe ~indent (Bool v)
		| Column_custom {to_psql_string} -> to_psql_string v

		| ColumniNull _ -> maybe_null (fun i -> soe ~indent (IntNull i)) v
		| ColumnrNull _ -> maybe_null (fun f -> soe ~indent (RealNull f)) v
		| ColumntNull _ -> maybe_null (fun s -> soe ~indent (TextNull s)) v
		| ColumndNull _ -> maybe_null (fun d -> soe ~indent (DateNull d)) v
		| ColumnbNull _ -> maybe_null (fun b -> soe ~indent (BoolNull b)) v
		| ColumnCustomNull {to_psql_string} -> maybe_null (fun c ->  to_psql_string c) v

	let q ~from target = {
		distinct = None ;
		target ;
		from ;
		where = None ;
		join = [] ;
		limit = None ;
		order_by = [] ;
		group_by = [] ;
		having = None ;
		offset = None
	}

	let distinct d query = { query with distinct = Some d }

	let limit count query = { query with limit = Some count }

	let offset count query = { query with offset = Some count }

	let group_by columns query = { query with group_by = columns }

	let having expr query = { query with having = Some expr }

	let combine_optional_expr expr new_expr =
		Some (match expr with
		| None -> new_expr
		| Some x -> And (x, new_expr))

	let where expr sel = {sel with where = (combine_optional_expr sel.where expr)}

	let order_by x dir sel = {sel with order_by = sel.order_by @ [(AnyExpr x, dir)]}

	let abbr_join : ?abbr:(string option)
		-> string
		-> join_direction
		-> on:bool expr
		-> t -> t =

		fun ?abbr:(abbr=None) table_name direction ~on query ->
			{ query with join = query.join @ [{
			direction; 
			table_name = (table_name, abbr) ;
			on
			}]}

	let join table_name dir ~on sel =
		abbr_join table_name dir ~on sel

	(* Perform a join on the same table *)
	(* 'abbr' stand for 'abbreviation' *)
	let self_join table_name as_abbr dir ~on sel = 
		let (`As abbr) = as_abbr
		in abbr_join ~abbr:(Some abbr) table_name dir ~on sel
	let index_of_expr target expr =
		SI.get_index_where (fun x -> AnyExpr expr = x) target

	let rec expression_value_of_string : type a. a expr -> bool -> string -> a = fun e is_null s ->
		let maybe_null f =
		if is_null then None
		else Some (f s)

		(* Because of table joins every column can be null *)
		and throw_if_null f =
		if is_null then SI.seekwhel_fail "Non-nullable column is null; use is_null when joining tables"
		else f s

		in let mn = maybe_null
		and tin = throw_if_null

		in let tini () = tin int_of_string
		and tinf () = tin float_of_string
		and tins () = tin (fun s -> s)
		and tind () = tin SI.date_of_string
		and tinb () = tin bool_of_string

		and mni () = mn int_of_string
		and mnf () = mn float_of_string
		and mns () = mn (fun s -> s)
		and mnd () = mn SI.date_of_string
		and mnb () = mn bool_of_string

		in match e with
		| Column c -> tin (column_value_of_string c)
		| Int _ -> tini ()
		| Real _ -> tinf ()
		| Text _ -> tins ()
		| Date _ -> tind ()
		| Bool _ -> tinb ()
		| Custom {of_psql_string} -> tin of_psql_string

		| Null -> None
		| IntNull _ -> mni ()
		| RealNull _ -> mnf ()
		| TextNull _ -> mns ()
		| DateNull _ -> mnd ()
		| BoolNull _ -> mnb ()
		| CustomNull {of_psql_string} -> mn of_psql_string

		| Coalesce (_, x) -> expression_value_of_string x is_null s
		| Random -> tinf ()
		| Sqrti _ -> tinf ()
		| Sqrtr _ -> tinf ()
		| Addi _ -> tini ()
		| Addr _ -> tinf ()
		| Subtracti _ -> tini ()
		| Subtractr _ -> tinf ()
		| Multi _ -> tini ()
		| Multr _ -> tinf ()
		| Divi _ -> tini ()
		| Divr _ -> tinf ()
		| Mod _ -> tini ()
		| Expr _ -> tinf ()
		| Absi _ -> tini ()
		| Absr _ -> tinf ()
		| Round _ -> tinf ()
		| Ceil _ -> tini ()
		| Trunc _ -> tini ()

		| Concat _ -> s
		| CharLength _ -> int_of_string s
		| Lower _ -> s
		| Upper _ -> s
		| Regex _ -> bool_of_string s

		| LocalTimeStamp -> SI.date_of_string s

		| Avgi _ -> mnf ()
		| Avgr _ -> mnf ()
		| BoolAnd _ -> mnb ()
		| BoolOr _ -> mnb ()
		| Count -> int_of_string s
		| CountExpr x -> int_of_string s
		
		| Maxi _ -> mni ()
		| Maxr _ -> mnf ()
		| Maxd _ -> mnd ()
		| Maxt _ -> mns ()
		
		| Mini _ -> mni ()
		| Minr _ -> mnf ()
		| Mind _ -> mnd ()
		| Mint _ -> mns ()
		
		| StringAgg _ -> mns ()
		
		| Sumi _ -> mni ()
		| Sumr _ -> mnf ()

		| IsNull _ -> tinb ()
		| Eq _ -> tinb ()
		| GT _ -> tinb ()
		| GTE _ -> tinb ()
		| LT _ -> tinb ()
		| LTE _ -> tinb ()
		| Not _ -> tinb ()
		| And _ -> tinb ()
		| Or _ -> tinb ()
		| In _ -> tinb ()

		| Case (_, x1, _) -> expression_value_of_string x1 is_null s
		| Greatesti _ -> tini ()
		| Greatestr _ -> tinf ()
		| Greatestd _ -> tind ()
		| Greatestt _ -> tins ()

		| Leasti _ -> tini ()
		| Leastr _ -> tinf ()
		| Leastd _ -> tind ()
		| Leastt _ -> tins ()

		| Subqueryi sel -> mni ()
		| Subqueryr sel -> mnf ()
		| Subqueryt sel -> mns ()
		| Subqueryd sel -> mnd ()


		| Exists _ -> tinb ()
		| AnyEq1 _ -> tinb ()
		| AnyEq2 _ -> tinb ()
		| AnyEqN _ -> tinb ()
		| AnyGT _ -> tinb ()
		| AnyGTE _ -> tinb ()
		| AnyLT _ -> tinb ()
		| AnyLTE _ -> tinb ()
		| AllEq1 _ -> tinb ()
		| AllEq2 _ -> tinb ()
		| AllEqN _ -> tinb ()
		| AllGT _ -> tinb ()
		| AllGTE _ -> tinb ()
		| AllLT _ -> tinb ()
		| AllLTE _ -> tinb ()

		| Casti x -> tini ()
		| Castr x -> tinf ()
		| Castt x -> tins ()
		| Castd x -> tind ()


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
		| _ -> SI.seekwhel_fail "in function get_unique; select query
		resulted in more than one row" 

	let exec sel =
		let res = C.conn#exec (to_string sel)
		in let st = res#status
		in if st != Postgresql.Command_ok && st != Postgresql.Tuples_ok
		then SI.seekwhel_fail res#error
		else (res, sel.target)


	let any x = [| AnyExpr x |]
	let col x = Column x
	let any_col c = c |> col |> any
	let (@||) xs x = Array.append xs [| AnyExpr x |]
	let (|||) cs c = Array.append cs (any_col c)

	let (&&||) x1 x2 = And (x1, x2)
	let (||||) x1 x2 = Or (x1, x2)

	let (=||) x1 x2 = Eq (x1, x2)
	let (!||) x = Not x
	let (>||) x1 x2 = GT (x1, x2)
	let (>=||) x1 x2 = GTE (x1, x2)
	let (<||) x1 x2 = LT (x1, x2)
	let (<=||) x1 x2 = LTE (x1, x2)
	let (<>||) x1 x2 = !|| (x1 =|| x2)

	let (+||) x1 x2 = Addi (x1, x2)
	let (-||) x1 x2 = Subtracti (x1, x2)
	let (+.||) x1 x2 = Addr (x1, x2)
	let (-.||) x1 x2 = Subtractr (x1, x2)
end


