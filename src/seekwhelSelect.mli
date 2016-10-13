open SeekwhelColumn
open CalendarLib

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


	(* Helper functions *)
	val any : 'a expr -> any_expr array
	val col : 'a column -> 'a expr
	val any_col : 'a column -> any_expr array
	val default : 'a column -> column_value

	module Infix : sig
		val (@||) : any_expr array -> 'a expr -> any_expr array
		val (|||) : any_expr array -> 'a column -> any_expr array

		val (=?||) : 'a option column -> 'a expr -> column_value
		val (==||) : 'a column -> 'a expr -> column_value

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
end

module Make : functor (C : SeekwhelConnection.S) -> S
