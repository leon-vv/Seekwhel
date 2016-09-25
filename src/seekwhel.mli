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

module Make : functor (C : Connection)
    -> sig

    exception Seekwhel_error of string	

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
    

    val ( <|| ): 'a column -> string -> 'a column 

    module type Query = sig
	type t
	type result
	
	val to_string : t -> string
	val exec : t -> result
    end

    module Select : sig
	type 'a custom_expr = {
	    value : 'a ;
	    to_psql_string : 'a -> string ;
	    of_psql_string : string -> 'a
	}

	type order_dir =
		    | ASC
		    | DESC

	include Query

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

	    (* Aggregate functions *)
	    | Avgi : int expr -> float option expr
	    | Avgr : float expr -> float option expr
	    | BoolAnd : bool expr -> bool option expr
	    | BoolOr : bool expr -> bool option expr
	    | Count : int expr
	    | CountExpr : 'a expr -> int expr

	    | MaxAggi : int expr -> int option expr
	    | MaxAggr : float expr -> float option expr
	    | MaxAggd : Calendar.t expr -> Calendar.t option expr
	    | MaxAggt : string expr -> string option expr

	    | MinAggi : int expr -> int option expr
	    | MinAggr : float expr -> float option expr
	    | MinAggd : Calendar.t expr -> Calendar.t option expr
	    | MinAggt : string expr -> string option expr

	    | StringAgg : string expr * string -> string option expr

	    | Sumi : int expr -> int option expr
	    | Sumr : float expr -> float option expr


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
	and target = any_expr array
	

	val string_of_expr : 'a expr -> string

	val expr_of_value : 'a -> 'a column -> 'a expr
	    
	type 'a expr_or_default =
	    | Expr of 'a expr
	    | Default

	type column_eq =
	    | ColumnEq : 'a column * 'a expr -> column_eq

	type column_eq_default =
	    | ColumnEqDefault : 'a column * 'a expr_or_default -> column_eq_default


	val where : bool expr-> t -> t
	val limit : int -> t -> t
	val offset : int -> t -> t

	val order_by : 'a expr -> order_dir -> t -> t

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
    end

    module Insert : sig
	include Query
	    with type result = unit

	type target = Select.column_eq_default array 

	val q : table:string -> target -> t
    end


    module Update : sig
	include Query with type result = unit

	type target = Select.column_eq_default array
	val q : table:string -> target -> t

	val where : bool Select.expr -> t -> t
    end

    module Delete : sig
	type t

	val q : table:string -> t
	val where : bool Select.expr -> t -> t

	val to_string : t -> string
	val exec : t -> unit
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

    module Queryable (T : Table) : sig
	val select_q : Select.target -> Select.t
	val insert_q : Insert.target -> Insert.t
	val update_q : Update.target -> Update.t
	val delete_q : Delete.t

	val select : bool Select.expr -> T.t array 
	val insert : T.t array -> unit
	val update : T.t array -> unit
	val delete : T.t array -> unit

	(* Non-essential helper stuff *)

	val select_first : bool Select.expr -> T.t option

	(* Will throw an error when more rows are returned *)
	val select_unique : bool Select.expr -> T.t option
    end

    module Join2(T1 : Table)(T2 : Table) : sig
    
	type ('a, 'b) join_result =
	    | Left of 'a
	    | Right of 'b
	    | Both of ('a * 'b)
	
	val join :
	    Select.join_direction
	    -> on:bool Select.expr
	    -> bool Select.expr
	    -> ((T1.t, T2.t) join_result) array
	
	val inner_join :
	    on:bool Select.expr
	    -> bool Select.expr
	    -> (T1.t * T2.t) array
    end
end

