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


    type 'a column =
	| Columni : string -> int column
	| Columnf : string -> float column
	| Columnt : string -> string column
	| Columnd : string -> Calendar.t column

    val sub_between_test : unit -> unit
    val split_string_around_dots_test : unit -> unit
	
    val ( <|| ): 'a column -> string -> 'a column 

    val rename_table_in_string_test : unit -> unit
    val rename_table_test : unit -> unit
    
    val quote_identifier_test : unit -> unit
    val safely_quote_string_test : unit -> unit

    type any_column =
	| AnyColumn : 'a column -> any_column

    type 'a tagged_value = 'a column * 'a

    type any_tagged_value =
	| AnyTaggedValue : 'a tagged_value -> any_tagged_value

    type 'a slot =
	| Column : 'a column -> 'a slot
	| Int : int -> int slot
	| Float : float -> float slot
	| Text : string -> string slot

    type bool_expr =
	| Eq : 'a slot * 'a slot -> bool_expr
	| Gt : 'a slot * 'a slot -> bool_expr
	| Lt : 'a slot * 'a slot -> bool_expr
	| Not : bool_expr -> bool_expr
	| And : bool_expr list -> bool_expr
	| Or : bool_expr list -> bool_expr

    val string_of_expr : bool_expr -> string ;;

    module type Query = sig
	type t
	type target
	type result
	
	val q : table:string -> target -> t
	val to_string : t -> string
	val exec : t -> result
    end

    module Insert : Query
	with type target = any_tagged_value array 
	and type result = unit

    module Select : sig
	include Query
	    with type target = any_column array

	val where : bool_expr -> t -> t

	type join_direction =
	    | Left | Inner | Right

	val join : string
	    -> join_direction
	    -> on:('a column * 'a column)
	    -> t
	    -> t

	type 'a column_callback = {
		get_value : 'a. ('a column -> 'a) ;
		is_null : 'a. ('a column -> bool)
	}
	type ('a, 'b) row_callback = 'a column_callback -> 'b

	val get_all : ('a, 'b) row_callback
	    -> result
	    -> 'b array
    end

    module Update : sig
	type t
	type target = any_tagged_value array

	val q : table:string -> target -> bool_expr -> t
	val where : bool_expr -> t -> t
	val to_string : t -> string

	val exec : t -> unit
    end

    module Delete : sig
	type t
	val q : table:string -> bool_expr -> t
	val where : bool_expr -> t -> t
	val to_string : t -> string
	val exec : t -> unit
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

    module Queryable (T : Table) : sig
	val select : bool_expr -> T.t array 
	val insert : T.t array -> unit
    end

    module Join2(T1 : Table)(T2 : Table) : sig
    
	type ('a, 'b) join_result =
	    | Left of 'a
	    | Right of 'b
	    | Both of ('a * 'b)
	
	val join :
	    Select.join_direction
	    -> on:('a column * 'a column)
	    -> bool_expr
	    -> ((T1.t, T2.t) join_result) array
	
	val inner_join :
	    on:('a column * 'a column)
	    -> bool_expr
	    -> (T1.t * T2.t) array
    end
end

