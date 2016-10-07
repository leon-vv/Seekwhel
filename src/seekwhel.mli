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

module type Connection = SeekwhelConnection.S

module Make : functor (C : Connection) -> sig

	module Column = SeekwhelColumn
    module Select : module type of SeekwhelSelect.Make(C)
    module Update : module type of SeekwhelUpdate.Make(C)
    module Insert : module type of SeekwhelInsert.Make(C)
    module Delete : module type of SeekwhelDelete.Make(C)

    module type Table = sig
		val name : string 
		type t
		
		val empty : t
		val primary_key : string array

		val default_columns : string array

		val column_mappings : t Column.any_column_mapping array
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

