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

module Make (C : module type of SeekwhelConnection) = struct

    module Column = SeekwhelColumn
    open Column

    module Insert = SeekwhelInsert.Make(C)
    module Select = SeekwhelSelect.Make(C)
    module Update = SeekwhelUpdate.Make(C)
    module Delete = SeekwhelDelete.Make(C)

    module SI = SeekwhelInner

    module type Table = sig
	val name : string 
	type t
    
	val empty : t
	val primary_key : string array

	val default_columns : string array

	val column_mappings : t Column.any_column_mapping array
    end

    module Queryable (T : Table) = struct
	
	let select_q = Select.q ~from:T.name
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
		(fun (AnyMapping (cl, _, get)) ->
		    let val_ = get t
		    in Select.(if Array.mem (quoted_string_of_column cl) T.default_columns
			then ColumnEqDefault (cl, Default)
			else ColumnEqDefault (cl, Expr (expr_of_value val_ cl))))
		T.column_mappings ;;

	let insert ts =
	    Array.iter (fun t ->
	    	insert_q (column_value_array_of_t t)
			|> Insert.exec)
		ts
	
	let equal_expr cl v =
	    Select.(Eq (Column cl, expr_of_value v cl))


	let primary_mappings = 
	    let lst = match Array.length T.primary_key with
		| 0 -> SI.seekwhel_fail ("primary key array cannot be empty
		    (table name: " ^ T.name ^ ")")
		| _ -> let lst = Array.to_list T.column_mappings
		    in List.filter (fun (AnyMapping (col, _, _)) ->
			Array.mem (string_of_column col) T.primary_key) lst
	    in if List.length lst > 0 then lst
		else SI.seekwhel_fail ("a column mapping of the primary keys
		    could not be found (table name: " ^ T.name ^ ")")

	let expr_of_expr_list = function
	    | x1::x2::rest ->
		Some (List.fold_left
			(fun and_x new_x -> Select.And (and_x, new_x))
			(Select.And (x1, x2))
			rest)
	    | [x1] -> Some x1
	    | _ -> None (* Can't create an expression from an empty list *)


	let where_of_primary t =
	    let equals = List.map (fun (AnyMapping (col, _, get)) ->
		equal_expr col (get t)) primary_mappings
	    in let expr = expr_of_expr_list equals
	    in match expr with
		| Some x -> x
		| None -> SI.seekwhel_fail "trying to build where clause of primary keys; but 
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
 
	let _ =
	    assert (Array.length T1.primary_key > 0) ;
	    assert (Array.length T2.primary_key > 0) 

	type ('a, 'b) join_result =
	    | Left of 'a
	    | Right of 'b
	    | Both of ('a * 'b)

	let expr_of_col col = Select.Column col


	let get_first_primary_column = fun (module T : Table) -> 
	    assert (Array.length T.primary_key > 0) ;
	    let first_p = Array.get T.primary_key 0
	    in let idx_opt = SI.get_index_where (fun (AnyMapping (c, _, _)) ->
		string_of_column c = first_p) T.column_mappings
	    in match idx_opt with
		| Some idx -> 
		    let (AnyMapping (c, _, _)) = Array.get T.column_mappings idx
		    in Select.(AnyExpr (Column c))
		| None -> SI.seekwhel_fail
		    ("primary key must be part of the column mappings; table name: " ^ T.name)
	
	let join dir ~on expr =
	    let columns = Array.append Q1.columns Q2.columns
	    in let result = Select.q ~from:T1.name columns
		|> Select.where expr
		|> Select.join T2.name dir ~on
		|> Select.exec

	    and gfpc = get_first_primary_column
	    in match (gfpc (module T1), gfpc (module T2)) with
		| (Select.AnyExpr x1, Select.AnyExpr x2) ->
		    (Select.get_all (fun cb ->
			match Select.(cb.is_null (x1), cb.is_null x2) with
			    | (false, false) -> Both (Q1.t_of_callback cb, Q2.t_of_callback cb)
			    | (true, false) -> Right (Q2.t_of_callback cb)
			    | (false, true) -> Left (Q1.t_of_callback cb)
			    | (true, true) ->
				SI.seekwhel_fail "'on' columns in join query both returned NULL")
			result)

	let inner_join ~on expr =
	    let rows = join Select.Inner ~on expr
	    in
		Array.map (function
		    | Both res -> res
		    | _ -> SI.seekwhel_fail "'on' columns in INNER join cannot be NULL; unexpected result")
		    rows
	
    end
end

