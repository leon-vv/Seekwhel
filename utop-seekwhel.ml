#require "postgresql";;
#require "calendar";;

#mod_use "src/seekwhelInner.ml";;
#mod_use "src/seekwhelConnection.ml";;
#mod_use "src/seekwhelInner.ml";;
#mod_use "src/seekwhelKeywords.ml";;
#mod_use "src/seekwhelColumn.ml";;

#mod_use "src/seekwhelSelect.ml";;
#mod_use "src/seekwhelUpdate.ml";;
#mod_use "src/seekwhelDelete.ml";;
#mod_use "src/seekwhelInsert.ml";;

#mod_use "src/seekwhel.ml";;
#mod_use "maintenance/queries.ml"
#mod_use "maintenance/tests.ml"

open CalendarLib

open Queries
open S
open S.Select

module SI = SeekwhelInner
module SC = SeekwhelColumn

module TS = Tests.String_of_expr_test;;

module Seekwhel_test : Seekwhel.Connection = struct
    let conn = new Postgresql.connection
	~host:"127.0.0.1"
	~dbname:"seekwhel_test"
	~user:"seekwhel_test"
	~password:"seekwhel_test" ()
end

let pp_of_to_string f =
    (fun formatter input -> Format.pp_print_text formatter (f input)) ;;

let pp_select = pp_of_to_string (S.Select.to_string) ;;
let pp_update = pp_of_to_string (S.Update.to_string) ;;
let pp_insert = pp_of_to_string (S.Insert.to_string) ;;
let pp_delete = pp_of_to_string (S.Delete.to_string) ;;

#install_printer pp_select ;;
#install_printer pp_update ;;
#install_printer pp_insert ;;
#install_printer pp_delete ;;
