#use "topfind";;
#require "postgresql";;
#require "calendar";;
#mod_use "src/seekwhel.ml";;

module Seekwhel_test : Seekwhel.Connection = struct
    let connection = new Postgresql.connection
	~host:"127.0.0.1"
	~dbname:"seekwhel_test"
	~user:"seekwhel_test"
	~password:"seekwhel_test" ()
end

module S = Seekwhel.Make(Seekwhel_test)
open S

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
