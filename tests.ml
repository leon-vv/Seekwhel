open Postgresql
open Seekwhel

module Seekwhel_test : Connection = struct
    let connection = new Postgresql.connection
	~host:"127.0.0.1"
	~dbname:"seekwhel_test"
	~user:"seekwhel_test"
	~password:"seekwhel_test" ()
end

module S = Seekwhel.Make(Seekwhel_test)
open S

let tests =
    [
	("sub", sub_between_test);
	("split", split_string_around_dots_test);
	("rename_string", rename_table_in_string_test);
	("rename_table", rename_table_test);
	("quote_ident", quote_identifier_test);
	("safely_quote", safely_quote_string_test)
    ] ;;

let _ = List.fold_left
	    (fun idx (name, f) ->
		print_endline ((string_of_int idx) ^ " " ^ name);
		f () ;
		idx + 1)
	    1
	    tests ;;

    
