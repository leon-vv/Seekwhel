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
