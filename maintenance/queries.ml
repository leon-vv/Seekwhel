open CalendarLib

let conn = try new Postgresql.connection
    ~host:"127.0.0.1"
    ~dbname:"seekwhel_test"
    ~user:"seekwhel_test"
    ~password:"seekwhel_test" ()
    with Postgresql.Error e -> (
		print_endline "Postgresql failed to conenct to database seekwhel_test" ; 
		print_endline "These tests depend on a database: \n
			host: 127.0.0.1 (localhost) \n
			database name: seekwhel_test \n
			username: seekwhel_test \n
			password: seekwhel_test \n\n
see the end of ./maintenance/tests.ml for information about which tables need to be created\n\n
the error returned by Postgresql is as follows:\n\n" ;
		failwith (Postgresql.string_of_error e))


module Seekwhel_test : Seekwhel.Connection = struct
    let conn = conn
end

module S = Seekwhel.Make(Seekwhel_test)
open S
open S.Select
open S.Select.Infix
open S.Column

module Person = struct
    let name = "person"

    type t = {
		name: string ;
		parent : string option
    }
    
    let empty = {
		name = "" ;
		parent = None
    }

    let name_s = "name"
    let parent_s = "parent"
    
    let name_col = Columnt name_s
    let parent_col = ColumntNull parent_s

    let primary_key = [| name_s |]

    let default_columns = [||]

    let column_mappings = [|
		AnyMapping (
			name_col,
			(fun p n -> {p with name = n }),
			(fun p -> p.name)) ;
		AnyMapping (
			parent_col,
			(fun p n -> {p with parent = n }),
			(fun p -> p.parent) )
    |]
end

module Post = struct
    let name = "post"

    type t = {
    	person_name : string ;
    	date : Calendar.t ;
    	content : string ;
    	id : int
    }

    let empty = {
		person_name = "" ;
		date = Calendar.now () ;
		content = "" ;
		id = 0
    }

    let person_name_s = "person_name"
    let date_s = "date"
    let content_s = "content"
    let id_s = "id"

    let person_name_col = Columnt person_name_s
    let date_col = Columnd date_s
    let content_col = Columnt content_s
    let id_col = Columni id_s

    let primary_key = [| id_s |]

    let default_columns = [| id_s |]

    let column_mappings = [|
		AnyMapping (
			person_name_col,
			(fun p n -> {p with person_name = n}),
			(fun p -> p.person_name)) ;
		AnyMapping (
			date_col,
			(fun p d -> {p with date = d}),
			(fun p -> p.date)) ;
		AnyMapping (
			content_col,
			(fun p c -> {p with content = c}),
			(fun p -> p.content)) ;
		AnyMapping (
			id_col,
			(fun p i -> {p with id = i}),
			(fun p -> p.id ))
    |]

end


module QPerson = S.Queryable(Person)
module QPost = S.Queryable(Post)

let select_query1 = QPerson.select_q (any (col Person.name_col))
    |> where (AnyGT (CharLength (
			Coalesce(
			    Column Person.parent_col,
			    Text "")),
		    (QPost.select_q (any (col Post.id_col)))))

let select_query2 =
    Person.(
	(any_col name_col) ||| parent_col
	    |> QPerson.select_q
	    |>
		(let p = Coalesce(col parent_col, Text "")
		and n = col name_col
		in let l = CharLength(Concat(p, n))
		in
		    where
			((l >|| (Int 15))
			&&|| (Addi(l, CharLength (col Post.content_col))
			    =|| (Int 33))))
	    |> join Post.name FullOuter 
			~on:(col Person.name_col =|| col Post.person_name_col))

let update_query1 =
	Person.(
		let p = Coalesce (col parent_col, Text "")
		in QPerson.update_q
			[|
				name_col ==|| Concat (Text "child of ", p) ;
				default parent_col
			|]
		|> Update.where (CharLength (col name_col) >|| (Int 15)))
			
let insert_query1 =
	Person.(
		QPerson.insert_q
			(Insert.ColumnValue [|
				name_col ==|| Text "This is the name" ;
				parent_col =?|| Concat (Text "This is the ", Text "name of the parent")
			|]))

let delete_query1 =
	Person.(
		let p = Coalesce (col parent_col, Text "")
		in QPerson.delete_q
		|> Delete.where (CharLength (Column name_col) >|| CharLength p))
			
	

