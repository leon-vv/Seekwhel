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

open CalendarLib

let conn = try new Postgresql.connection
    ~host:"127.0.0.1"
    ~dbname:"seekwhel_test"
    ~user:"seekwhel_test"
    ~password:"seekwhel_test" ()
    with Postgresql.Error e -> failwith (Postgresql.string_of_error e)

module Seekwhel_test : Seekwhel.Connection = struct
    let conn = conn
end

module S = Seekwhel.Make(Seekwhel_test)
open S
open S.Select
module SI = SeekwhelInner
module SC = SeekwhelColumn

(* Begin helpful functions for testing *)

let rec test1 f = function
    | (input, output)::rest ->
	assert (f input = output) ;
	test1 f rest 
    | [] -> ()

let rec test2 f = function
    | (i1, i2, output)::rest ->
	assert (f i1 i2 = output) ;
	test2 f rest
    | [] -> ()

let rec expect_exception f = function
    | input::is ->
	let ex = (try f input; false
	    with _ -> true)
	in if ex then expect_exception f is
	else failwith "No exception thrown"
    | [] -> ()
    

(* End helpful functions for testing *)

let sub_between_test () =
    assert (SI.sub_between "" 0 0 = "") ;
    assert (SI.sub_between "a" 0 0 = "") ;
    assert (SI.sub_between "ab" 0 0 = "") ;
    assert (SI.sub_between "ab" 0 1 = "") ;
    assert (SI.sub_between "abc" 0 1 = "") ;
    assert (SI.sub_between "abc" 0 2 = "b") ;
    assert (SI.sub_between "abcd" 1 3 = "c") ;
    assert (SI.sub_between "abcdefg" 1 5 = "cde") ;
    assert (SI.sub_between "abc" 0 3 = "bc") ;
    assert (SI.sub_between "abc" (-1) 3 = "abc")
    

let rename_table_in_string_test () =
    let flip f = (fun x y -> f y x)
    in (expect_exception
	((flip SC.rename_table_in_string) "table")
	[""; "a"; "..."; "a.b.c.d"] ;
    test2 SC.rename_table_in_string [
	("a.b", "def", "def.b") ;
	("a.b.c", "d", "a.d.c") ;
	("..", "a", ".a.") ;
	(".", "a", "a.")
    ])
    
let rename_table_test () = assert (
    (SC.rename_table (SC.Columni "products.stock") "p1")
	= SC.Columni "p1.stock")


let split_string_around_dots_test () =
    test1 SI.split_string_around_dots
	[	("", [""]) ;
	    ("adf", ["adf"]) ;
	    ("#$%^", ["#$%^"]) ;
	    ("asé¶»«³", ["asé¶»«³"]) ;
	    (".", [""; ""]) ;
	    ("...", [""; ""; ""; ""]) ;
	    ("a.b..d", ["a"; "b"; ""; "d"]) ;
	    ("asdf adsf.ads ..", ["asdf adsf"; "ads "; ""; ""]) ;
	    (".a.b", [""; "a"; "b"]) ;
	    ("a.b.c", ["a"; "b"; "c"]) ] ;;


 
let quote_identifier_test () =
    expect_exception SC.safely_quote_identifier
	["asdf\""; "\"sfd"; "\""; "aasf\"asdfasdf";
	"\"\"a"; "a\"\""; "\"\"asdfad\"\""] ;
    test1 SC.safely_quote_identifier [
	("", "");
	("a", "\"a\""); (* "a" is a keyword *)
	("b", "b");
	("abc", "abc");
	("\"\"", "\"\"");
	("\"a\"", "\"a\"");
	("select", "\"select\"");
	("array_MAX_cardinality", "\"array_MAX_cardinality\"")
    ]
	
let quote_identifier_test () =
    expect_exception SC.safely_quote_identifier
	["asdf\""; "\"sfd"; "\""; "aasf\"asdfasdf";
	"\"\"a"; "a\"\""; "\"\"asdfad\"\""] ;
    test1 SC.safely_quote_identifier [
	("", "");
	("a", "\"a\""); (* "a" is a keyword *)
	("b", "b");
	("abc", "abc");
	("\"\"", "\"\"");
	("\"a\"", "\"a\"");
	("select", "\"select\"");
	("array_MAX_cardinality", "\"array_MAX_cardinality\"")
    ]
    
let string_of_order_by_list_test () =
    test1 (string_of_order_by ~indent:0)
	Select.([
	    ([], "") ;
	    ([(AnyExpr (Column (SC.Columni "stock")), ASC)], "\nORDER BY stock ASC") ;
	    ([(AnyExpr (Column (SC.Columni "stock")), ASC);
	    (AnyExpr (Addi ((Int 10), (Int 30))), Select.DESC)],
		"\nORDER BY stock ASC, (10 + 30) DESC")
	])

let safely_quote_column_test () =
    expect_exception SC.safely_quote_column
	["\""; "adc.d\"d"; "a\""] ;
    test1 SC.safely_quote_column
	[("kl.h.f", "kl.h.f");
	("..", "..");
	("\"a\".b", "\"a\".b");
	("a.d.\"k\"", "\"a\".d.\"k\"");
	("\"\"", "\"\"");
	("select.uncommitted..abc",
	    "\"select\".\"uncommitted\"..abc")]

module String_of_expr_test = struct

    (* Column *)
    let price_col = SC.Columnr "price"
    let stock_col = SC.Columni "stock"
    let name_col = SC.Columnt "name"
    let date_col = SC.Columnd "date"

    let date = Calendar.lmake ~year:2016 ~month:10 ~day:10 ()

    (* Values *)
    let i = Int 10
    let f = Real 20.3
    let t = Text "This is' som'e text"
    let d = Date date

    let n = Null

    (* Nullable values *)
    let int_null = IntNull 100
    let float_null = RealNull 200.0
    let text_null = TextNull "Some other text"
    let date_null = DateNull date

    (* Functions *)
    let coalesce = Coalesce (int_null, i)
    let random = Random
    let sqrti = Sqrti i
    let sqrtf = Sqrtr f
    let addi = Addi (i, i)
    let addf = Addr (f, f)

    (* Boolean *)
    let is_null = IsNull int_null
    let eq = Eq (Column price_col, Real 10.0)
    let gt = GT (Column stock_col, Int 5)
    let lt = LT (Text "def", Column name_col)
    let not_ = Not eq
    let and_ = And (eq, gt)
    let or_ = Or (is_null, not_)

    (* Conditionals *)
    let case_single = Case (lt, f, sqrtf)
    let case_double = Case (eq, f, case_single)


    let greatest_double = Greatestr [f; sqrtf]
    let greatest_triple = Greatesti [i; addi; i]
    let least_double = Leastr [f; sqrtf]
    let least_triple = Leasti [i; addi; i]

    (* Some complicated expressions *)
    let root_coalesce = Sqrti (Coalesce (int_null, i))
    let logical = And (and_, And (eq, Or (is_null, or_)))

    let assert_expr_to_string : type a. a expr -> string -> unit
	= fun x s -> assert (string_of_expr x = s)

    let ( >>|| ) = assert_expr_to_string 

    let string_of_expr_test () =
	i >>|| "10" ;
	f >>|| "20.3" ;
	t >>|| "'This is'' som''e text'" ;
	d >>|| "'2016-10-10 00:00:00'" ;
	n >>|| "NULL" ;
	int_null >>|| "100" ;
	float_null >>|| "200." ;
	text_null >>|| "'Some other text'" ;
	date_null >>|| "'2016-10-10 00:00:00'" ;
	coalesce >>|| "COALESCE(100, 10)" ;
	random >>|| "RANDOM()" ;
	sqrti >>|| "|/ 10" ;
	sqrtf >>|| "|/ 20.3" ;
	addi >>|| "10 + 10" ;
	addf >>|| "20.3 + 20.3" ;
	is_null >>|| "100 IS NULL" ;
	eq >>|| "price = 10." ;
	gt >>|| "stock > 5" ;
	lt >>|| "'def' < \"name\"" ;
	not_ >>|| "NOT (price = 10.)" ;
	and_ >>|| "(price = 10.)\n\tAND (stock > 5)" ;
	or_ >>|| "(100 IS NULL)\n\tOR (NOT (price = 10.))" ;
	case_single >>|| "CASE WHEN ('def' < \"name\") THEN 20.3 ELSE (|/ 20.3) END" ;
	case_double >>|| "CASE WHEN (price = 10.) THEN 20.3 WHEN ('def' < \"name\") THEN 20.3 ELSE (|/ 20.3) END" ;
	greatest_double >>|| "GREATEST(20.3, (|/ 20.3))" ;
	greatest_triple >>|| "GREATEST(10, (10 + 10), 10)";
	least_double >>|| "LEAST(20.3, (|/ 20.3))" ;
	least_triple >>|| "LEAST(10, (10 + 10), 10)";

	root_coalesce >>|| "|/ COALESCE(100, 10)" ;
	logical >>||
	    "(price = 10.)\n\tAND (stock > 5)\n\tAND (price = 10.)\n\tAND ((100 IS NULL)\n\tOR (100 IS NULL)\n\tOR (NOT (price = 10.)))" ;;
	     
end


let pure_tests =
    [
	("sub", sub_between_test);
	("split", split_string_around_dots_test);
	("rename_string", rename_table_in_string_test);
	("rename_table", rename_table_test);
	("quote_ident", quote_identifier_test);
	("safely_quote_column", safely_quote_column_test);
	("string_of_expr", String_of_expr_test.string_of_expr_test);
	("string_of_order_by_list", string_of_order_by_list_test)
    ] ;;

let run_pure_tests () =
	print_endline "Starting pure tests...";
	List.fold_left
		(fun idx (name, f) ->
			print_endline ((string_of_int idx) ^ " " ^ name);
			f () ;
			idx + 1)
		1
		pure_tests ;;

(*
    The following tests actually use the database.
    To run these tests you'll need need a database with
    the connecion configuration as seen above (or change
    them appropriately).

    Seekwhel expects two tables, namely 'Person' and 'Post'.

    Table 'Person' has the following structure:
	create table Person (
	    name text primary key,
	    parent text null references Person(name),
	);

	create table Post (
	    id serial primary key,
	    person_name text references person(name),
	    date timestamp not null,
	    content text not null
	);

    The following actions will be performed:
    - Delete all rows to clean the results from the 
    previous tests (if they didn't finish correctly).
    - Insert some persons and some posts.
    - Some selections (including joins).
    - Update some persons and some posts.
*)


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
    
    let name_col = SC.Columnt name_s
    let parent_col = SC.ColumntNull parent_s

    let primary_key = [| name_s |]

    let default_columns = [||]

    let column_mappings = [|
		SC.AnyMapping (
			name_col,
			(fun p n -> {p with name = n }),
			(fun p -> p.name)) ;
		SC.AnyMapping (
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

    let person_name_col = SC.Columnt person_name_s
    let date_col = SC.Columnd date_s
    let content_col = SC.Columnt content_s
    let id_col = SC.Columni id_s

    let primary_key = [| id_s |]

    let default_columns = [| id_s |]

    let column_mappings = [|
		SC.AnyMapping (
			person_name_col,
			(fun p n -> {p with person_name = n}),
			(fun p -> p.person_name)) ;
		SC.AnyMapping (
			date_col,
			(fun p d -> {p with date = d}),
			(fun p -> p.date)) ;
		SC.AnyMapping (
			content_col,
			(fun p c -> {p with content = c}),
			(fun p -> p.content)) ;
		SC.AnyMapping (
			id_col,
			(fun p i -> {p with id = i}),
			(fun p -> p.id ))
    |]

end


module QPerson = Queryable(Person)
module QPost = Queryable(Post)


let string_of_test_file f =
    let path = "maintenance/tests/" ^ f ^ ".sql"
    in let ch = open_in path
    in let l = in_channel_length ch
    in really_input_string ch l


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


let run_database_tests () =
	print_endline "Starting database tests..." ;
    (* Clean previous database operations *)
    QPerson.delete_q
	|> Delete.exec ;
    QPost.delete_q
	|> Delete.exec ;
 
    (* Do some tests of the high-level Queryable operations *)
    let catarina = Person.{
		name = "Catarina" ;
		parent = None
    }
    and jhonny = Person.{
		name = "Jhonny" ;
		parent = Some "Jhonny"
    }
    in
	QPerson.insert [| catarina ; jhonny |] ;

	QPerson.update [| { catarina with Person.parent = Some "Jhonny" } |] ;

	let jhonny_option = QPerson.select_unique (Eq (Column Person.name_col, Text "Jhonny"))
		in (match jhonny_option with
			| None -> failwith "Jhonny not found"
			| Some j -> assert (j = jhonny)) ;

	(* Now we'll produce some complicated queries and
	check if they result in the same queries as the files
	in /tests/* contain. *)
	let file_with_query = [
	    ("select1", `Select select_query1 ) ;
	    ("select2", `Select select_query2 )
	] in List.iter (fun (f, q) ->
	    let query = string_of_test_file f
	    in let s = match q with
		| `Select s -> Select.to_string s
	    in
		if query = s
		then ignore (conn#exec s) (* Execute to make sure the syntax is valid. *)
		else (
		    print_endline ("\n\nTest " ^ f ^ " failed.\n") ;
		    print_endline ("Query produced: \n\n" ^ s) ;
		    print_endline ("\nQuery expected: \n\n" ^ query)))
	    file_with_query ;

	Seekwhel_test.conn#finish 
    

(* Timing functions, useful for benchmarking *)

let time_taken f =
	let start = Sys.time ()
	in (f () ; Sys.time () -. start)

let benchmark () =
	print_endline "Starting benchmark..." ;
	let s1_time = ref 0.0
	and s2_time = ref 0.0
	in let rec aux n =
		if n == 100000 then ()
		else (
			s1_time :=  !s1_time +.
				time_taken (fun () -> Select.to_string select_query1) ;
			s2_time := !s2_time +.
				time_taken (fun () -> Select.to_string select_query2) ;
			aux (n + 1))
	in (aux 0 ;
	print_endline ("Time taken for select_query1: " ^ string_of_float !s1_time ^ " s") ;
	print_endline ("Time taken for select_query2: " ^ string_of_float !s2_time ^ " s")) 

	
let _ = match Sys.argv with
    | [|_ ; "pure" |] -> ignore (run_pure_tests ())
    | [|_ ; "database" |] -> ignore (run_database_tests ())
    | [|_ ; "benchmark" |] -> ignore (benchmark ())
    | _ -> print_endline "Please supply a command (either 'pure' or 'database')"

    
