#require "postgresql";;
#require "calendar";;
#mod_use "src/keywords.ml";;
#mod_use "src/seekwhel.ml";;

open CalendarLib

module Seekwhel_test : Seekwhel.Connection = struct
    let connection = try new Postgresql.connection
	~host:"127.0.0.1"
	~dbname:"seekwhel_test"
	~user:"seekwhel_test"
	~password:"seekwhel_test" ()
    with Postgresql.Error e -> failwith (Postgresql.string_of_error e)
end

module S = Seekwhel.Make(Seekwhel_test)
open S
open S.Select

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
    assert (sub_between "" 0 0 = "") ;
    assert (sub_between "a" 0 0 = "") ;
    assert (sub_between "ab" 0 0 = "") ;
    assert (sub_between "ab" 0 1 = "") ;
    assert (sub_between "abc" 0 1 = "") ;
    assert (sub_between "abc" 0 2 = "b") ;
    assert (sub_between "abcd" 1 3 = "c") ;
    assert (sub_between "abcdefg" 1 5 = "cde") ;
    assert (sub_between "abc" 0 3 = "bc") ;
    assert (sub_between "abc" (-1) 3 = "abc")
    

let rename_table_in_string_test () =
    let flip f = (fun x y -> f y x)
    in (expect_exception
	((flip rename_table_in_string) "table")
	[""; "a"; "..."; "a.b.c.d"] ;
    test2 rename_table_in_string [
	("a.b", "def", "def.b") ;
	("a.b.c", "d", "a.d.c") ;
	("..", "a", ".a.") ;
	(".", "a", "a.")
    ])
    
let rename_table_test () = assert (
    (Columni "products.stock" <|| "p1")
	= Columni "p1.stock")


let split_string_around_dots_test () =
    test1 split_string_around_dots
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
    expect_exception safely_quote_identifier
	["asdf\""; "\"sfd"; "\""; "aasf\"asdfasdf";
	"\"\"a"; "a\"\""; "\"\"asdfad\"\""] ;
    test1 safely_quote_identifier [
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
    expect_exception safely_quote_identifier
	["asdf\""; "\"sfd"; "\""; "aasf\"asdfasdf";
	"\"\"a"; "a\"\""; "\"\"asdfad\"\""] ;
    test1 safely_quote_identifier [
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
    test1 string_of_order_by
	Select.([
	    ([], "") ;
	    ([(AnyExpr (Column (Columni "stock")), ASC)], "ORDER BY (stock) ASC") ;
	    ([(AnyExpr (Column (Columni "stock")), ASC);
	    (AnyExpr (Addi ((Int 10), (Int 30))), Select.DESC)],
		"ORDER BY (stock) ASC, (10 + 30) DESC")
	])

let safely_quote_column_test () =
    expect_exception safely_quote_column
	["\""; "adc.d\"d"; "a\""] ;
    test1 safely_quote_column
	[("kl.h.f", "kl.h.f");
	("..", "..");
	("\"a\".b", "\"a\".b");
	("a.d.\"k\"", "\"a\".d.\"k\"");
	("\"\"", "\"\"");
	("select.uncommitted..abc",
	    "\"select\".\"uncommitted\"..abc")]

module String_of_expr_test = struct

    (* Column *)
    let price_col = Columnr "price"
    let stock_col = Columni "stock"
    let name_col = Columnt "name"
    let date_col = Columnd "date"

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
	and_ >>|| "(price = 10.) AND (stock > 5)" ;
	or_ >>|| "(100 IS NULL) OR (NOT (price = 10.))" ;
	case_single >>|| "CASE WHEN ('def' < \"name\") THEN 20.3 ELSE (|/ 20.3) END" ;
	case_double >>|| "CASE WHEN (price = 10.) THEN 20.3 WHEN ('def' < \"name\") THEN 20.3 ELSE (|/ 20.3) END" ;
	greatest_double >>|| "GREATEST(20.3, (|/ 20.3))" ;
	greatest_triple >>|| "GREATEST(10, (10 + 10), 10)";
	least_double >>|| "LEAST(20.3, (|/ 20.3))" ;
	least_triple >>|| "LEAST(10, (10 + 10), 10)";

	root_coalesce >>|| "|/ (COALESCE(100, 10))" ;
	logical >>||
	    "(price = 10.) AND (stock > 5) AND (price = 10.) AND ((100 IS NULL) OR (100 IS NULL) OR (NOT (price = 10.)))" ;;
	     
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

let run_pure_tests () = List.fold_left
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

    let person_name_s = "person_name_s"
    let date_s = "date_s"
    let content_s = "content_s"
    let id_s = "id_s"

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


module QPerson = Queryable(Person)
module QPost = Queryable(Post)

let run_integration_tests () =
    QPerson.delete_q
	|> Delete.exec ;
    QPost.delete_q
	|> Delete.exec ;
 
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

	let jhonny_option = QPerson.select_unique
	    (Eq (Column Person.name_col, Text "Jhonny")) in (match jhonny_option with
	    | None -> failwith "Jhonny not found"
	    | Some j -> assert (j = jhonny)) ;
	
	Seekwhel_test.connection#finish

let _ = match Sys.argv with
    | [|_ ; "pure" |] -> ignore (run_pure_tests ())
    | [|_ ; "database" |] -> ignore (run_integration_tests ())
    | _ -> print_endline "Please supply a command (either 'pure' or 'database')"

    
