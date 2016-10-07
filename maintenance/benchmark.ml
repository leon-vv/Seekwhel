open Queries
open S

(* Timing functions, useful for benchmarking *)
let time_taken f =
	let start = Sys.time ()
	in (f () ; Sys.time () -. start)

let _ =
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


