open Drovelib

let with_connection saddr f =
  let ic, oc = Unix.(open_connection (ADDR_UNIX saddr)) in
  with_finalize
    ~f_final:(fun () -> close_in ic)
    ~f:(fun () -> f ic oc)

let show ic oc =
  output_char oc '\000';
  flush oc;
  let (db : int64 Int64.Map.t) = input_value ic in
  if Int64.Map.is_empty db then
    Printf.eprintf "<ledger empty>\n"
  else
    Int64.Map.iter (fun id qty ->
        Printf.printf "%Ld: %Ld\n%!" id qty
      ) db

let atomic ~op ~id ~qty ic oc =
  let buf = Bytes.create 16 in
  Entry.write' ~id ~qty ~tr:`Atomic ~op buf 0;
  output_char oc '\001';
  output oc buf 0 16;
  flush oc;
  prerr_endline (input_line ic)

let transfer ~id ~id' ~qty ic oc =
  let buf = Bytes.create 32 in
  Entry.write' ~id ~qty ~tr:`Begin ~op:`Withdraw buf 0;
  Entry.write' ~id:id' ~qty ~tr:`End ~op:`Deposit buf 16;
  output_char oc '\002';
  output oc buf 0 32;
  flush oc;
  prerr_endline (input_line ic)

let () =
  let op = ref None in
  let anon_args = ref [] in
  let speclist = Arg.[
      "-s", Unit (fun () -> op := Some `Show), " Show ledger";
      "-d", Unit (fun () -> op := Some `Deposit),
      "<id> <qty> Deposit cash into an account";
      "-w", Unit (fun () -> op := Some `Withdraw),
      "<id> <qty> Withdraw cash from an account";
      "-t", Unit (fun () -> op := Some `Transfer),
      "<id> <id'> <qty> Transfer cash from an account to another";
  ] in
  let usage_msg =
    Printf.sprintf "Usage: %s <options>\nOptions are:" Sys.argv.(0) in
  Arg.parse speclist (fun a -> anon_args := Int64.(of_string a)::!anon_args) usage_msg;
  let tmp = Sys.readdir "/tmp" in
  let drovebanks =
    Array.fold_left
      (fun a v ->
         if String.length v > 10 && String.sub v 0 10 = "drovebank-"
         then v::a else a
      ) [] tmp in
  let drovebank = match drovebanks with
    | [] ->
        prerr_endline "Impossible to locate any running DroveBank daemon, exiting.";
        exit 1
    | h::_ -> "/tmp/" ^ h in
  try
    match !op with
    | None -> Arg.usage speclist usage_msg
    | Some `Show ->
        with_connection drovebank show
    | Some `Deposit ->
        (match !anon_args with
         | [qty; id] -> with_connection drovebank (atomic ~op:`Deposit ~id ~qty)
         | _ -> raise Exit
        )
    | Some `Withdraw ->
        (match !anon_args with
         | [qty; id] -> with_connection drovebank (atomic ~op:`Withdraw ~id ~qty)
         | _ -> raise Exit
        )
    | Some `Transfer ->
        (match !anon_args with
         | [qty; id'; id] -> with_connection drovebank (transfer ~id ~id' ~qty)
         | _ -> raise Exit
        )
  with exn ->
    prerr_endline Printexc.(to_string exn)
