open Drovelib


let is_acceptable prev cur = match prev, cur with
   | `Atomic, `Atomic -> true
   | `Atomic, `Begin -> true
   | `Atomic, #Entry.transaction -> false
   | `Begin, `Cont -> true
   | `Begin, `End -> true
   | `Begin, #Entry.transaction -> false
   | `End, `Atomic -> true
   | `End, `Begin -> true
   | `End, #Entry.transaction -> false
   | `Cont, `End -> true
   | `Cont, `Cont -> true
   | `Cont, #Entry.transaction -> false

let load_from_disk ic =
  let db = ref Int64.Map.empty in
  let prev_tr = ref `Atomic in
  try
    while true do
      let open Entry in
      let entry = input ic in
      if is_acceptable !prev_tr entry.tr then  db := Entry.process !db entry
      else raise Exit;
      prev_tr := entry.tr
    done;
    assert false
  with
  | End_of_file -> Some db
  | exn -> None

let srv_fun db_oc db client_ic client_oc =
  let buf = Bytes.create 1024 in
  while true do
    let nb_read = input client_ic buf 0 1 in
    if nb_read <> 1 then raise End_of_file;
    match Char.code Bytes.(get buf 0) with
    | 0 ->
        output_value client_oc db
    | n ->
        let open Entry in
        let prev_tr = ref `Atomic in
        for i = 0 to n-1 do
          let entry = input client_ic in
          if is_acceptable !prev_tr entry.tr
          then output db_oc entry
          else raise Exit
        done
  done

let () =
  let mypid = Unix.getpid () in
  let mysock = "/tmp/drovebank-" ^ string_of_int mypid ^ ".sock" in
  let db_oc = open_out_gen [Open_append; Open_creat; Open_binary] 0o600 "drovebank.db" in
  let db_ic = open_in_bin "drovebank.db" in
  Sys.catch_break true;
  match load_from_disk db_ic with
  | None -> (prerr_endline "Corrupted DB, aborting."; exit 1)
  | Some db ->
      Printf.printf "DroveBank server listening on %s\n%!" mysock;
      try
        Unix.establish_server (srv_fun db_oc db) Unix.(ADDR_UNIX mysock)
      with Sys.Break ->
        Printf.eprintf "\nUser-requested abort. Removing sock %s\n" mysock;
        Sys.remove mysock
