open Drovelib

module Entry = struct
  let int64_of_transaction = function
    | `Begin -> 1L
    | `End -> 2L
    | `None -> 3L

  let transaction_of_int64 = function
    | 1L -> `Begin
    | 2L -> `End
    | 3L -> `None
    | _ -> invalid_arg "transaction_of_int64"

  let int64_of_op = function
    | `Deposit -> 0L
    | `Withdraw -> 4L

  let write ~buf ~pos ~id ~qty ~transaction ~op =
    let b1 =
      let open Int64 in
      logor
        (shift_left id 3)
        (logor (int64_of_transaction transaction) (int64_of_op op))
    in
    EndianBytes.BigEndian.set_int64 buf pos b1;
    EndianBytes.BigEndian.set_int64 buf (pos+8) qty

  let read buf pos =
    let b1 = EndianBytes.BigEndian.get_int64 buf pos in
    let qty = EndianBytes.BigEndian.get_int64 buf (pos+8) in
    let id = Int64.shift_right_logical b1 3 in
    let transaction = transaction_of_int64 Int64.(logand b1 3L) in
    transaction, ((if Int64.(logand b1 4L) = 0L then `Deposit else `Withdraw), id, qty)

  let process db = function
    | `Deposit, id, qty ->
        (try
           Hashtbl.(replace db id Int64.((find db id) + qty))
         with Not_found ->
           Hashtbl.add db id qty)
    | `Withdraw, id, qty ->
        try
          let cur_qty = Hashtbl.find db id in
          if cur_qty >= qty
          then Hashtbl.replace db id Int64.(cur_qty - qty)
          else raise Exit
        with Not_found ->
          raise Exit
end

let consume_transaction ?db_oc db ic =
  let buf = Bytes.create 16 in
  let rec inner acc =
    let nb_read = input ic buf 0 16 in
    if nb_read <> 16 then raise End_of_file;
    (match db_oc with
    | None -> ()
    | Some oc -> output oc buf 0 16
    );
    let transaction, entry = Entry.read buf 0 in
    match transaction with
    | `None ->
        Entry.process db entry
    | `Begin ->
        inner (entry::acc);
    | `End ->
        List.(iter (Entry.process db) (rev (entry::acc)))
  in inner []

let load_from_disk ic =
  let db = Hashtbl.create 13 in
  try
    while true do
      consume_transaction db ic
    done;
    assert false
  with
  | End_of_file -> Some db
  | exn -> None

let srv_fun db_oc db client_ic client_oc =
  while true do
    consume_transaction ~db_oc db client_ic
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
