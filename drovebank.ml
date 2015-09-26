open Drovelib

let verbose = ref false

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
  let nb_records = ref 0 in
  try
    while true do
      let entry = Entry.input ic in
      if !verbose then
        prerr_endline (Entry.show entry);
      if is_acceptable !prev_tr entry.Entry.tr
      then db := Entry.process !db entry
      else raise Exit;
      prev_tr := entry.Entry.tr;
      incr nb_records
    done;
    assert false
  with
  | End_of_file -> Some (!nb_records, !prev_tr, !db)
  | exn -> None

module DB = struct
  type t = {
    mutable db: int64 Int64.Map.t;
    mutable oc: out_channel;
    mutable prev_tr: Entry.transaction;
    mutable in_use: bool;
    m: Mutex.t;
    c: Condition.t;
  }

  let create () =
    {
      db = Int64.Map.empty;
      oc = stdout;
      prev_tr = `Atomic;
      in_use = false;
      m = Mutex.create ();
      c = Condition.create ();
    }

  let with_db ({ db; oc; in_use; m; c } as t) f =
    Mutex.lock m;
    while in_use = true do
      Condition.wait c m
    done;
    t.in_use <- true;
    with_finalize
      ~f:(fun () -> f t)
      ~f_final:(fun () ->
          t.in_use <- false;
          if in_use = false then Condition.signal c;
          Mutex.unlock m
        )
end

let db = DB.create ()

let srv_fun client_ic client_oc =
  let buf = Bytes.create 1024 in
  while true do
    let nb_read = input client_ic buf 0 1 in
    Printf.eprintf "Read %d bytes.\n%!" nb_read;
    if nb_read <> 1 then raise End_of_file;
    match Char.code Bytes.(get buf 0) with
    | 0 ->
        Printf.eprintf "<- LIST\n%!";
        DB.(with_db db (fun { db; _ } -> output_value client_oc db));
        flush client_oc
    | n ->
        for i = 0 to n-1 do
          let entry = Entry.input client_ic in
          let open DB in
          with_db db (fun ({ db; oc; prev_tr; _ } as t) ->
              if is_acceptable prev_tr entry.Entry.tr
              then
                (Entry.output oc entry;
                 flush oc;
                 t.db <- Entry.process db entry;
                 t.prev_tr <- entry.Entry.tr;
                 Printf.eprintf "DB <- DB :: %s\n" (Entry.show entry)
                )
              else
                (output_string client_oc "NOK\n";
                 flush client_oc;
                 raise Exit
                )
            );
        done;
        output_string client_oc "OK\n";
        flush client_oc
  done

let () =
  let mypid = Unix.getpid () in
  let mysock = ref Filename.(concat
                               "/tmp"
                               ("drovebank-" ^ string_of_int mypid ^ ".sock")) in
  let mydb = ref "drovebank.db" in
  let speclist = Arg.[
      "-v", Set verbose, " Be verbose (default: false)";
      "-sock", Set_string mysock,
      "<string> UNIX socket to listen on (default: /tmp/drovebank-<PID>.sock)";
      "-db", Set_string mydb,
      "<string> Filename of the DB to use (default: drovebank.db)";
  ] in
  let usage_msg =
    Printf.sprintf "Usage: %s <options>\nOptions are:" Sys.argv.(0) in
  Arg.parse speclist ignore usage_msg;
  let lockfile = Filename.(chop_extension !mydb) ^ ".LOCK" in
  if Sys.file_exists lockfile then
    (
      Printf.eprintf "DB %s already used by another process, exiting.\n" !mydb;
      exit 1
    );
  Sys.catch_break true;
  match with_ic (open_in_bin "drovebank.db") (fun ic -> load_from_disk ic) with
  | None ->
      (prerr_endline "Corrupted DB, aborting."; exit 1)
  | Some (nb_records, prev_tr, db') ->
      db.DB.db <- db';
      db.DB.prev_tr <- prev_tr;
      close_out (let oc = open_out_bin lockfile in output_string oc (string_of_int mypid); oc);
      Printf.printf "Successfully imported %d records from DB.\n%!" nb_records;
      Printf.printf "DroveBank server listening on %s\n%!" !mysock;
      try
        let oc = open_out_gen
            [Open_append; Open_creat; Open_binary] 0o600 "drovebank.db" in
        db.DB.oc <- oc;
        Unix.establish_server srv_fun Unix.(ADDR_UNIX !mysock)
      with Sys.Break ->
        Printf.eprintf "\nUser-requested abort. Removing sock %s\n" !mysock;
        Sys.remove !mysock;
        Sys.remove lockfile
