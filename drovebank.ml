(*---------------------------------------------------------------------------
   Copyright (c) 2015 Vincent Bernardoff. All rights reserved.
   Distributed under the MIT license, see license at the end of the file.
  ---------------------------------------------------------------------------*)

open Drovelib

let verbose = ref false

(* This is where the atomicity checks are performed. It defines what
   is a valid sequence of two operations. #Entry.transaction means
   "any variant that belongs to the type Entry.transaction". *)
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
      match Entry.input ic with
      | Result.Error End_of_file -> raise End_of_file
      | Result.Error _ -> raise Exit
      | Result.Ok entry ->
          if !verbose then
            prerr_endline (Entry.show entry);
          if is_acceptable !prev_tr entry.Entry.tr then
            match Entry.process !db entry with
            | Result.Error () -> raise Exit
            | Result.Ok db' -> db := db'
          else raise Exit;
          prev_tr := entry.Entry.tr;
          incr nb_records
    done;
    assert false
  with
  | End_of_file -> Some (!nb_records, !prev_tr, !db)
  | exn -> None

(* Module implementing the global live database*)
module DB = struct
  type t = {
    mutable db: int64 Int64.Map.t; (* the type of the live database *)
    mutable oc: out_channel; (* the channel to append entries to the ledger on disk. *)
    mutable prev_tr: Entry.transaction; (* the kind of the previous transaction. *)
    m: Mutex.t; (* the lock to serialize access to the DB. *)
  }

  let create () =
    {
      db = Int64.Map.empty;
      oc = stdout;
      prev_tr = `Atomic;
      m = Mutex.create ();
    }

  (* All accesses to the DB are done using this function that
     handles locking/unlocking the lock. *)
  let with_db ({ db; oc; m } as t) f =
    with_finalize ~f:(fun () -> Mutex.lock m; f t) ~f_final:(fun () -> Mutex.unlock m)
end

(* The global database is created here. It is a global variable, and
   access to it is only done using the DB.with_db function. *)
let db = DB.create ()

(* This code is run in a separate POSIX thread for each client. *)
let srv_fun client_ic client_oc =
  let buf = Bytes.create 1024 in
  while true do
    let nb_read = input client_ic buf 0 1 in
    if nb_read <> 1 then raise End_of_file;
    match Char.code Bytes.(get buf 0) with
    | 0 ->
        DB.(with_db db (fun { db; _ } -> output_value client_oc db));
        flush client_oc
    | 1 ->
        let nb_read = input client_ic buf 0 8 in
        if nb_read <> 8 then raise End_of_file;
        let id = Bytes.BE.get_int64 buf 0 in
        DB.(with_db db (fun { db; _ } ->
            Bytes.BE.set_int64 buf 0 0L;
            (try
              let v = Int64.Map.find id db in
              Bytes.BE.set_int64 buf 0 v;
            with Not_found -> ());
            output client_oc buf 0 8;
            flush client_oc
          ))
    | n ->
        let open DB in
        with_db db (fun t ->
            for i = 0 to n-2 do
              let fail v =
                let retchar = match v with
                  | `Atomicity -> '\001'
                  | `Consistency -> '\002' in
                output_char client_oc retchar;
                flush client_oc
              in
              (* FIXME: A client sending a part of a multiblock
                 transaction and stopping here without disconnecting
                 could block the whole server! This could be fixed by
                 adding timeouts here. *)
              match Entry.input client_ic with
              | Result.Error End_of_file -> raise End_of_file
              | Result.Error _ -> raise Exit
              | Result.Ok entry ->
                  if not (is_acceptable t.prev_tr entry.Entry.tr)
                  then fail `Atomicity
                  else
                    match Entry.process t.db entry with
                    | Result.Error () -> fail `Consistency
                    | Result.Ok db' ->
                        (Entry.output t.oc entry;
                         flush t.oc;
                         t.db <- db';
                         t.prev_tr <- entry.Entry.tr;
                         Printf.eprintf "DB <- DB :: %s\n%!" (Entry.show entry);
                         output_char client_oc '\000';
                         flush client_oc
                        )
            done
          );
  done

(* Entry point to the program, mostly boilerplate code. *)
let () =
  let fresh = ref false in
  let force = ref false in
  let mypid = Unix.getpid () in
  let mysock = ref Filename.(concat
                               "/tmp"
                               ("drovebank-" ^ string_of_int mypid ^ ".sock")) in
  let mydb = ref "drovebank.db" in
  let speclist = Arg.[
      "-fresh", Set fresh, " Start with a fresh DB (default: false)";
      "-force", Set force, " Overwrite the DB file if already exists (default: false)";
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
  if not !force && Sys.file_exists lockfile then
    (
      Printf.eprintf "DB %s already used by another process, exiting.\n" !mydb;
      exit 1
    );
  Sys.catch_break true;
  let oc = open_out_gen
      ((if !fresh then [Open_trunc] else []) @
       [Open_append; Open_creat; Open_binary]) 0o600 "drovebank.db" in
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
        db.DB.oc <- oc;
        Unix.establish_server srv_fun Unix.(ADDR_UNIX !mysock)
      with Sys.Break ->
        Printf.eprintf "\nUser-requested abort. Removing sock %s\n" !mysock;
        Sys.remove !mysock;
        Sys.remove lockfile

(*---------------------------------------------------------------------------
  Copyright (c) 2015 Vincent Bernardoff

  Permission is hereby granted, free of charge, to any person
  obtaining a copy of this software and associated documentation files
  (the "Software"), to deal in the Software without restriction,
  including without limitation the rights to use, copy, modify, merge,
  publish, distribute, sublicense, and/or sell copies of the Software,
  and to permit persons to whom the Software is furnished to do so,
  subject to the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
  ---------------------------------------------------------------------------*)
