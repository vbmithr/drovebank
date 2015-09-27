let with_finalize ~f_final ~f =
  try
    let res = f () in
    f_final ();
    res
  with exn ->
    f_final ();
    raise exn

let with_ic ic f =
  let f () = f ic in
  let f_final () = close_in ic in
  with_finalize ~f_final ~f

let with_oc ic f =
  let f () = f ic in
  let f_final () = close_out ic in
  with_finalize ~f_final ~f

module Unix = struct
  include Unix

  let rec accept_non_intr s =
  try accept s
  with Unix_error (EINTR, _, _) -> accept_non_intr s

  let with_connection saddr f =
  let ic, oc = Unix.(open_connection saddr) in
  with_finalize
    ~f_final:(fun () -> close_in ic)
    ~f:(fun () -> f ic oc)

  let establish_server server_fun sockaddr =
    let sock =
      socket (domain_of_sockaddr sockaddr) SOCK_STREAM 0 in
    setsockopt sock SO_REUSEADDR true;
    bind sock sockaddr;
    listen sock 5;
    let thread_fun (s, caller) =
      set_close_on_exec s;
      let inchan = in_channel_of_descr s in
      let outchan = out_channel_of_descr s in
      server_fun inchan outchan;
      (* Do not close inchan nor outchan, as the server_fun could
         have done it already, and we are about to exit anyway
         (PR#3794) *)
      Thread.exit ()
    in
    while true do
      let (s, caller) = accept_non_intr sock in
      ignore(Thread.create thread_fun (s, caller))
    done
end

module Int64 = struct
  include Int64
  let ( + ) = add
  let ( - ) = sub
  let ( * ) = mul
  let ( / ) = div

  module Map = Map.Make(Int64)
end

module Bytes = struct
  include Bytes

  external swap64 : int64 -> int64 = "%bswap_int64"
  external set_64 : Bytes.t -> int -> int64 -> unit = "%caml_string_set64"
  external get_64 : Bytes.t -> int -> int64 = "%caml_string_get64"

  module BE = struct
    let set_int64 buf pos i =
      set_64 buf pos (if Sys.big_endian then i else swap64 i)
    let get_int64 buf pos =
      if Sys.big_endian
      then get_64 buf pos
      else swap64 (get_64 buf pos)
  end

  module LE = struct
    let set_int64 buf pos i =
      set_64 buf pos (if Sys.big_endian then swap64 i else i)
    let get_int64 buf pos =
      if Sys.big_endian
      then swap64 (get_64 buf pos)
      else get_64 buf pos
  end
end

module Result = struct
  type ('a, 'b) t = Ok of 'a | Error of 'b

  let return v = Ok v
  let fail v = Error v
end

module Entry = struct
  type transaction = [`Cont | `Begin | `End | `Atomic]

  let string_of_transaction = function
    | `Cont -> "C"
    | `Begin -> "B"
    | `End -> "E"
    | `Atomic -> "A"

  let transaction_of_string = function
    | "C" -> `Cont
    | "B" -> `Begin
    | "E" -> `End
    | "A" -> `Atomic
    | _ -> invalid_arg "transaction_of_string"

  type op = [`Deposit | `Withdraw]

  let string_of_op = function
    | `Deposit -> "D"
    | `Withdraw -> "W"

  let op_of_string = function
    | "D" -> `Deposit
    | "W" -> `Withdraw
    | _ -> invalid_arg "op_of_string"

  type t = {
    tr: transaction;
    op: op;
    id: int64;
    qty: int64
  }

  let pp fmt t =
    Format.fprintf fmt "{ tr=%s; op=%s; id=%Ld; qty=%Ld; }"
      (string_of_transaction t.tr)
      (string_of_op t.op)
      t.id t.qty

  let show t =
    Printf.sprintf "{ tr=%s; op=%s; id=%Ld; qty=%Ld; }"
      (string_of_transaction t.tr)
      (string_of_op t.op)
      t.id t.qty

  let create ?(tr=`Atomic) ~op ~id ~qty () = { tr; op; id; qty; }

  let int64_of_transaction = function
    | `Cont -> 0L
    | `Begin -> 1L
    | `End -> 2L
    | `Atomic -> 3L

  let transaction_of_int64 = function
    | 0L -> `Cont
    | 1L -> `Begin
    | 2L -> `End
    | 3L -> `Atomic
    | _ -> invalid_arg "transaction_of_int64"

  let int64_of_op = function
    | `Deposit -> 0L
    | `Withdraw -> 4L

  let write buf pos t =
    let b1 =
      let open Int64 in
      logor
        (shift_left t.id 3)
        (logor (int64_of_transaction t.tr) (int64_of_op t.op))
    in
    Bytes.BE.set_int64 buf pos b1;
    Bytes.BE.set_int64 buf (pos+8) t.qty

  let write' ?(tr=`Atomic) ~op ~id ~qty buf pos =
    let b1 =
      let open Int64 in
      logor
        (shift_left id 3)
        (logor (int64_of_transaction tr) (int64_of_op op))
    in
    Bytes.BE.set_int64 buf pos b1;
    Bytes.BE.set_int64 buf (pos+8) qty

  let read buf pos =
    let b1 = Bytes.BE.get_int64 buf pos in
    let qty = Bytes.BE.get_int64 buf (pos+8) in
    let id = Int64.shift_right_logical b1 3 in
    let tr = transaction_of_int64 Int64.(logand b1 3L) in
    let op = if Int64.(logand b1 4L) = 0L then `Deposit else `Withdraw in
    { tr; op; id; qty; }

  let input ic =
    let buf = Bytes.create 16 in
    let rec inner pos len =
      let nb_read = input ic buf pos len in
      if nb_read = len then Result.return (read buf 0)
      else if nb_read = 0 && len = 16 then Result.(fail End_of_file)
      else if nb_read = 0 then Result.(fail Exit)
      else inner (pos+nb_read) (len-nb_read)
    in inner 0 16

  let output oc t =
    let buf = Bytes.create 16 in
    write buf 0 t;
    output oc buf 0 16

  let process db t = match t.op with
    | `Deposit ->
        Result.return
          (try
             Int64.(Map.add t.id (Map.find t.id db + t.qty) db)
           with Not_found ->
             Int64.Map.add t.id t.qty db)
    | `Withdraw ->
        try
          let cur_qty = Int64.Map.find t.id db in
          if cur_qty >= t.qty
          then Result.return (Int64.Map.add t.id Int64.(cur_qty - t.qty) db)
          else Result.fail ()
        with Not_found -> Result.fail ()
end

module Operation = struct
  let dump ic oc =
  output_char oc '\000';
  flush oc;
  try Result.return (input_value ic : int64 Int64.Map.t)
  with _ -> Result.fail ()

  let query ic oc id =
    let buf = Bytes.create 8 in
    Bytes.BE.set_int64 buf 0 id;
    output_char oc '\001';
    output oc buf 0 8;
    flush oc;
    input ic buf 0 8

  let atomic ~op ~id ~qty ic oc =
    let buf = Bytes.create 16 in
    Entry.write' ~id ~qty ~tr:`Atomic ~op buf 0;
    output_char oc '\002';
    output oc buf 0 16;
    flush oc;
    match input_char ic with
    | '\000' -> Result.return ()
    | '\001' -> Result.fail `Atomicity
    | '\002' -> Result.fail `Consistency
    | _ -> assert false

  let transfer ~id ~id' ~qty ic oc =
    let buf = Bytes.create 32 in
    Entry.write' ~id ~qty ~tr:`Begin ~op:`Withdraw buf 0;
    Entry.write' ~id:id' ~qty ~tr:`End ~op:`Deposit buf 16;
    output_char oc '\003';
    output oc buf 0 32;
    flush oc;
    let c1 = input_char ic in
    let c2 = input_char ic in
    match c1, c2 with
    | '\000', '\000' -> Result.return ()
    | '\001', _ -> Result.fail `Atomicity
    | '\002', _ -> Result.fail `Consistency
    | _ -> assert false

  let custom ~id ~qty ~tr ~op ~len ic oc =
    let buf = Bytes.create 16 in
    let op = Entry.op_of_string op in
    let tr = Entry.transaction_of_string tr in
    let len = Int64.to_int len in
    Entry.write' ~id ~qty ~tr ~op buf 0;
    output_char oc '\002';
    output oc buf 0 (min 16 len);
    flush oc;
    match input_char ic with
    | '\000' -> Result.return ()
    | '\001' -> Result.fail `Atomicity
    | '\002' -> Result.fail `Consistency
    | _ -> assert false
end
