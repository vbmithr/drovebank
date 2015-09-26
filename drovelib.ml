module Int64 = struct
  include Int64
  let ( + ) = add
  let ( - ) = rem
  let ( * ) = mul
  let ( / ) = div

  module Map = Map.Make(Int64)
end

module Operation = struct
  type t =
    | List
    | Deposit of int64 * int64
    | Withdraw of int64 * int64
    | Transfer of int64 * int64 * int64
  (* let of_string s = match String.lowercase s with *)
  (*   | "list" -> List *)
  (*   | "deposit" -> Deposit *)
  (*   | "withdraw" -> Withdraw *)
  (*   | "transfer" -> Transfer *)
  (*   | _ -> invalid_arg "of_string" *)

  (* let to_string = function *)
  (*   | List -> "list" *)
  (*   | Deposit -> "deposit" *)
  (*   | Withdraw -> "withdraw" *)
  (*   | Transfer -> "transfer" *)
end

module Entry = struct
  type transaction = [`Cont | `Begin | `End | `Atomic]
  type op = [`Deposit | `Withdraw]

  type t = {
    tr: transaction;
    op: op;
    id: int64;
    qty: int64
  }

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
    EndianBytes.BigEndian.set_int64 buf pos b1;
    EndianBytes.BigEndian.set_int64 buf (pos+8) t.qty

  let read buf pos =
    let b1 = EndianBytes.BigEndian.get_int64 buf pos in
    let qty = EndianBytes.BigEndian.get_int64 buf (pos+8) in
    let id = Int64.shift_right_logical b1 3 in
    let tr = transaction_of_int64 Int64.(logand b1 3L) in
    let op = if Int64.(logand b1 4L) = 0L then `Deposit else `Withdraw in
    { tr; op; id; qty; }

  let input ic =
    let buf = Bytes.create 16 in
    let rec inner pos len =
      let nb_read = input ic buf pos len in
      if nb_read = len then read buf 0
      else inner (pos+nb_read) (len-nb_read)
    in inner 0 16

  let output oc t =
    let buf = Bytes.create 16 in
    write buf 0 t;
    output oc buf 0 16

  let process db t = match t.op with
    | `Deposit ->
        (try
           Int64.Map.(add t.id Int64.(Map.find t.id db + t.qty) db)
         with Not_found ->
           Int64.Map.add t.id t.qty db)
    | `Withdraw ->
        try
          let cur_qty = Int64.Map.find t.id db in
          if cur_qty >= t.qty
          then Int64.Map.add t.id Int64.(cur_qty - t.qty) db
          else raise Exit
        with Not_found ->
          raise Exit
end
