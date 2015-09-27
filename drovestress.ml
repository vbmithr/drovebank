open Drovelib

(* This code creates a closure that is used to locally keep track of
   drovebank balances. It returns a function taking a function as an
   argument, that will be applied on the hashtable, the operation
   being protected by a mutex. This is typically the "memoize" pattern
   used in functional programming. *)
let mk_bank () =
  let bank : (int64, int) Hashtbl.t = Hashtbl.create 13 in
  let m = Mutex.create () in
  fun f ->
    with_finalize
      ~f_final:(fun () -> Mutex.unlock m)
      ~f:(fun () -> Mutex.lock m; f bank)

(* with_bank is a function that takes a function as parameter. It
   stores the hashtable and the mutex in its closure. This way, the
   only way to access this data is to call `with_bank` with the
   function that perform the data access. *)
let with_bank = mk_bank ()

(* Each trade simulating a piggy will call this code. Each piggy
   starts with the same amount of cash, `init_cash` and repeatedly
   perform either a deposit, withdraw, or transfer with a 1/3
   probability. The quantities of cash operated on are randomly chosed
   between zero and the amount of cash available. *)
let thread_fun (drovebank, myid, number_peers, init_cash) =
  let myid = Int64.of_int myid in
  let cash_outstanding = ref init_cash in
  Unix.with_connection drovebank (fun ic oc ->
      while true do
        Thread.delay 0.001;
        match Random.int 3 with
        | 0 ->
            let qty = Random.int (succ !cash_outstanding) in
            with_bank (fun ht ->
                let cash_in_bank = try Hashtbl.find ht myid with Not_found -> 0 in
                (match Operation.atomic
                         ~op:`Deposit ~id:myid ~qty:Int64.(of_int qty) ic oc with
                | Result.Ok () ->
                    cash_outstanding := !cash_outstanding - qty;
                    Hashtbl.replace ht myid (cash_in_bank + qty);
                    Printf.printf "%Ld D %d [out=%d, bank=%d]\n%!"
                      myid qty !cash_outstanding (cash_in_bank + qty)
                | Result.Error v ->
                    let errortype =
                      match v with `Atomicity -> "!" | `Consistency -> "!!" in
                    Printf.printf "%s%Ld D %d [out=%d, bank=%d]\n%!"
                      errortype myid qty !cash_outstanding cash_in_bank
                );
              )
        | 1 ->
            with_bank (fun ht ->
                let cash_in_bank = try Hashtbl.find ht myid with Not_found -> 0 in
                let qty = Random.int (succ cash_in_bank) in
                (match Operation.atomic
                         ~op:`Withdraw ~id:myid ~qty:Int64.(of_int qty) ic oc with
                | Result.Ok () ->
                    cash_outstanding := !cash_outstanding + qty;
                    Hashtbl.(replace ht myid (cash_in_bank - qty));
                    Printf.printf "%Ld W %d [out=%d, bank=%d]\n%!"
                      myid qty !cash_outstanding (cash_in_bank - qty)
                | Result.Error v ->
                    let errortype =
                      match v with `Atomicity -> "!" | `Consistency -> "!!" in
                    Printf.printf "%s%Ld W %d [out=%d, bank=%d]\n%!"
                      errortype myid qty !cash_outstanding cash_in_bank
                )
              )
        | 2 ->
            let id' = Random.int number_peers |> Int64.of_int in
            if myid <> id' then
              with_bank (fun ht ->
                  let my_balance =
                    try Hashtbl.find ht myid with Not_found -> 0 in
                  let peer_balance =
                    try Hashtbl.find ht id' with Not_found -> 0 in
                  let qty = try Random.int my_balance with _ -> 0 in
                  (match Operation.transfer
                           ~id:myid
                           ~id'
                           ~qty:Int64.(of_int qty) ic oc with
                  | Result.Ok () ->
                      Hashtbl.replace ht myid (my_balance - qty);
                      Hashtbl.replace ht id' (peer_balance + qty);
                      Printf.printf "%Ld T %Ld %d [out=%d, bank=%d]\n%!"
                        myid id' qty !cash_outstanding (my_balance - qty)
                  | Result.Error v ->
                      let errortype =
                        match v with `Atomicity -> "!" | `Consistency -> "!!" in
                      Printf.printf "%s%Ld T %Ld %d [out=%d, bank=%d] \n%!"
                        errortype myid id' qty !cash_outstanding my_balance
                  )
                )
        | _ -> assert false
      done
    )

let () =
  let n = ref 10 in
  let init_cash = ref 1000 in
  let anon_args = ref [] in
  let speclist = Arg.[
      "-n", Set_int n,  "<int> Number of concurrent threads (default: 10)";
      "-init_cash", Set_int init_cash,
      "<int> Qty of cash the piggies start with (default: 1000)";
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
    | h::_ -> Unix.ADDR_UNIX ("/tmp/" ^ h) in
  let rec create_threads acc i =
    if i >= 0 then
      create_threads ((Thread.create thread_fun (drovebank, i, !n, !init_cash))::acc) (pred i)
    else acc in
  let ths = create_threads [] (!n - 1) in
  List.iter Thread.join ths
