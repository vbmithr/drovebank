open Drovelib

let thread_fun (drovebank, myid, number_peers, init_cash) =
  let myid = Int64.of_int myid in
  let cash_outstanding = ref init_cash in
  Unix.with_connection drovebank (fun ic oc ->
      while true do
        Thread.yield ();
        match Random.int 3 with
        | 0 ->
            let qty = Random.int (succ !cash_outstanding) in
            (match Operation.atomic
                     ~op:`Deposit ~id:myid ~qty:Int64.(of_int qty) ic oc with
            | Result.Ok () ->
                cash_outstanding := !cash_outstanding - qty;
                Printf.printf "<%Ld>" myid
            | _ ->
                Printf.printf "<|%Ld|>" myid
            )
        | 1 ->
            let qty = Random.int (succ (init_cash - !cash_outstanding)) in
            (match Operation.atomic
                     ~op:`Withdraw ~id:myid ~qty:Int64.(of_int qty)  ic oc with
            | Result.Ok () ->
                cash_outstanding := !cash_outstanding + qty;
                Printf.printf "{%Ld}" myid
            | _ ->
                Printf.printf "{|%Ld|}" myid
            )
        | 2 ->
            let id' = Random.int number_peers in
            let qty = 1 in
            (match Operation.transfer
                     ~id:myid
                     ~id':Int64.(of_int id')
                     ~qty:Int64.(of_int qty) ic oc with
            | Result.Ok () ->
                Printf.printf "[%Ld]" myid
            | _ ->
                Printf.printf "[|%Ld|]" myid
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
    if i > 0 then (Thread.create thread_fun (drovebank, i, !n, !init_cash))::acc
    else acc in
  let ths = create_threads [] !n in
  List.iter Thread.join ths
