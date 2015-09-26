open Drovelib

let with_connection saddr f =
  let ic, oc = Unix.(open_connection (ADDR_UNIX saddr)) in
  try
    f ic oc;
    close_in ic;
    close_out oc
  with exn ->
    close_in ic;
    close_out oc;
    raise exn

let list ic oc =
  let (db : int64 Int64.Map.t) = input_value ic in
  Int64.Map.iter (fun id qty ->
      Printf.printf "%Ld: %Ld\n%!" id qty
    ) db

let deposit ic oc = ()

let () =
  let op = ref None in
  let speclist = Arg.[
      "-list", Unit (fun () -> op := Some `List), " Display ledger";
      "-deposit", Unit (fun () -> op := Some `Deposit),
      "<id> <qty> Deposit cash into an account";
      "-withdraw", Unit (fun () -> op := Some `Withdraw),
      "<id> <qty> Withdraw cash from an account";
      "-transfer", Unit (fun () -> op := Some `Transfer),
      "<id> <id'> <qty> Transfer cash from an account to another";
  ] in
  let usage_msg =
    Printf.sprintf "Usage: %s <options>\nOptions are:" Sys.argv.(0) in
  Arg.parse speclist ignore usage_msg;
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
    | h::_ -> h in
  try
    match !op with
    | None -> Arg.usage speclist usage_msg
    | Some `List ->
        with_connection drovebank list
    | Some `Deposit -> ()
    | Some `Withdraw -> ()
    | Some `Transfer -> ()
  with _ ->
    Arg.usage speclist usage_msg
