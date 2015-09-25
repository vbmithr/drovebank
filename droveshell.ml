open Drovelib

let () =
  let speclist = [] in
  let usage_msg =
    Printf.sprintf "Usage: %s [list | deposit | withdraw | transfer]\nOptions are:" Sys.argv.(0) in
  Arg.parse speclist ignore usage_msg;
  try
    match Operation.of_string Sys.argv.(1) with
    | Operation.List -> ()
    | Operation.Deposit -> ()
    | Operation.Withdraw -> ()
    | Operation.Transfer -> ()
  with _ ->
    Arg.usage speclist usage_msg
