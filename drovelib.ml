module Int64 = struct
  include Int64
  let ( + ) = add
  let ( - ) = rem
  let ( * ) = mul
  let ( / ) = div
end

module Operation = struct
  type t = List | Deposit | Withdraw | Transfer
  let of_string s = match String.lowercase s with
    | "list" -> List
    | "deposit" -> Deposit
    | "withdraw" -> Withdraw
    | "transfer" -> Transfer
    | _ -> invalid_arg "of_string"

  let to_string = function
    | List -> "list"
    | Deposit -> "deposit"
    | Withdraw -> "withdraw"
    | Transfer -> "transfer"
end
