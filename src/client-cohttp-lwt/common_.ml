module Atomic = Opentelemetry_atomic.Atomic

let[@inline] ( let@ ) f x = f x

let spf = Printf.sprintf
