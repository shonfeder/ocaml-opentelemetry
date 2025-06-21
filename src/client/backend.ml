module OT = Opentelemetry

module Make
    (Env : Config.ENV)
    (State : State.STATE)
    (Emitter : Signal.EMITTER) : Opentelemetry.Collector.BACKEND = struct
  open Opentelemetry.Proto
  open Opentelemetry.Collector

  let ( let@ ) = ( @@ )

  let lock = Emitter.lock |> Option.value ~default:(fun f -> f ())

  let dbg_send signals =
    if Env.get_debug () then
      let@ () = lock in
      match signals with
      | `Traces s ->
        Format.eprintf "span %a"
          (Format.pp_print_list Trace.pp_resource_spans)
          s
      | `Metrics m ->
        Format.eprintf "metrics %a"
          (Format.pp_print_list Metrics.pp_resource_metrics)
          m
      | `Logs l ->
        Format.eprintf "logs %a" (Format.pp_print_list Logs.pp_resource_logs) l

  let send_trace : Trace.resource_spans list sender =
    {
      send =
        (fun l ~ret ->
          dbg_send (`Traces l);
          Emitter.push_trace l;
          ret ());
    }

  let signal_emit_gc_metrics () =
    if Env.get_debug () then
      Printf.eprintf "opentelemetry: emit GC metrics requested\n%!";
    State.set_needs_gc_metrics true

  let send_metrics : Metrics.resource_metrics list sender =
    {
      send =
        (fun m ~ret ->
          dbg_send (`Metrics m);
          State.sample_gc_metrics_if_needed ();
          let m = List.rev_append (State.additional_metrics ()) m in
          Emitter.push_metrics m;
          ret ());
    }

  let send_logs : Logs.resource_logs list sender =
    {
      send =
        (fun l ~ret ->
          dbg_send (`Logs l);
          Emitter.push_logs l;
          ret ());
    }

  let set_on_tick_callbacks = Emitter.set_on_tick_callbacks

  let tick = Emitter.tick

  let cleanup = Emitter.cleanup
end
