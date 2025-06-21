module OT = Opentelemetry
module Trace_service = OT.Proto.Trace_service
module Metrics_service = OT.Proto.Metrics_service
module Logs_service = OT.Proto.Logs_service
module Span = OT.Span

let ( let@ ) = ( @@ )

module Converter = struct
  let resource_to_string ~encoder ~ctor ~enc resource =
    let encoder =
      match encoder with
      | Some e -> e
      | None -> Pbrt.Encoder.create ()
    in
    let x = ctor resource in
    let@ _sc = Self_trace.with_ ~kind:Span.Span_kind_internal "encode-proto" in
    Pbrt.Encoder.reset encoder;
    enc x encoder;
    Pbrt.Encoder.to_string encoder

  let logs ?encoder resource_logs =
    resource_logs
    |> resource_to_string ~encoder
         ~ctor:(fun r ->
           Logs_service.default_export_logs_service_request ~resource_logs:r ())
         ~enc:Logs_service.encode_pb_export_logs_service_request

  let metrics ?encoder resource_metrics =
    resource_metrics
    |> resource_to_string ~encoder
         ~ctor:(fun r ->
           Metrics_service.default_export_metrics_service_request
             ~resource_metrics:r ())
         ~enc:Metrics_service.encode_pb_export_metrics_service_request

  let traces ?encoder resource_spans =
    resource_spans
    |> resource_to_string ~encoder
         ~ctor:(fun r ->
           Trace_service.default_export_trace_service_request ~resource_spans:r
             ())
         ~enc:Trace_service.encode_pb_export_trace_service_request
end

module Sender (Env : Config.ENV) (State : State.STATE) = struct
  module OT = Opentelemetry

  let dbg_send ?(lock = fun f -> f ()) signals =
    if Env.get_debug () then
      let@ () = lock in
      match signals with
      | `Traces s ->
        Format.eprintf "span %a"
          (Format.pp_print_list OT.Proto.Trace.pp_resource_spans)
          s
      | `Metrics m ->
        Format.eprintf "metrics %a"
          (Format.pp_print_list OT.Proto.Metrics.pp_resource_metrics)
          m
      | `Logs l ->
        Format.eprintf "logs %a"
          (Format.pp_print_list OT.Proto.Logs.pp_resource_logs)
          l

  let send_trace ?lock emitter :
      OT.Proto.Trace.resource_spans list OT.Collector.sender =
    {
      send =
        (fun l ~ret ->
          dbg_send ?lock (`Traces l);
          emitter l;
          ret ());
    }

  let send_metrics ?lock emitter :
      OT.Proto.Metrics.resource_metrics list OT.Collector.sender =
    {
      send =
        (fun m ~ret ->
          dbg_send ?lock (`Metrics m);
          let m = List.rev_append (State.additional_metrics ()) m in
          emitter m;
          ret ());
    }

  let send_logs ?lock emitter :
      OT.Proto.Logs.resource_logs list OT.Collector.sender =
    {
      send =
        (fun l ~ret ->
          dbg_send ?lock (`Logs l);
          emitter l;
          ret ());
    }

  let signal_emit_gc_metrics () =
    if Env.get_debug () then
      Printf.eprintf "opentelemetry: emit GC metrics requested\n%!";
    State.set_needs_gc_metrics true
end
