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

(** An emitter. API to send signals to the collector client. *)
module type EMITTER = sig
  open Opentelemetry.Proto

  val push_trace : Trace.resource_spans list -> unit

  val push_metrics : Metrics.resource_metrics list -> unit

  val push_logs : Logs.resource_logs list -> unit

  val tick : unit -> unit

  val cleanup : on_done:(unit -> unit) -> unit -> unit

  val lock : ((unit -> unit) -> unit) option
end
