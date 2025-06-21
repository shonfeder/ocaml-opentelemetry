(** Constructing and managing OTel
    {{:https://opentelemetry.io/docs/concepts/signals/} signals} *)

(** Convert signals to protobuf encoded strings, ready to be sent over the wire

    NOTE: The converters share an underlying stateful encoder, so each domain or
    system thread should have its own [Converter] instance *)
module Converter : sig
  val logs :
    ?encoder:Pbrt.Encoder.t ->
    Opentelemetry_proto.Logs.resource_logs list ->
    string
  (** [logs ls] is a protobuf encoded string of the logs [ls]

      @param encoder provide an encoder state to reuse *)

  val metrics :
    ?encoder:Pbrt.Encoder.t ->
    Opentelemetry_proto.Metrics.resource_metrics list ->
    string
  (** [metrics ms] is a protobuf encoded string of the metrics [ms]
      @param encoder provide an encoder state to reuse *)

  val traces :
    ?encoder:Pbrt.Encoder.t ->
    Opentelemetry_proto.Trace.resource_spans list ->
    string
  (** [metrics ts] is a protobuf encoded string of the traces [ts]

      @param encoder provide an encoder state to reuse *)
end

(** An emitter. API to send signals to the collector client. *)
module type EMITTER = sig
  open Opentelemetry.Proto

  val push_trace : Trace.resource_spans list -> unit

  val push_metrics : Metrics.resource_metrics list -> unit

  val push_logs : Logs.resource_logs list -> unit

  val tick : unit -> unit

  val set_on_tick_callbacks : (unit -> unit) Opentelemetry.AList.t -> unit

  val cleanup : on_done:(unit -> unit) -> unit -> unit

  val lock : ((unit -> unit) -> unit) option
end
