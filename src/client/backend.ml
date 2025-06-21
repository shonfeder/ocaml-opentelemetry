module Make (Sender : Signal.SENDER) (Emitter : Signal.EMITTER) :
  Opentelemetry.Collector.BACKEND = struct
  open Opentelemetry.Proto
  open Opentelemetry.Collector

  let lock = Emitter.lock

  let send_trace : Trace.resource_spans list sender =
    Sender.send_trace ?lock Emitter.push_trace

  let signal_emit_gc_metrics = Sender.signal_emit_gc_metrics

  let send_metrics : Metrics.resource_metrics list sender =
    Sender.send_metrics ?lock Emitter.push_metrics

  let send_logs : Logs.resource_logs list sender =
    Sender.send_logs ?lock Emitter.push_logs

  let set_on_tick_callbacks = Emitter.set_on_tick_callbacks

  let tick = Emitter.tick

  let cleanup = Emitter.cleanup
end
