module OT = Opentelemetry
module AList = OT.AList

module Make () : sig
  val incr_dropped : unit -> unit

  val incr_errors : unit -> unit

  val drain_gc_metrics : unit -> OT.Proto.Metrics.resource_metrics list

  val sample_gc_metrics_if_needed : unit -> unit

  val set_needs_gc_metrics : bool -> unit

  val additional_metrics : unit -> OT.Proto.Metrics.resource_metrics list

  val gc_metrics_empty : unit -> bool

  val get_gc_metrics : unit -> OT.Proto.Metrics.resource_metrics list
end = struct
  open Opentelemetry.Proto

  let needs_gc_metrics = Atomic.make false

  let set_needs_gc_metrics b = Atomic.set needs_gc_metrics b

  let last_gc_metrics = Atomic.make (Mtime_clock.now ())

  let timeout_gc_metrics = Mtime.Span.(20 * s)

  let gc_metrics = AList.make ()
  (* side channel for GC *)

  let last_sent_metrics_ = Atomic.make (Mtime_clock.now ())

  let timeout_sent_metrics = Mtime.Span.(5 * s)
  (* Span after which to send exporter metrics from time to time *)

  let n_errors = Atomic.make 0

  let incr_errors () = Atomic.incr n_errors

  let n_dropped = Atomic.make 0

  let incr_dropped () = Atomic.incr n_dropped

  let get_last_sent_metrics () = Atomic.get last_sent_metrics_

  let set_last_sent_metrics now = Atomic.set last_sent_metrics_ now

  let additional_metrics () : Metrics.resource_metrics list =
    (* add exporter metrics to the lot? *)
    let last_emit = get_last_sent_metrics () in
    let now = Mtime_clock.now () in
    let add_own_metrics =
      let elapsed = Mtime.span last_emit now in
      Mtime.Span.compare elapsed timeout_sent_metrics > 0
    in

    (* there is a possible race condition here, as several threads might update
       metrics at the same time. But that's harmless. *)
    if add_own_metrics then (
      set_last_sent_metrics now;
      let open OT.Metrics in
      [
        make_resource_metrics
          [
            sum ~name:"otel.export.dropped" ~is_monotonic:true
              [
                int
                  ~start_time_unix_nano:(Mtime.to_uint64_ns last_emit)
                  ~now:(Mtime.to_uint64_ns now) (Atomic.get n_dropped);
              ];
            sum ~name:"otel.export.errors" ~is_monotonic:true
              [
                int
                  ~start_time_unix_nano:(Mtime.to_uint64_ns last_emit)
                  ~now:(Mtime.to_uint64_ns now) (Atomic.get n_errors);
              ];
          ];
      ]
    ) else
      []

  (* capture current GC metrics if {!needs_gc_metrics} is true,
   or it has been a long time since the last GC metrics collection,
   and push them into {!gc_metrics} for later collection *)
  let sample_gc_metrics_if_needed () =
    let now = Mtime_clock.now () in
    let alarm = Atomic.compare_and_set needs_gc_metrics true false in
    let timeout () =
      let elapsed = Mtime.span now (Atomic.get last_gc_metrics) in
      Mtime.Span.compare elapsed timeout_gc_metrics > 0
    in
    if alarm || timeout () then (
      Atomic.set last_gc_metrics now;
      let l =
        OT.Metrics.make_resource_metrics
          ~attrs:(Opentelemetry.GC_metrics.get_runtime_attributes ())
        @@ Opentelemetry.GC_metrics.get_metrics ()
      in
      AList.add gc_metrics l
    )

  let drain_gc_metrics () = AList.pop_all gc_metrics

  let get_gc_metrics () = AList.get gc_metrics

  let gc_metrics_empty () = AList.is_empty gc_metrics
end
