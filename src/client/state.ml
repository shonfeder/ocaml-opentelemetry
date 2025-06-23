module OT = Opentelemetry
module AList = OT.AList

module type STATE = sig
  val tid : unit -> int

  val incr_dropped : unit -> unit

  val incr_errors : unit -> unit

  val drain_gc_metrics : unit -> OT.Proto.Metrics.resource_metrics list

  val sample_gc_metrics_if_needed : unit -> unit

  val set_needs_gc_metrics : bool -> unit

  val additional_metrics : unit -> OT.Proto.Metrics.resource_metrics list

  val gc_metrics_empty : unit -> bool

  val get_gc_metrics : unit -> OT.Proto.Metrics.resource_metrics list

  module Tick : sig
    val set_on_tick_callbacks : (unit -> unit) AList.t -> unit

    val tick : (unit -> 'a) -> unit -> 'a
  end

  module State : sig
    type t
  end

  module Action : sig
    type t = private
      | Start
      | Wait
      | Send of {
          url: string;
          data: string;
        }
      | End
  end

  module Event : sig
    type t

    val stop : t

    val ready : t

    val error : t

    val batch : Signal.t -> t
  end

  val update : State.t -> Event.t -> Action.t Seq.t
end

module Make (Env : Config.ENV) : STATE = struct
  open Opentelemetry.Proto

  let tid () = Thread.id @@ Thread.self ()

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

  module Tick = struct
    let on_tick_cbs_ = Atomic.make (AList.make ())

    let set_on_tick_callbacks = Atomic.set on_tick_cbs_

    let tick f () =
      if Env.get_debug () then Printf.eprintf "tick (from %d)\n%!" (tid ());
      sample_gc_metrics_if_needed ();
      List.iter
        (fun f ->
          try f ()
          with e ->
            Printf.eprintf "on tick callback raised: %s\n"
              (Printexc.to_string e))
        (AList.get @@ Atomic.get on_tick_cbs_);
      f ()
  end

  module State = struct
    type batches = {
      traces: OT.Proto.Trace.resource_spans Batch.t;
      logs: OT.Proto.Logs.resource_logs Batch.t;
      metrics: OT.Proto.Metrics.resource_metrics Batch.t;
    }

    type t = {
      batch: batches;
      config: Config.t;
    }

    let make (config : Config.t) () =
      let timeout =
        if config.batch_timeout_ms > 0 then
          Some Mtime.Span.(config.batch_timeout_ms * ms)
        else
          None
      in
      {
        config;
        batch =
          {
            traces = Batch.make ?batch:config.batch_traces ?timeout ();
            metrics = Batch.make ?batch:config.batch_logs ?timeout ();
            logs = Batch.make ?batch:config.batch_metrics ?timeout ();
          };
      }
  end

  module Action = struct
    type t =
      | Start
      | Wait
      | Send of {
          url: string;
          data: string;
        }
      | End

    let send ~url data = Send { url; data }
  end

  module Event = struct
    type t =
      | Stop
      | Ready
      | Send_error
      | Batch of Signal.t

    let stop = Stop

    let ready = Ready

    let error = Send_error

    let batch s = Batch s
  end

  module Conv = Signal.Converter

  let send_metrics ?force now (s : State.t) =
    match Batch.pop_if_ready ?force ~now s.batch.metrics with
    | None -> Seq.empty
    | Some ms ->
      drain_gc_metrics () @ ms
      |> Conv.metrics
      |> Action.send ~url:s.config.url_metrics
      |> Seq.return

  let send_traces ?force now (s : State.t) =
    match Batch.pop_if_ready ?force ~now s.batch.traces with
    | None -> Seq.empty
    | Some ts ->
      Conv.traces ts |> Action.send ~url:s.config.url_traces |> Seq.return

  let send_logs ?force now (s : State.t) =
    match Batch.pop_if_ready ?force ~now s.batch.logs with
    | None -> Seq.empty
    | Some ls ->
      Conv.logs ls |> Action.send ~url:s.config.url_logs |> Seq.return

  let send ?force (s : State.t) =
    let now = Mtime_clock.now () in
    send_logs ?force now s
    |> Seq.append (send_traces ?force now s)
    |> Seq.append (send_metrics ?force now s)

  let batch (s : State.t) : Signal.t -> Action.t Seq.t =
   fun signal ->
    let now = Mtime_clock.now () in
    let dropped, action =
      match signal with
      | Metrics ms -> Batch.push s.batch.metrics ms, send_metrics now s
      | Traces ts -> Batch.push s.batch.traces ts, send_traces now s
      | Logs ls -> Batch.push s.batch.logs ls, send_logs now s
    in
    let () =
      match dropped with
      | `Dropped -> incr_dropped ()
      | `Ok -> ()
    in
    action

  let send_error (_ : State.t) =
    incr_errors ();
    Seq.return Action.Wait

  let stop (t : State.t) =
    Seq.append (send ~force:true t) (Seq.return Action.End)

  let update (t : State.t) : Event.t -> Action.t Seq.t = function
    | Ready -> send t
    | Batch signal -> batch t signal
    | Send_error -> send_error t
    | Stop -> stop t

  (* let run config : (Action.t -> Event.t) -> (Action.t * Event.t) Seq.t = *)
  (*  fun delta -> *)
  (*   let t = State.make config () in *)
  (*   let rec loop action = *)
  (*     let event = delta action in *)
  (*     match update t event with *)
  (*     | None -> Seq.empty *)
  (*     | Some actions -> Seq.cons (action, event) (Seq.flat_map loop actions) *)
  (*   in *)
  (*   loop Start *)
end
