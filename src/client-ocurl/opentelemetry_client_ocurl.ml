(*
   https://github.com/open-telemetry/oteps/blob/main/text/0035-opentelemetry-protocol.md
   https://github.com/open-telemetry/oteps/blob/main/text/0099-otlp-http.md
 *)

module OT = Opentelemetry
module Config = Config
module Client = Opentelemetry_client
module Self_trace = Client.Self_trace
module Signal = Client.Signal
module Batch = Client.Batch
open Opentelemetry
include Common_

let get_headers = Config.Env.get_headers

let set_headers = Config.Env.set_headers

module State = Opentelemetry_client.State.Make (Config.Env)

(** Something sent to the collector *)
module Event = struct
  open Opentelemetry.Proto

  type t =
    | E_metric of Metrics.resource_metrics list
    | E_trace of Trace.resource_spans list
    | E_logs of Logs.resource_logs list
    | E_tick
    | E_flush_all  (** Flush all batches *)
end

(** Something to be sent via HTTP *)
module To_send = struct
  open Opentelemetry.Proto

  type t =
    | Send_metric of Metrics.resource_metrics list
    | Send_trace of Trace.resource_spans list
    | Send_logs of Logs.resource_logs list
end

(** start a thread in the background, running [f()] *)
let start_bg_thread (f : unit -> unit) : Thread.t =
  let unix_run () =
    let signals =
      [
        Sys.sigusr1;
        Sys.sigusr2;
        Sys.sigterm;
        Sys.sigpipe;
        Sys.sigalrm;
        Sys.sigstop;
      ]
    in
    ignore (Thread.sigmask Unix.SIG_BLOCK signals : _ list);
    f ()
  in
  (* no signals on Windows *)
  let run () =
    if Sys.win32 then
      f ()
    else
      unix_run ()
  in
  Thread.create run ()

let str_to_hex (s : string) : string =
  let i_to_hex (i : int) =
    if i < 10 then
      Char.chr (i + Char.code '0')
    else
      Char.chr (i - 10 + Char.code 'a')
  in

  let res = Bytes.create (2 * String.length s) in
  for i = 0 to String.length s - 1 do
    let n = Char.code (String.get s i) in
    Bytes.set res (2 * i) (i_to_hex ((n land 0xf0) lsr 4));
    Bytes.set res ((2 * i) + 1) (i_to_hex (n land 0x0f))
  done;
  Bytes.unsafe_to_string res

module Emitter (Arg : sig
  val stop : bool Atomic.t

  val config : Config.t
end) : Signal.EMITTER = struct
  open Opentelemetry.Proto

  let lock = None

  let timeout =
    if Arg.config.common.batch_timeout_ms > 0 then
      Some Mtime.Span.(Arg.config.common.batch_timeout_ms * ms)
    else
      None

  type t = {
    cleaned: bool Atomic.t;  (** True when we cleaned up after closing *)
    q: Event.t B_queue.t;  (** Queue to receive data from the user's code *)
    mutable main_th: Thread.t option;  (** Thread that listens on [q] *)
    send_q: To_send.t B_queue.t;  (** Queue for the send worker threads *)
    mutable send_threads: Thread.t array;  (** Threads that send data via http *)
  }

  let send_http_ (client : Curl.t) ~url data : unit =
    let@ _sc =
      Self_trace.with_ ~kind:Span.Span_kind_producer "otel-ocurl.send-http"
    in

    if Config.Env.get_debug () then
      Printf.eprintf "opentelemetry: send http POST to %s (%dB)\n%!" url
        (String.length data);
    let headers =
      ("Content-Type", "application/x-protobuf") :: Arg.config.common.headers
    in
    match
      let@ _sc =
        Self_trace.with_ ~kind:Span.Span_kind_internal "curl.post"
          ~attrs:[ "sz", `Int (String.length data); "url", `String url ]
      in
      Ezcurl.post ~headers ~client ~params:[] ~url ~content:(`String data) ()
    with
    | Ok { code; _ } when code >= 200 && code < 300 ->
      if Config.Env.get_debug () then
        Printf.eprintf "opentelemetry: got response code=%d\n%!" code
    | Ok { code; body; headers = _; info = _ } ->
      State.incr_errors ();
      Self_trace.add_event _sc
      @@ Opentelemetry.Event.make "error" ~attrs:[ "code", `Int code ];

      if Config.Env.get_debug () then (
        let dec = Pbrt.Decoder.of_string body in
        let body =
          try
            let status = Status.decode_pb_status dec in
            Format.asprintf "%a" Status.pp_status status
          with _ ->
            spf "(could not decode status)\nraw bytes: %s" (str_to_hex body)
        in
        Printf.eprintf
          "opentelemetry: error while sending data to %s:\n  code=%d\n  %s\n%!"
          url code body
      );
      ()
    | exception Sys.Break ->
      Printf.eprintf "ctrl-c captured, stopping\n%!";
      Atomic.set Arg.stop true
    | Error (code, msg) ->
      (* TODO: log error _via_ otel? *)
      State.incr_errors ();

      Printf.eprintf
        "opentelemetry: export failed:\n  %s\n  curl code: %s\n  url: %s\n%!"
        msg (Curl.strerror code) url;

      (* avoid crazy error loop *)
      Thread.delay 3.

  (* Thread that, in a loop, reads from [q] to get the next message to send via
      http *)
  let bg_thread_loop (self : t) : unit =
    Ezcurl.with_client ?set_opts:None @@ fun client ->
    let config = Arg.config in
    let send ~name ~url ~conv signals =
      let@ _sp =
        Self_trace.with_ ~kind:Span_kind_producer name
          ~attrs:[ "n", `Int (List.length signals) ]
      in
      conv signals |> send_http_ ~url client
    in
    let module Conv = Signal.Converter in
    try
      while not (Atomic.get Arg.stop) do
        let msg = B_queue.pop self.send_q in
        match msg with
        | To_send.Send_trace tr ->
          send ~name:"send-traces" ~conv:Conv.traces
            ~url:config.common.url_traces tr
        | To_send.Send_metric ms ->
          send ~name:"send-metrics" ~conv:Conv.metrics
            ~url:config.common.url_metrics ms
        | To_send.Send_logs logs ->
          send ~name:"send-logs" ~conv:Conv.logs ~url:config.common.url_logs
            logs
      done
    with B_queue.Closed -> ()

  type batches = {
    traces: Proto.Trace.resource_spans Batch.t;
    logs: Proto.Logs.resource_logs Batch.t;
    metrics: Proto.Metrics.resource_metrics Batch.t;
  }

  let main_thread_loop (self : t) : unit =
    let local_q = Queue.create () in

    (* keep track of batches *)
    let batches =
      {
        traces = Batch.make ?batch:Arg.config.common.batch_traces ?timeout ();
        metrics = Batch.make ?batch:Arg.config.common.batch_logs ?timeout ();
        logs = Batch.make ?batch:Arg.config.common.batch_metrics ?timeout ();
      }
    in

    (* emit metrics, if the batch is full or timeout lapsed *)
    let emit_metrics_maybe ?force now : bool =
      match Batch.pop_if_ready ?force ~now batches.metrics with
      | None -> false
      | Some l ->
        let metrics = State.drain_gc_metrics () @ l in
        B_queue.push self.send_q (To_send.Send_metric metrics);
        true
    in

    let emit_traces_maybe ?force now : bool =
      match Batch.pop_if_ready ?force ~now batches.traces with
      | None -> false
      | Some l ->
        B_queue.push self.send_q (To_send.Send_trace l);
        true
    in

    let emit_logs_maybe ?force now : bool =
      match Batch.pop_if_ready ?force ~now batches.logs with
      | None -> false
      | Some l ->
        B_queue.push self.send_q (To_send.Send_logs l);
        true
    in

    try
      while not (Atomic.get Arg.stop) do
        (* read multiple events at once *)
        B_queue.pop_all self.q local_q;

        (* are we asked to flush all events? *)
        let must_flush_all = ref false in

        (* how to process a single event *)
        let process_ev (ev : Event.t) : unit =
          match ev with
          | Event.E_metric m -> ignore (Batch.push batches.metrics m)
          | Event.E_trace tr -> ignore (Batch.push batches.traces tr)
          | Event.E_logs logs -> ignore (Batch.push batches.logs logs)
          | Event.E_tick ->
            (* the only impact of "tick" is that it wakes us up regularly *)
            ()
          | Event.E_flush_all -> must_flush_all := true
        in

        Queue.iter process_ev local_q;
        Queue.clear local_q;

        let now = Mtime_clock.now () in
        let force = !must_flush_all in
        ignore (emit_metrics_maybe ~force now);
        ignore (emit_traces_maybe ~force now);
        ignore (emit_logs_maybe ~force now)
      done
    with B_queue.Closed -> ()

  let backend : t =
    let n_send_threads = max 2 Arg.config.bg_threads in
    let self =
      {
        q = B_queue.create ();
        send_threads = [||];
        send_q = B_queue.create ();
        cleaned = Atomic.make false;
        main_th = None;
      }
    in

    let main_th = start_bg_thread (fun () -> main_thread_loop self) in
    self.main_th <- Some main_th;

    self.send_threads <-
      Array.init n_send_threads (fun _i ->
          start_bg_thread (fun () -> bg_thread_loop self));

    self

  let[@inline] send_event ev : unit = B_queue.push backend.q ev

  let cleanup ~on_done () : unit =
    Atomic.set Arg.stop true;
    if not (Atomic.exchange backend.cleaned true) then (
      (* empty batches *)
      send_event Event.E_flush_all;
      (* close the incoming queue, wait for the thread to finish
         before we start cutting off the background threads, so that they
         have time to receive the final batches *)
      B_queue.close backend.q;
      Option.iter Thread.join backend.main_th;
      (* close send queues, then wait for all threads *)
      B_queue.close backend.send_q;
      Array.iter Thread.join backend.send_threads
    );
    on_done ()

  let push_trace l = send_event (Event.E_trace l)

  let push_metrics m = send_event (Event.E_metric m)

  let push_logs l = send_event (Event.E_logs l)

  let tick = State.Tick.tick @@ fun () -> send_event Event.E_tick
end

let create_backend ?(stop = Atomic.make false)
    ?(config : Config.t = Config.make ()) () : (module Collector.BACKEND) =
  (module Client.Backend.Make (Config.Env) (State)
            (Emitter (struct
              let stop = stop

              let config = config
            end)))

(** thread that calls [tick()] regularly, to help enforce timeouts *)
let setup_ticker_thread ~stop ~sleep_ms (module B : Collector.BACKEND) () =
  let sleep_s = float sleep_ms /. 1000. in
  let tick_loop () =
    try
      while not @@ Atomic.get stop do
        Thread.delay sleep_s;
        B.tick ()
      done
    with B_queue.Closed -> ()
  in
  start_bg_thread tick_loop

let setup_ ?(stop = Atomic.make false) ?(config : Config.t = Config.make ()) ()
    : unit =
  let backend = create_backend ~stop ~config () in
  Opentelemetry.Collector.set_backend backend;

  Self_trace.set_enabled config.common.self_trace;

  if config.ticker_thread then (
    (* at most a minute *)
    let sleep_ms = min 60_000 (max 2 config.ticker_interval_ms) in
    ignore (setup_ticker_thread ~stop ~sleep_ms backend () : Thread.t)
  )

let remove_backend () : unit =
  (* we don't need the callback, this runs in the same thread *)
  OT.Collector.remove_backend () ~on_done:ignore

let setup ?stop ?config ?(enable = true) () =
  if enable then setup_ ?stop ?config ()

let with_setup ?stop ?config ?(enable = true) () f =
  if enable then (
    setup_ ?stop ?config ();
    Fun.protect ~finally:remove_backend f
  ) else
    f ()
