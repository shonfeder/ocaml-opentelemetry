(*
   https://github.com/open-telemetry/oteps/blob/main/text/0035-opentelemetry-protocol.md
   https://github.com/open-telemetry/oteps/blob/main/text/0099-otlp-http.md
 *)

module OT = Opentelemetry
module Config = Config
module Client = Opentelemetry_client
module Signal = Client.Signal
open Opentelemetry
open Common_

let set_headers = Config.Env.set_headers

let get_headers = Config.Env.get_headers

module State = Opentelemetry_client.State.Make (Config.Env)
module Sender = Signal.Sender (Config.Env) (State)

external reraise : exn -> 'a = "%reraise"
(** This is equivalent to [Lwt.reraise]. We inline it here so we don't force to
    use Lwt's latest version *)

type error =
  [ `Status of int * Opentelemetry.Proto.Status.status
  | `Failure of string
  | `Sysbreak
  ]

let report_err_ = function
  | `Sysbreak -> Printf.eprintf "opentelemetry: ctrl-c captured, stopping\n%!"
  | `Failure msg ->
    Format.eprintf "@[<2>opentelemetry: export failed: %s@]@." msg
  | `Status (code, { Opentelemetry.Proto.Status.code = scode; message; details })
    ->
    let pp_details out l =
      List.iter
        (fun s -> Format.fprintf out "%S;@ " (Bytes.unsafe_to_string s))
        l
    in
    Format.eprintf
      "@[<2>opentelemetry: export failed with@ http code=%d@ status \
       {@[code=%ld;@ message=%S;@ details=[@[%a@]]@]}@]@."
      code scode
      (Bytes.unsafe_to_string message)
      pp_details details

module Httpc : sig
  type t

  val create : unit -> t

  val send :
    t ->
    url:string ->
    decode:[ `Dec of Pbrt.Decoder.t -> 'a | `Ret of 'a ] ->
    string ->
    ('a, error) result Lwt.t

  val cleanup : t -> unit
end = struct
  open Opentelemetry.Proto
  open Lwt.Syntax
  module Httpc = Cohttp_lwt_unix.Client

  type t = unit

  let create () : t = ()

  let cleanup _self = ()

  (* send the content to the remote endpoint/path *)
  let send (_self : t) ~url ~decode (bod : string) : ('a, error) result Lwt.t =
    let uri = Uri.of_string url in

    let open Cohttp in
    let headers = Header.(add_list (init ()) (Config.Env.get_headers ())) in
    let headers =
      Header.(add headers "Content-Type" "application/x-protobuf")
    in

    let body = Cohttp_lwt.Body.of_string bod in

    let* r =
      try%lwt
        let+ r = Httpc.post ~headers ~body uri in
        Ok r
      with e -> Lwt.return @@ Error e
    in
    match r with
    | Error e ->
      let err =
        `Failure
          (spf "sending signals via http POST to %S\nfailed with:\n%s" url
             (Printexc.to_string e))
      in
      Lwt.return @@ Error err
    | Ok (resp, body) ->
      let* body = Cohttp_lwt.Body.to_string body in
      let code = Response.status resp |> Code.code_of_status in
      if not (Code.is_error code) then (
        match decode with
        | `Ret x -> Lwt.return @@ Ok x
        | `Dec f ->
          let dec = Pbrt.Decoder.of_string body in
          let r =
            try Ok (f dec)
            with e ->
              let bt = Printexc.get_backtrace () in
              Error
                (`Failure
                   (spf "decoding failed with:\n%s\n%s" (Printexc.to_string e)
                      bt))
          in
          Lwt.return r
      ) else (
        let dec = Pbrt.Decoder.of_string body in

        let r =
          try
            let status = Status.decode_pb_status dec in
            Error (`Status (code, status))
          with e ->
            let bt = Printexc.get_backtrace () in
            Error
              (`Failure
                 (spf
                    "httpc: decoding of status (url=%S, code=%d) failed with:\n\
                     %s\n\
                     status: %S\n\
                     %s"
                    url code (Printexc.to_string e) body bt))
        in
        Lwt.return r
      )
end

(* make an Emitter.

   exceptions inside should be caught, see
   https://opentelemetry.io/docs/reference/specification/error-handling/ *)
module Emitter (Arg : sig
  val stop : bool Atomic.t

  val config : Config.t
end) : Signal.EMITTER = struct
  open Proto
  open Lwt.Syntax
  module Conv = Signal.Converter

  let lock = Some Lock.with_lock

  (* local helpers *)
  open struct
    let httpc = Httpc.create ()

    let timeout =
      if Arg.config.batch_timeout_ms > 0 then
        Some Mtime.Span.(Arg.config.batch_timeout_ms * ms)
      else
        None

    let batch_traces : Trace.resource_spans list Batch.t =
      Batch.make ?batch:Arg.config.batch_traces ?timeout ()

    let batch_metrics : Metrics.resource_metrics list Batch.t =
      Batch.make ?batch:Arg.config.batch_metrics ?timeout ()

    let batch_logs : Logs.resource_logs list Batch.t =
      Batch.make ?batch:Arg.config.batch_logs ?timeout ()

    let send_http_ ~url data : unit Lwt.t =
      let* r = Httpc.send httpc ~url ~decode:(`Ret ()) data in
      match r with
      | Ok () -> Lwt.return ()
      | Error `Sysbreak ->
        Printf.eprintf "ctrl-c captured, stopping\n%!";
        Atomic.set Arg.stop true;
        Lwt.return ()
      | Error err ->
        (* TODO: log error _via_ otel? *)
        State.incr_errors ();
        report_err_ err;
        (* avoid crazy error loop *)
        Lwt_unix.sleep 3.

    let send_metrics_http (l : Metrics.resource_metrics list) =
      Conv.metrics l |> send_http_ ~url:Arg.config.url_metrics

    let send_traces_http (l : Trace.resource_spans list) =
      Conv.traces l |> send_http_ ~url:Arg.config.url_traces

    let send_logs_http (l : Logs.resource_logs list) =
      Conv.logs l |> send_http_ ~url:Arg.config.url_logs

    let maybe_pop ?force ~now batch =
      Batch.pop_if_ready ?force ~now batch
      |> Option.map (List.fold_left (fun acc l -> List.rev_append l acc) [])

    (* emit metrics, if the batch is full or timeout lapsed *)
    let emit_metrics_maybe ?force now : bool Lwt.t =
      match maybe_pop ?force ~now batch_metrics with
      | None -> Lwt.return false
      | Some l ->
        let batch = State.drain_gc_metrics () @ l in
        let+ () = send_metrics_http batch in
        true

    let emit_traces_maybe ?force now : bool Lwt.t =
      match maybe_pop ?force ~now batch_traces with
      | None -> Lwt.return false
      | Some l ->
        let+ () = send_traces_http l in
        true

    let emit_logs_maybe ?force now : bool Lwt.t =
      match maybe_pop ?force ~now batch_logs with
      | None -> Lwt.return false
      | Some l ->
        let+ () = send_logs_http l in
        true

    let[@inline] guard_exn_ where f =
      try f ()
      with e ->
        let bt = Printexc.get_backtrace () in
        Printf.eprintf
          "opentelemetry-curl: uncaught exception in %s: %s\n%s\n%!" where
          (Printexc.to_string e) bt

    let emit_all_force () : unit Lwt.t =
      let now = Mtime_clock.now () in
      let+ (_ : bool) = emit_traces_maybe ~force:true now
      and+ (_ : bool) = emit_logs_maybe ~force:true now
      and+ (_ : bool) = emit_metrics_maybe ~force:true now in
      ()

    (* thread that calls [tick()] regularly, to help enforce timeouts *)
    let setup_ticker_thread ~tick ~finally () =
      let rec tick_thread () =
        if Atomic.get Arg.stop then (
          finally ();
          Lwt.return ()
        ) else
          let* () = Lwt_unix.sleep 0.5 in
          let* () = tick () in
          tick_thread ()
      in
      Lwt.async tick_thread
  end

  (* we make sure that this is thread-safe, even though we don't have a
       background thread. There can still be a ticker thread, and there
       can also be several user threads that produce spans and call
       the emit functions. *)

  let report_dropped = function
    | `Dropped -> State.incr_dropped ()
    | `Ok -> ()

  let push_trace e =
    let@ () = guard_exn_ "push trace" in
    Batch.push batch_traces e |> report_dropped;
    let now = Mtime_clock.now () in
    Lwt.async (fun () ->
        let+ (_ : bool) = emit_traces_maybe now in
        ())

  let push_metrics e =
    let@ () = guard_exn_ "push metrics" in
    State.sample_gc_metrics_if_needed ();
    Batch.push batch_metrics e |> report_dropped;
    let now = Mtime_clock.now () in
    Lwt.async (fun () ->
        let+ (_ : bool) = emit_metrics_maybe now in
        ())

  let push_logs e =
    let@ () = guard_exn_ "push logs" in
    Batch.push batch_logs e |> report_dropped;
    let now = Mtime_clock.now () in
    Lwt.async (fun () ->
        let+ (_ : bool) = emit_logs_maybe now in
        ())

  let tick_ =
    (* TODO: Can we get tick on the outside? *)
    State.Tick.tick @@ fun () ->
    let now = Mtime_clock.now () in
    let+ (_ : bool) = emit_traces_maybe now
    and+ (_ : bool) = emit_logs_maybe now
    and+ (_ : bool) = emit_metrics_maybe now in
    ()

  let set_on_tick_callbacks = State.Tick.set_on_tick_callbacks

  let () = setup_ticker_thread ~tick:tick_ ~finally:ignore ()

  (* if called in a blocking context: work in the background *)
  let tick () = Lwt.async tick_

  let cleanup ~on_done () =
    if Config.Env.get_debug () then Printf.eprintf "opentelemetry: exiting…\n%!";
    Lwt.async (fun () ->
        let* () = emit_all_force () in
        Httpc.cleanup httpc;
        on_done ();
        Lwt.return ())
end

let create_backend ?(stop = Atomic.make false) ?(config = Config.make ()) () :
    (module Collector.BACKEND) =
  (module Client.Backend.Make
            (Sender)
            (Emitter (struct
              let stop = stop

              let config = config
            end)))

let setup_ ?stop ?config () : unit =
  let backend = create_backend ?stop ?config () in
  OT.Collector.set_backend backend;
  ()

let setup ?stop ?config ?(enable = true) () =
  if enable then setup_ ?stop ?config ()

let remove_backend () : unit Lwt.t =
  let done_fut, done_u = Lwt.wait () in
  OT.Collector.remove_backend ~on_done:(fun () -> Lwt.wakeup_later done_u ()) ();
  done_fut

let with_setup ?stop ?(config = Config.make ()) ?(enable = true) () f : _ Lwt.t
    =
  if enable then (
    let open Lwt.Syntax in
    setup_ ?stop ~config ();

    Lwt.catch
      (fun () ->
        let* res = f () in
        let+ () = remove_backend () in
        res)
      (fun exn ->
        let* () = remove_backend () in
        reraise exn)
  ) else
    f ()
