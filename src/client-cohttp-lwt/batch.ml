type 'a t = {
  mutable size: int;
  mutable q: 'a list;
  batch: int option;
  high_watermark: int;
  timeout: Mtime.span option;
  mutable start: Mtime.t;
}

let make ?batch ?timeout () : _ t =
  Option.iter (fun b -> assert (b > 0)) batch;
  let high_watermark = Option.fold ~none:100 ~some:(fun x -> x * 10) batch in
  {
    size = 0;
    start = Mtime_clock.now ();
    q = [];
    batch;
    timeout;
    high_watermark;
  }

let timeout_expired_ ~now self : bool =
  match self.timeout with
  | Some t ->
    let elapsed = Mtime.span now self.start in
    Mtime.Span.compare elapsed t >= 0
  | None -> false

let is_full_ self : bool =
  match self.batch with
  | None -> self.size > 0
  | Some b -> self.size >= b

let ready_to_pop ~force ~now self =
  self.size > 0 && (force || is_full_ self || timeout_expired_ ~now self)

let pop_if_ready ?(force = false) ~now (self : _ t) : _ list option =
  if ready_to_pop ~force ~now self then (
    let l = self.q in
    self.q <- [];
    self.size <- 0;
    assert (l <> []);
    Some l
  ) else
    None

let push (self : _ t) x : [ `Dropped | `Ok ] =
  if self.size >= self.high_watermark then
    (* drop this to prevent queue from growing too fast *)
    `Dropped
  else (
    if self.size = 0 && Option.is_some self.timeout then
      (* current batch starts now *)
      self.start <- Mtime_clock.now ();

    (* add to queue *)
    self.size <- 1 + self.size;
    self.q <- x :: self.q;
    let ready = is_full_ self in
    `Ok
  )
