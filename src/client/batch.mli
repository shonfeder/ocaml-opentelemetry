(** A thread-safe batch of resources to be popper when ready . *)

type 'a t

val push : 'a t -> 'a list -> [ `Dropped | `Ok ]

val pop_if_ready : ?force:bool -> now:Mtime.t -> 'a t -> 'a list option
(** Is the batch ready to be emitted? If batching is disabled, this is true as
    soon as {!is_empty} is false. If a timeout is provided for this batch, then
    it will be ready if an element has been in it for at least the timeout.
    @param now passed to implement timeout *)

val make : ?batch:int -> ?timeout:Mtime.span -> unit -> 'a t
(** Create a new batch *)
