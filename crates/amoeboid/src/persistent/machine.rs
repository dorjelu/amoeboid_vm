//! The persistent actor's mode machine, pure: one input in, actions out. The actor executes
//! the actions; nothing here sends, persists or sleeps.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use super::{Behaviour, Effect, Unrecoverable};
use crate::journal::{BatchId, EventPayload, PersistFailure, Seq, StoreError, StoredEvent};
use crate::nonempty::NonEmpty;

/// ```text
/// Recovering ─Replayed─▶ (fold on_event) ─▶ on_recovered ─▶ Reconciling ─reconciled()─▶ Ready
///      │ undecodable / unknown version ─▶ Unrecoverable (terminal)
///      │ store error ─▶ Stop: supervisor restarts with backoff
/// Reconciling / Ready ─Effect::Persist─▶ Persisting ─Persisted─▶ back to where it came from
///                                                   (failure, timeout ─▶ Stop)
/// ```
/// Commands wait in `stash` until `Ready`; everything arriving while `Persisting` waits in
/// `held`, in order, and is re-fed once the batch is durable. Both are unbounded.
pub enum Mode<B: Behaviour> {
    Recovering { stash: VecDeque<B::Command> },
    Reconciling { stash: VecDeque<B::Command> },
    Ready,
    Persisting(InFlight<B>),
    Unrecoverable { reason: Unrecoverable },
}

/// A batch sent to the journal and not yet answered.
pub struct InFlight<B: Behaviour> {
    pub batch: BatchId,
    pub events: NonEmpty<B::Event>,
    pub then: Vec<B::Out>,
    pub resume: Resume<B>,
    /// Commands and observations that arrived meanwhile, in order.
    pub held: VecDeque<Input<B>>,
}

/// Where `Persisting` returns once the batch is durable.
pub enum Resume<B: Behaviour> {
    Reconciling { stash: VecDeque<B::Command> },
    Ready,
}

pub enum Input<B: Behaviour> {
    Command(B::Command),
    Observation(B::Observation),
    Replayed(Vec<StoredEvent>),
    ReplayFailed(StoreError),
    Persisted {
        batch: BatchId,
        last: Seq,
    },
    PersistFailed {
        batch: BatchId,
        reason: PersistFailure,
    },
    PersistTimeout(BatchId),
}

pub enum Action<B: Behaviour> {
    Persist {
        batch: BatchId,
        first: Seq,
        events: NonEmpty<EventPayload>,
    },
    /// Deliver `PersistTimeout(batch)` after the configured timeout.
    ArmTimeout(BatchId),
    Send(B::Out),
    /// A logged transition.
    Entered(ModeName),
    /// An input that no longer applies (stale batch, observation before recovery).
    Ignored(&'static str),
    /// The actor stops with an error; its supervisor restarts it and replay rebuilds it.
    Stop(Fatal),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ModeName {
    Reconciling,
    Ready,
    Unrecoverable,
}

#[derive(Debug, thiserror::Error)]
pub enum Fatal {
    #[error("can't replay: {0}")]
    Replay(StoreError),
    #[error("can't persist: {0}")]
    Persist(PersistFailure),
    #[error("persisting a batch timed out")]
    PersistTimeout,
    #[error("can't encode an event: {0}")]
    Encode(String),
    #[error("stream position overflow")]
    SeqOverflow,
}

pub struct Core<B: Behaviour> {
    pub state: B::State,
    /// Last durable position; `None` for an empty stream.
    pub last: Option<Seq>,
    pub next_batch: BatchId,
    pub mode: Mode<B>,
}

impl<B: Behaviour> Core<B> {
    #[must_use]
    pub fn recovering() -> Self {
        Self {
            state: B::initial(),
            last: None,
            next_batch: BatchId::FIRST,
            mode: Mode::Recovering {
                stash: VecDeque::new(),
            },
        }
    }
}

/// Persisted form of every event; `v` is [`Behaviour::EVENT_VERSION`] at write time.
#[derive(Serialize)]
struct Envelope<'a, E> {
    v: u16,
    event: &'a E,
}

/// Read in two steps so an unknown version is reported as such, not as a decode error.
#[derive(Deserialize)]
struct RawEnvelope {
    v: u16,
    event: serde_json::Value,
}

/// Applies `input` and everything it releases (stash, held inputs), in order. Stops at the
/// first `Stop`.
pub fn step<B: Behaviour>(core: Core<B>, input: Input<B>) -> (Core<B>, Vec<Action<B>>) {
    let mut actions = Vec::new();
    let mut queue = VecDeque::from([input]);
    let mut core = core;
    while let Some(input) = queue.pop_front() {
        core = transition(core, input, &mut actions, &mut queue);
        if matches!(actions.last(), Some(Action::Stop(_))) {
            break;
        }
    }
    (core, actions)
}

/// The core without its mode: what every transition carries over.
struct Parts<B: Behaviour> {
    state: B::State,
    last: Option<Seq>,
    next_batch: BatchId,
}

impl<B: Behaviour> Parts<B> {
    fn with(self, mode: Mode<B>) -> Core<B> {
        Core {
            state: self.state,
            last: self.last,
            next_batch: self.next_batch,
            mode,
        }
    }
}

type Actions<'a, B> = &'a mut Vec<Action<B>>;
type Queue<'a, B> = &'a mut VecDeque<Input<B>>;

fn transition<B: Behaviour>(
    core: Core<B>,
    input: Input<B>,
    out: Actions<B>,
    queue: Queue<B>,
) -> Core<B> {
    let parts = Parts {
        state: core.state,
        last: core.last,
        next_batch: core.next_batch,
    };
    match core.mode {
        Mode::Recovering { stash } => recovering(parts, stash, input, out, queue),
        Mode::Reconciling { stash } => reconciling(parts, stash, input, out, queue),
        Mode::Ready => ready(parts, input, out, queue),
        Mode::Persisting(flight) => persisting(parts, flight, input, out, queue),
        Mode::Unrecoverable { reason } => unrecoverable(parts, reason, input, out),
    }
}

fn recovering<B: Behaviour>(
    parts: Parts<B>,
    mut stash: VecDeque<B::Command>,
    input: Input<B>,
    out: Actions<B>,
    queue: Queue<B>,
) -> Core<B> {
    match input {
        Input::Command(command) => {
            stash.push_back(command);
            parts.with(Mode::Recovering { stash })
        }
        Input::Replayed(events) => match decode::<B>(&events) {
            Err(reason) => {
                out.push(Action::Entered(ModeName::Unrecoverable));
                out.push(Action::Send(B::on_unrecoverable(&reason)));
                out.extend(
                    stash
                        .into_iter()
                        .filter_map(|c| B::refuse(c, &reason))
                        .map(Action::Send),
                );
                parts.with(Mode::Unrecoverable { reason })
            }
            Ok(decoded) => {
                let parts = Parts {
                    state: decoded.iter().fold(parts.state, B::on_event),
                    last: events.last().map(|e| e.seq),
                    next_batch: parts.next_batch,
                };
                out.push(Action::Entered(ModeName::Reconciling));
                let effect = B::on_recovered(&parts.state);
                apply(parts, Resume::Reconciling { stash }, effect, out, queue)
            }
        },
        Input::ReplayFailed(error) => {
            out.push(Action::Stop(Fatal::Replay(error)));
            parts.with(Mode::Recovering { stash })
        }
        Input::Observation(_) => {
            // Watchers are spawned by on_recovered; anything earlier is a previous incarnation's.
            out.push(Action::Ignored("observation before recovery"));
            parts.with(Mode::Recovering { stash })
        }
        Input::Persisted { .. } | Input::PersistFailed { .. } | Input::PersistTimeout(_) => {
            out.push(Action::Ignored("persist reply before recovery"));
            parts.with(Mode::Recovering { stash })
        }
    }
}

fn reconciling<B: Behaviour>(
    parts: Parts<B>,
    mut stash: VecDeque<B::Command>,
    input: Input<B>,
    out: Actions<B>,
    queue: Queue<B>,
) -> Core<B> {
    match input {
        Input::Command(command) => {
            stash.push_back(command);
            parts.with(Mode::Reconciling { stash })
        }
        Input::Observation(observation) => {
            let effect = B::on_observation(&parts.state, observation);
            apply(parts, Resume::Reconciling { stash }, effect, out, queue)
        }
        Input::Replayed(_)
        | Input::ReplayFailed(_)
        | Input::Persisted { .. }
        | Input::PersistFailed { .. }
        | Input::PersistTimeout(_) => ignore_stale(parts, Mode::Reconciling { stash }, out),
    }
}

fn ready<B: Behaviour>(
    parts: Parts<B>,
    input: Input<B>,
    out: Actions<B>,
    queue: Queue<B>,
) -> Core<B> {
    match input {
        Input::Command(command) => {
            let effect = B::on_command(&parts.state, command);
            apply(parts, Resume::Ready, effect, out, queue)
        }
        Input::Observation(observation) => {
            let effect = B::on_observation(&parts.state, observation);
            apply(parts, Resume::Ready, effect, out, queue)
        }
        Input::Replayed(_)
        | Input::ReplayFailed(_)
        | Input::Persisted { .. }
        | Input::PersistFailed { .. }
        | Input::PersistTimeout(_) => ignore_stale(parts, Mode::Ready, out),
    }
}

fn persisting<B: Behaviour>(
    parts: Parts<B>,
    mut flight: InFlight<B>,
    input: Input<B>,
    out: Actions<B>,
    queue: Queue<B>,
) -> Core<B> {
    match input {
        input @ (Input::Command(_) | Input::Observation(_)) => {
            flight.held.push_back(input);
            parts.with(Mode::Persisting(flight))
        }
        Input::Persisted { batch, last } if batch == flight.batch => {
            let state = flight.events.iter().fold(parts.state, B::on_event);
            out.extend(flight.then.into_iter().map(Action::Send));
            let mode = settle(&state, flight.resume, out, queue);
            queue.extend(flight.held);
            Core {
                state,
                last: Some(last),
                next_batch: parts.next_batch,
                mode,
            }
        }
        Input::PersistFailed { batch, reason } if batch == flight.batch => {
            out.push(Action::Stop(Fatal::Persist(reason)));
            parts.with(Mode::Persisting(flight))
        }
        Input::PersistTimeout(batch) if batch == flight.batch => {
            out.push(Action::Stop(Fatal::PersistTimeout));
            parts.with(Mode::Persisting(flight))
        }
        Input::Replayed(_)
        | Input::ReplayFailed(_)
        | Input::Persisted { .. }
        | Input::PersistFailed { .. }
        | Input::PersistTimeout(_) => ignore_stale(parts, Mode::Persisting(flight), out),
    }
}

fn unrecoverable<B: Behaviour>(
    parts: Parts<B>,
    reason: Unrecoverable,
    input: Input<B>,
    out: Actions<B>,
) -> Core<B> {
    match input {
        Input::Command(command) => {
            out.extend(B::refuse(command, &reason).map(Action::Send));
        }
        Input::Observation(_) => out.push(Action::Ignored("observation while unrecoverable")),
        Input::Replayed(_)
        | Input::ReplayFailed(_)
        | Input::Persisted { .. }
        | Input::PersistFailed { .. }
        | Input::PersistTimeout(_) => {
            out.push(Action::Ignored("journal input while unrecoverable"));
        }
    }
    parts.with(Mode::Unrecoverable { reason })
}

/// A journal reply or timeout for a batch that is no longer in flight.
fn ignore_stale<B: Behaviour>(parts: Parts<B>, mode: Mode<B>, out: Actions<B>) -> Core<B> {
    out.push(Action::Ignored("stale journal reply or timeout"));
    parts.with(mode)
}

/// Runs an effect from `Reconciling` or `Ready`.
fn apply<B: Behaviour>(
    parts: Parts<B>,
    resume: Resume<B>,
    effect: Effect<B::Event, B::Out>,
    out: Actions<B>,
    queue: Queue<B>,
) -> Core<B> {
    match effect {
        Effect::None => {
            let mode = settle(&parts.state, resume, out, queue);
            parts.with(mode)
        }
        Effect::Send(outs) => {
            out.extend(outs.into_iter().map(Action::Send));
            let mode = settle(&parts.state, resume, out, queue);
            parts.with(mode)
        }
        Effect::Persist { events, then } => {
            let encoded = events
                .iter()
                .map(encode::<B>)
                .collect::<Result<Vec<_>, _>>();
            match (encoded.map(NonEmpty::from_vec), Seq::after(parts.last)) {
                (Ok(Some(payloads)), Some(first)) => {
                    let batch = parts.next_batch;
                    out.push(Action::Persist {
                        batch,
                        first,
                        events: payloads,
                    });
                    out.push(Action::ArmTimeout(batch));
                    let parts = Parts {
                        next_batch: batch.next(),
                        ..parts
                    };
                    parts.with(Mode::Persisting(InFlight {
                        batch,
                        events,
                        then,
                        resume,
                        held: VecDeque::new(),
                    }))
                }
                (Err(error), _) => {
                    out.push(Action::Stop(Fatal::Encode(error.to_string())));
                    parts.with(resume_mode(resume))
                }
                (Ok(_), _) => {
                    // `from_vec` can't be `None` here (`events` is non-empty): this is the
                    // stream running out of positions.
                    out.push(Action::Stop(Fatal::SeqOverflow));
                    parts.with(resume_mode(resume))
                }
            }
        }
    }
}

/// The mode to return to; `Reconciling` becomes `Ready` (releasing the stash) once reconciled.
fn settle<B: Behaviour>(
    state: &B::State,
    resume: Resume<B>,
    out: Actions<B>,
    queue: Queue<B>,
) -> Mode<B> {
    match resume {
        Resume::Ready => Mode::Ready,
        Resume::Reconciling { stash } if B::reconciled(state) => {
            out.push(Action::Entered(ModeName::Ready));
            queue.extend(stash.into_iter().map(Input::Command));
            Mode::Ready
        }
        Resume::Reconciling { stash } => Mode::Reconciling { stash },
    }
}

fn resume_mode<B: Behaviour>(resume: Resume<B>) -> Mode<B> {
    match resume {
        Resume::Reconciling { stash } => Mode::Reconciling { stash },
        Resume::Ready => Mode::Ready,
    }
}

fn encode<B: Behaviour>(event: &B::Event) -> Result<EventPayload, serde_json::Error> {
    EventPayload::encode(&Envelope {
        v: B::EVENT_VERSION,
        event,
    })
}

fn decode<B: Behaviour>(events: &[StoredEvent]) -> Result<Vec<B::Event>, Unrecoverable> {
    events
        .iter()
        .map(|stored| {
            let at = stored.seq;
            let raw: RawEnvelope = serde_json::from_str(stored.payload.as_str()).map_err(|e| {
                Unrecoverable::UndecodableEvent {
                    at,
                    error: e.to_string(),
                }
            })?;
            if raw.v != B::EVENT_VERSION {
                return Err(Unrecoverable::UnknownEventVersion { at, found: raw.v });
            }
            serde_json::from_value(raw.event).map_err(|e| Unrecoverable::UndecodableEvent {
                at,
                error: e.to_string(),
            })
        })
        .collect()
}
