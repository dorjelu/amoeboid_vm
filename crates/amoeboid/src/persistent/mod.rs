//! Event-sourced actors (0002 §Persistent actors). A [`Behaviour`] supplies pure handlers;
//! [`Persistent`] runs recovery, reconcile, persist-then-send and the stash. The mode machine
//! is pure ([`machine::step`]); the actor only executes its actions.

pub mod machine;
#[cfg(test)]
mod tests;

use std::marker::PhantomData;
use std::time::Duration;

use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::journal::{BatchId, JournalMessage, JournalReply, PersistenceId, Sequence};
use crate::nonempty::NonEmpty;
use machine::{Action, Core, Input};

/// One persistent actor's logic. Implemented by a zero-sized marker type; no method takes
/// `self`, state lives in `State`. Every handler is pure; [`Behaviour::send`] is the one effect.
pub trait Behaviour: Send + Sync + Sized + 'static {
    /// Requests (from clients, parent, peers). Stashed while not `Ready`; refusable.
    type Command: Send + 'static;
    /// Facts about the world (child reports, process exits, probe results). Never stashed
    /// while reconciling: reconcile runs on them.
    type Observation: Send + 'static;
    type Event: Serialize + DeserializeOwned + Send + 'static;
    type State: Send + 'static;
    /// Messages to peers. Sent only via [`Effect`], after that effect's events are durable.
    type Out: Send + 'static;
    /// Actor refs and settings from the spawner; never persisted.
    type Peers: Send + Sync + 'static;

    /// Bumped when `Event`'s encoding changes; stored in every envelope. A stream holding an
    /// unknown version is [`Unrecoverable`].
    const EVENT_VERSION: u16;

    fn initial() -> Self::State;

    /// Runs only in `Ready`.
    fn on_command(state: &Self::State, command: Self::Command) -> Effect<Self::Event, Self::Out>;

    /// Runs in `Reconciling` and `Ready`.
    fn on_observation(
        state: &Self::State,
        observation: Self::Observation,
    ) -> Effect<Self::Event, Self::Out>;

    /// Total: runs live after a persist and during replay, so it must not fail or send.
    fn on_event(state: Self::State, event: &Self::Event) -> Self::State;

    /// Runs once after replay; starts the reconcile (probes, child spawns) whose answers come
    /// back as observations.
    fn on_recovered(state: &Self::State) -> Effect<Self::Event, Self::Out>;

    /// Checked after every state change in `Reconciling`; `true` moves to `Ready` and drains
    /// the stash.
    fn reconciled(state: &Self::State) -> bool;

    /// Typed refusal for a command that can't run; `None` when the command expects no answer.
    fn refuse(command: Self::Command, reason: &Unrecoverable) -> Option<Self::Out>;

    /// The report to the parent when this actor becomes unrecoverable.
    fn on_unrecoverable(reason: &Unrecoverable) -> Self::Out;

    /// The one effect: a tell, chosen by matching on `out`.
    fn send(peers: &Self::Peers, out: Self::Out);
}

/// What a handler wants done. A handler never sends directly: a send that happened before its
/// events were durable would be a side effect the journal can't account for after a crash.
/// Messages can't be derived from events instead, because replay re-runs `on_event` (it would
/// resend) and the recipient often comes from the command, not the event (a reply's asker).
pub enum Effect<E, O> {
    /// Nothing to persist or send.
    None,
    /// Persist `events`, apply them with `on_event`, then send `then` in order.
    /// ≈ Akka `Effect.persist(events).thenRun(...)`.
    Persist { events: NonEmpty<E>, then: Vec<O> },
    /// Send without persisting: replies and tells that change no state.
    Send(NonEmpty<O>),
}

/// Why a stream can never be replayed by this binary. Terminal: the actor stays alive, refuses
/// every command, reports once to its parent, and touches nothing in the world (processes,
/// scope, runtime directory and stream are kept for forensics until `amoeboid vacuum_vm NAME`).
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum Unrecoverable {
    #[error("event at position {} doesn't decode: {error}", at.get())]
    UndecodableEvent { at: Sequence, error: String },
    #[error("event at position {} has unknown version {found}", at.get())]
    UnknownEventVersion { at: Sequence, found: u16 },
}

/// Mailbox type of every persistent actor. One wrapper variant per derived type keeps
/// `TryFrom` total (ractor's `get_derived` panics otherwise).
#[derive(Debug)]
pub enum PersistentMessage<C, O> {
    Command(C),
    Observation(O),
    Journal(JournalReply),
    PersistTimeout(BatchId),
}

impl<C, O> From<JournalReply> for PersistentMessage<C, O> {
    fn from(reply: JournalReply) -> Self {
        Self::Journal(reply)
    }
}

impl<C, O> TryFrom<PersistentMessage<C, O>> for JournalReply {
    type Error = PersistentMessage<C, O>;
    fn try_from(message: PersistentMessage<C, O>) -> Result<Self, Self::Error> {
        match message {
            PersistentMessage::Journal(reply) => Ok(reply),
            other @ (PersistentMessage::Command(_)
            | PersistentMessage::Observation(_)
            | PersistentMessage::PersistTimeout(_)) => Err(other),
        }
    }
}

pub struct PersistentArgs<B: Behaviour> {
    pub id: PersistenceId,
    pub journal: ActorRef<JournalMessage>,
    pub peers: B::Peers,
    /// No `Persisted` within this: the actor stops and is restarted.
    pub persist_timeout: Duration,
}

pub struct PersistentState<B: Behaviour> {
    args: PersistentArgs<B>,
    /// `None` only during a step, which takes the core by value.
    core: Option<Core<B>>,
}

/// The ractor actor that runs a [`Behaviour`].
pub struct Persistent<B>(PhantomData<fn() -> B>);

impl<B> Persistent<B> {
    #[must_use]
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<B> Default for Persistent<B> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("persistent actor state is missing")]
struct CoreMissing;

impl<B: Behaviour> Actor for Persistent<B> {
    type Msg = PersistentMessage<B::Command, B::Observation>;
    type State = PersistentState<B>;
    type Arguments = PersistentArgs<B>;

    fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: PersistentArgs<B>,
    ) -> impl Future<Output = Result<PersistentState<B>, ActorProcessingErr>> + Send {
        std::future::ready(start(&myself, args))
    }

    fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut PersistentState<B>,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        std::future::ready(run(&myself, message, state))
    }
}

/// Asks the journal for this actor's stream; recovery continues when `Replayed` arrives.
fn start<B: Behaviour>(
    myself: &ActorRef<PersistentMessage<B::Command, B::Observation>>,
    args: PersistentArgs<B>,
) -> Result<PersistentState<B>, ActorProcessingErr> {
    tracing::info!(id = args.id.as_str(), "recovering");
    args.journal
        .cast(JournalMessage::Replay {
            id: args.id.clone(),
            reply_to: myself.get_derived(),
        })
        .map_err(|error| error.to_string())?;
    Ok(PersistentState {
        args,
        core: Some(Core::recovering()),
    })
}

/// One step of the machine, then its actions in order. `Stop` ends the actor with `Err`.
fn run<B: Behaviour>(
    myself: &ActorRef<PersistentMessage<B::Command, B::Observation>>,
    message: PersistentMessage<B::Command, B::Observation>,
    state: &mut PersistentState<B>,
) -> Result<(), ActorProcessingErr> {
    let input = match message {
        PersistentMessage::Command(command) => Input::Command(command),
        PersistentMessage::Observation(observation) => Input::Observation(observation),
        PersistentMessage::PersistTimeout(batch) => Input::PersistTimeout(batch),
        PersistentMessage::Journal(reply) => match reply {
            JournalReply::Replayed { events, .. } => Input::Replayed(events),
            JournalReply::ReplayFailed { reason, .. } => Input::ReplayFailed(reason),
            JournalReply::Persisted { batch, last } => Input::Persisted { batch, last },
            JournalReply::PersistFailed { batch, reason } => Input::PersistFailed { batch, reason },
        },
    };
    let core = state.core.take().ok_or(CoreMissing)?;
    let (core, actions) = machine::step(core, input);
    state.core = Some(core);
    let id = state.args.id.as_str();
    for action in actions {
        match action {
            Action::Persist {
                batch,
                first,
                events,
            } => state
                .args
                .journal
                .cast(JournalMessage::Persist {
                    batch,
                    id: state.args.id.clone(),
                    first,
                    events,
                    reply_to: myself.get_derived(),
                })
                .map_err(|error| error.to_string())?,
            Action::ArmTimeout(batch) => {
                myself.send_after(state.args.persist_timeout, move || {
                    PersistentMessage::PersistTimeout(batch)
                });
            }
            Action::Send(out) => B::send(&state.args.peers, out),
            Action::Entered(mode) => tracing::info!(id, ?mode, "entered"),
            Action::Ignored(why) => tracing::debug!(id, why, "input ignored"),
            Action::Stop(fatal) => {
                tracing::error!(id, %fatal, "stopping");
                return Err(fatal.into());
            }
        }
    }
    Ok(())
}

impl crate::diagnostic::Diagnostic for Unrecoverable {
    fn code(&self) -> &'static str {
        match self {
            Self::UndecodableEvent { .. } => "persistent.unrecoverable.undecodable_event",
            Self::UnknownEventVersion { .. } => "persistent.unrecoverable.unknown_event_version",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::UndecodableEvent { .. } | Self::UnknownEventVersion { .. } => {
                Some("the stream is kept for inspection; amoeboid vacuum_vm NAME removes it".into())
            }
        }
    }
}
