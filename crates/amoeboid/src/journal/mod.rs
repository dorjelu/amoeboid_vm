//! Journal actor (0002 §Persistence): sole owner of the SQLite connection; appends and replays
//! per-actor event streams. Payloads are opaque here; persistent actors encode and decode them.

mod store;
#[cfg(test)]
mod tests;

use std::num::NonZeroU64;

use ractor::{Actor, ActorProcessingErr, ActorRef, DerivedActorRef};
use serde::Serialize;

use crate::nonempty::NonEmpty;
use crate::runtime::paths::RuntimePaths;
use store::Store;

/// `vm/work`, `runtime`: `/`-joined, non-empty segments of `[A-Za-z0-9-]` (VM names are
/// case-sensitive, so their streams are too).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PersistenceId(String);

#[derive(Debug, thiserror::Error)]
#[error("{0:?} is not a persistence id: `/`-joined segments of [A-Za-z0-9-]")]
pub struct BadPersistenceId(String);

impl PersistenceId {
    /// # Errors
    ///
    /// Empty, an empty segment, or a character outside `[A-Za-z0-9-/]`.
    pub fn parse(id: String) -> Result<Self, BadPersistenceId> {
        let valid = id.split('/').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        });
        if valid {
            Ok(Self(id))
        } else {
            Err(BadPersistenceId(id))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The id one level below: `vm` + `work` = `vm/work`.
    ///
    /// # Errors
    ///
    /// `segment` is not a valid segment.
    pub fn child(&self, segment: &str) -> Result<Self, BadPersistenceId> {
        Self::parse(format!("{}/{segment}", self.0))
    }

    /// The rest of this id below `parent` (`vm/work` under `vm` is `work`), if it is below it.
    #[must_use]
    pub fn below(&self, parent: &Self) -> Option<&str> {
        self.0.strip_prefix(&parent.0)?.strip_prefix('/')
    }
}

/// Position in one stream, from 1.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Sequence(NonZeroU64);

impl Sequence {
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// `None` past `u64::MAX`.
    #[must_use]
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    /// The position after `last`, or [`Sequence::FIRST`] for an empty stream.
    #[must_use]
    pub fn after(last: Option<Self>) -> Option<Self> {
        last.map_or(Some(Self::FIRST), Self::next)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// JSON text of one versioned event envelope.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EventPayload(String);

impl EventPayload {
    /// # Errors
    ///
    /// The value doesn't serialize.
    pub fn encode<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        serde_json::to_string(value).map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StoredEvent {
    pub sequence: Sequence,
    pub payload: EventPayload,
}

/// Correlates a `Persist` with its reply; unique per persistent-actor incarnation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct BatchId(u64);

impl BatchId {
    pub const FIRST: Self = Self(0);

    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

#[derive(Debug)]
pub enum JournalMessage {
    Replay {
        id: PersistenceId,
        reply_to: DerivedActorRef<JournalReply>,
    },
    /// The ids of every stream directly or indirectly under `under` (`vm` lists `vm/work`, ...).
    Streams {
        under: PersistenceId,
        reply_to: DerivedActorRef<JournalReply>,
    },
    /// Appends at `first`, `first + 1`, ...; `first` must be the stream's next position.
    Persist {
        batch: BatchId,
        id: PersistenceId,
        first: Sequence,
        events: NonEmpty<EventPayload>,
        reply_to: DerivedActorRef<JournalReply>,
    },
}

#[derive(Debug)]
pub enum JournalReply {
    Replayed {
        id: PersistenceId,
        events: Vec<StoredEvent>,
    },
    ReplayFailed {
        id: PersistenceId,
        reason: StoreError,
    },
    Persisted {
        batch: BatchId,
        last: Sequence,
    },
    PersistFailed {
        batch: BatchId,
        reason: PersistFailure,
    },
    Streams {
        under: PersistenceId,
        ids: Vec<PersistenceId>,
    },
    StreamsFailed {
        under: PersistenceId,
        reason: StoreError,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum PersistFailure {
    /// `first` is not the stream's next position: another incarnation wrote, the writer is
    /// stale.
    #[error("stream position {first} is not next; the next is {next}")]
    Conflict { first: u64, next: u64 },
    #[error(transparent)]
    Store(StoreError),
}

/// SQLite's message, or a corrupt row; the journal does not interpret it.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct StoreError(String);

/// Public selector; the store itself stays private so no outside implementation can weaken the
/// append-position fence.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Persistence {
    /// At [`RuntimePaths::journal`], created `0600`.
    Sqlite,
    Memory,
}

pub struct JournalArgs {
    pub persistence: Persistence,
    pub paths: RuntimePaths,
}

pub struct Journal;

pub struct JournalState {
    store: Store,
}

impl Actor for Journal {
    type Msg = JournalMessage;
    type State = JournalState;
    type Arguments = JournalArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<JournalMessage>,
        args: JournalArgs,
    ) -> Result<JournalState, ActorProcessingErr> {
        let store = Store::open(args.persistence, &args.paths).await?;
        Ok(JournalState { store })
    }

    async fn handle(
        &self,
        _myself: ActorRef<JournalMessage>,
        message: JournalMessage,
        state: &mut JournalState,
    ) -> Result<(), ActorProcessingErr> {
        // Store failures are answers, not journal failures: the requester decides.
        let (reply, reply_to) = match message {
            JournalMessage::Replay { id, reply_to } => {
                let reply = match state.store.replay(&id).await {
                    Ok(events) => JournalReply::Replayed { id, events },
                    Err(reason) => JournalReply::ReplayFailed { id, reason },
                };
                (reply, reply_to)
            }
            JournalMessage::Streams { under, reply_to } => {
                let reply = match state.store.streams(&under).await {
                    Ok(ids) => JournalReply::Streams { under, ids },
                    Err(reason) => JournalReply::StreamsFailed { under, reason },
                };
                (reply, reply_to)
            }
            JournalMessage::Persist {
                batch,
                id,
                first,
                events,
                reply_to,
            } => {
                let reply = match state.store.append(&id, first, events).await {
                    Ok(last) => JournalReply::Persisted { batch, last },
                    Err(reason) => JournalReply::PersistFailed { batch, reason },
                };
                (reply, reply_to)
            }
        };
        if reply_to.send_message(reply).is_err() {
            tracing::debug!("journal reply undeliverable: requester stopped");
        }
        Ok(())
    }
}

impl crate::diagnostic::Diagnostic for BadPersistenceId {
    fn code(&self) -> &'static str {
        "journal.persistence_id.invalid"
    }
    fn hint(&self) -> Option<String> {
        None
    }
}

impl crate::diagnostic::Diagnostic for PersistFailure {
    fn code(&self) -> &'static str {
        match self {
            Self::Conflict { .. } => "journal.persist.conflict",
            Self::Store(_) => "journal.persist.store",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::Conflict { .. } | Self::Store(_) => None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    pub(crate) fn store_error(message: &str) -> super::StoreError {
        super::StoreError(message.to_owned())
    }
}
