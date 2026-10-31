//! Stream storage behind the journal actor: SQLite, or memory for tests. Both enforce the same
//! fence: an append must start at the stream's next position.

use std::collections::BTreeMap;

use rustix::fs::{Mode, OFlags};
use tokio_rusqlite::rusqlite::{self, ErrorCode, TransactionBehavior, params};

use super::{
    EventPayload, JournalStore, PersistFailure, PersistenceId, Replay, Sequence, StoreError,
    StoredEvent,
};
use crate::clock::Timestamp;
use crate::nonempty::NonEmpty;

const SCHEMA_VERSION: i64 = 2;

/// Version 1: event streams.
const SCHEMA_1: &str = "
CREATE TABLE events (
  persistence_id TEXT NOT NULL,
  sequence       INTEGER NOT NULL,
  payload        TEXT NOT NULL,
  at             INTEGER NOT NULL,
  PRIMARY KEY (persistence_id, sequence)
) STRICT;
PRAGMA user_version = 1;
";

/// Version 2: tombstones. Events at or below `through` are retired: kept, never replayed; a
/// stream with no event above its tombstone is not listed.
const SCHEMA_2: &str = "
CREATE TABLE tombstones (
  persistence_id TEXT PRIMARY KEY,
  through        INTEGER NOT NULL,
  at             INTEGER NOT NULL
) STRICT;
PRAGMA user_version = 2;
";

pub(super) enum Store {
    Sqlite(tokio_rusqlite::Connection),
    Memory(BTreeMap<PersistenceId, MemoryStream>),
}

/// One stream in memory: every event ever appended, and how many of them are retired.
#[derive(Default)]
pub(super) struct MemoryStream {
    events: Vec<EventPayload>,
    through: usize,
}

impl MemoryStream {
    fn live(&self) -> bool {
        self.events.len() > self.through
    }
}

impl StoreError {
    fn of(error: impl std::fmt::Display) -> Self {
        Self(error.to_string())
    }
}

impl Store {
    pub(super) async fn open(store: &JournalStore) -> Result<Self, StoreError> {
        match store {
            JournalStore::Memory => Ok(Self::Memory(BTreeMap::new())),
            JournalStore::Sqlite(path) => {
                let path = path.clone();
                // Created 0600 before SQLite sees it; O_NOFOLLOW refuses a planted symlink.
                rustix::fs::open(
                    path.as_path(),
                    OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::from_raw_mode(0o600),
                )
                .map_err(StoreError::of)?;
                let connection = tokio_rusqlite::Connection::open(&path)
                    .await
                    .map_err(StoreError::of)?;
                connection.call(migrate).await.map_err(StoreError::of)?;
                Ok(Self::Sqlite(connection))
            }
        }
    }

    /// The stream's events after its tombstone, and its last position (retired events
    /// included).
    pub(super) async fn replay(&mut self, id: &PersistenceId) -> Result<Replay, StoreError> {
        match self {
            Self::Memory(streams) => Ok(streams.get(id).map_or_else(Replay::empty, |stream| {
                let positions: Vec<Sequence> =
                    std::iter::successors(Some(Sequence::FIRST), |sequence| sequence.next())
                        .take(stream.events.len())
                        .collect();
                Replay {
                    events: positions
                        .iter()
                        .zip(&stream.events)
                        .skip(stream.through)
                        .map(|(sequence, payload)| StoredEvent {
                            sequence: *sequence,
                            payload: payload.clone(),
                        })
                        .collect(),
                    last: positions.last().copied(),
                }
            })),
            Self::Sqlite(connection) => {
                let persistence_id = id.as_str().to_owned();
                connection
                    .call(move |connection| replay_rows(connection, &persistence_id))
                    .await
                    .map_err(StoreError::of)
            }
        }
    }

    /// Ids of the streams below `under`, in order.
    pub(super) async fn streams(
        &mut self,
        under: &PersistenceId,
    ) -> Result<Vec<PersistenceId>, StoreError> {
        match self {
            Self::Memory(streams) => Ok(streams
                .iter()
                .filter(|(id, stream)| id.below(under).is_some() && stream.live())
                .map(|(id, _)| id.clone())
                .collect()),
            Self::Sqlite(connection) => {
                let prefix = format!("{}/", under.as_str());
                let names = connection
                    .call(move |connection| {
                        let mut statement = connection.prepare_cached(
                            "SELECT events.persistence_id FROM events \
                             LEFT JOIN tombstones USING (persistence_id) \
                             WHERE substr(events.persistence_id, 1, length(?1)) = ?1 \
                             GROUP BY events.persistence_id \
                             HAVING max(events.sequence) > coalesce(max(tombstones.through), 0) \
                             ORDER BY events.persistence_id",
                        )?;
                        let rows = statement.query_map([prefix], |row| row.get::<_, String>(0))?;
                        rows.collect::<rusqlite::Result<Vec<String>>>()
                    })
                    .await
                    .map_err(StoreError::of)?;
                names
                    .into_iter()
                    .map(|name| PersistenceId::parse(name).map_err(StoreError::of))
                    .collect()
            }
        }
    }

    /// Appends `events` at `first`, stamped `at`.
    pub(super) async fn append(
        &mut self,
        id: &PersistenceId,
        first: Sequence,
        events: NonEmpty<EventPayload>,
        at: Timestamp,
    ) -> Result<Sequence, PersistFailure> {
        match self {
            Self::Memory(streams) => {
                let stream = &mut streams.entry(id.clone()).or_default().events;
                let next = u64::try_from(stream.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(1);
                if first.get() != next {
                    return Err(PersistFailure::Conflict {
                        first: first.get(),
                        next,
                    });
                }
                stream.extend(events.iter().cloned());
                last_of(first, &events)
            }
            Self::Sqlite(connection) => {
                let persistence_id = id.as_str().to_owned();
                let rows: Vec<String> = events.iter().map(|event| event.0.clone()).collect();
                let first_value = first.get();
                let at = at.milliseconds();
                connection
                    .call(move |connection| {
                        append_rows(connection, &persistence_id, first_value, &rows, at)
                    })
                    .await
                    .map_err(|error| match error {
                        tokio_rusqlite::Error::Error(failure) => failure,
                        // `Error` is non-exhaustive.
                        other @ (tokio_rusqlite::Error::ConnectionClosed
                        | tokio_rusqlite::Error::Close(_)
                        | _) => PersistFailure::Store(StoreError::of(other)),
                    })?;
                last_of(first, &events)
            }
        }
    }
}

impl Store {
    /// Retires every event the stream holds now: a tombstone at its last position. Later
    /// appends continue after it and are live. Retiring an empty or retired stream changes
    /// nothing.
    pub(super) async fn retire(
        &mut self,
        id: &PersistenceId,
        at: Timestamp,
    ) -> Result<(), StoreError> {
        match self {
            Self::Memory(streams) => {
                if let Some(stream) = streams.get_mut(id) {
                    stream.through = stream.events.len();
                }
                Ok(())
            }
            Self::Sqlite(connection) => {
                let persistence_id = id.as_str().to_owned();
                let at = at.milliseconds();
                connection
                    .call(move |connection| {
                        connection
                            .prepare_cached(
                                "INSERT INTO tombstones (persistence_id, through, at) \
                                 SELECT persistence_id, max(sequence), ?2 FROM events \
                                 WHERE persistence_id = ?1 GROUP BY persistence_id \
                                 ON CONFLICT (persistence_id) \
                                 DO UPDATE SET through = excluded.through, at = excluded.at",
                            )?
                            .execute(params![persistence_id, at])
                    })
                    .await
                    .map(|_| ())
                    .map_err(StoreError::of)
            }
        }
    }
}

fn last_of(first: Sequence, events: &NonEmpty<EventPayload>) -> Result<Sequence, PersistFailure> {
    events
        .iter()
        .skip(1)
        .try_fold(first, |sequence, _| sequence.next())
        .ok_or(PersistFailure::Store(StoreError::of(
            "stream position overflow",
        )))
}

fn migrate(connection: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    match version {
        0 => connection.execute_batch(&format!("BEGIN; {SCHEMA_1} {SCHEMA_2} COMMIT;")),
        1 => connection.execute_batch(&format!("BEGIN; {SCHEMA_2} COMMIT;")),
        SCHEMA_VERSION => Ok(()),
        other => Err(rusqlite::Error::InvalidParameterName(format!(
            "journal schema version {other} is newer than this binary's {SCHEMA_VERSION}"
        ))),
    }
}

fn replay_rows(
    connection: &mut rusqlite::Connection,
    persistence_id: &str,
) -> Result<Replay, StoreError> {
    let (through, last): (i64, Option<i64>) = connection
        .query_row(
            "SELECT \
               coalesce((SELECT through FROM tombstones WHERE persistence_id = ?1), 0), \
               (SELECT max(sequence) FROM events WHERE persistence_id = ?1)",
            [persistence_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(StoreError::of)?;
    let position = |value: i64| {
        u64::try_from(value)
            .ok()
            .and_then(std::num::NonZeroU64::new)
            .map(Sequence)
            .ok_or_else(|| {
                StoreError(format!(
                    "stream {persistence_id} has an invalid position {value}"
                ))
            })
    };
    let last = last.map(position).transpose()?;
    let mut statement = connection
        .prepare_cached(
            "SELECT sequence, payload FROM events WHERE persistence_id = ? AND sequence > ? \
             ORDER BY sequence",
        )
        .map_err(StoreError::of)?;
    let rows = statement
        .query_map(params![persistence_id, through], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(StoreError::of)?;
    let mut events = Vec::new();
    let mut expected = Sequence::after(if through == 0 {
        None
    } else {
        Some(position(through)?)
    });
    for row in rows {
        let (sequence, payload) = row.map_err(StoreError::of)?;
        // Positions are contiguous after the tombstone; anything else is a corrupt stream.
        let sequence = expected
            .filter(|expected| u64::try_from(sequence).ok() == Some(expected.get()))
            .ok_or_else(|| {
                StoreError(format!(
                    "stream {persistence_id} has a gap at position {sequence}"
                ))
            })?;
        events.push(StoredEvent {
            sequence,
            payload: EventPayload(payload),
        });
        expected = sequence.next();
    }
    Ok(Replay { events, last })
}

fn append_rows(
    connection: &mut rusqlite::Connection,
    persistence_id: &str,
    first: u64,
    payloads: &[String],
    at: i64,
) -> Result<(), PersistFailure> {
    let store = |error: rusqlite::Error| PersistFailure::Store(StoreError::of(error));
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(store)?;
    let count: i64 = transaction
        .query_row(
            "SELECT count(*) FROM events WHERE persistence_id = ?",
            [persistence_id],
            |row| row.get(0),
        )
        .map_err(store)?;
    let next = u64::try_from(count).unwrap_or(u64::MAX).saturating_add(1);
    if first != next {
        return Err(PersistFailure::Conflict { first, next });
    }
    {
        let mut insert = transaction
            .prepare_cached(
                "INSERT INTO events (persistence_id, sequence, payload, at) VALUES (?, ?, ?, ?)",
            )
            .map_err(store)?;
        for (offset, payload) in payloads.iter().enumerate() {
            let sequence = u64::try_from(offset)
                .ok()
                .and_then(|o| first.checked_add(o))
                .and_then(|s| i64::try_from(s).ok())
                .ok_or(PersistFailure::Store(StoreError::of(
                    "stream position overflow",
                )))?;
            insert
                .execute(params![persistence_id, sequence, payload, at])
                .map_err(|error| match error.sqlite_error_code() {
                    // The primary key is the last line of the fence.
                    Some(ErrorCode::ConstraintViolation) => {
                        PersistFailure::Conflict { first, next }
                    }
                    _ => store(error),
                })?;
        }
    }
    transaction.commit().map_err(store)
}
