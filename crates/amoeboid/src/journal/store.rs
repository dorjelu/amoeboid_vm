//! Stream storage behind the journal actor: SQLite, or memory for tests. Both enforce the same
//! fence: an append must start at the stream's next position.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use rustix::fs::{Mode, OFlags};
use tokio_rusqlite::rusqlite::{self, ErrorCode, TransactionBehavior, params};

use super::{
    EventPayload, PersistFailure, Persistence, PersistenceId, Sequence, StoreError, StoredEvent,
};
use crate::nonempty::NonEmpty;
use crate::runtime::paths::RuntimePaths;

const SCHEMA_VERSION: i64 = 1;

const SCHEMA: &str = "
CREATE TABLE events (
  persistence_id     TEXT NOT NULL,
  sequence       INTEGER NOT NULL,
  payload TEXT NOT NULL,
  at      INTEGER NOT NULL,
  PRIMARY KEY (persistence_id, sequence)
) STRICT;
PRAGMA user_version = 1;
";

pub(super) enum Store {
    Sqlite(tokio_rusqlite::Connection),
    Memory(BTreeMap<PersistenceId, Vec<EventPayload>>),
}

impl StoreError {
    fn of(error: impl std::fmt::Display) -> Self {
        Self(error.to_string())
    }
}

impl Store {
    pub(super) async fn open(
        persistence: Persistence,
        paths: &RuntimePaths,
    ) -> Result<Self, StoreError> {
        match persistence {
            Persistence::Memory => Ok(Self::Memory(BTreeMap::new())),
            Persistence::Sqlite => {
                let path = paths.journal();
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

    pub(super) async fn replay(
        &mut self,
        id: &PersistenceId,
    ) -> Result<Vec<StoredEvent>, StoreError> {
        match self {
            Self::Memory(streams) => Ok(streams
                .get(id)
                .map(|events| {
                    std::iter::successors(Some(Sequence::FIRST), |sequence| sequence.next())
                        .zip(events)
                        .map(|(sequence, payload)| StoredEvent {
                            sequence,
                            payload: payload.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default()),
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
                .keys()
                .filter(|id| id.below(under).is_some())
                .cloned()
                .collect()),
            Self::Sqlite(connection) => {
                let prefix = format!("{}/", under.as_str());
                let names = connection
                    .call(move |connection| {
                        let mut statement = connection.prepare_cached(
                            "SELECT DISTINCT persistence_id FROM events \
                             WHERE substr(persistence_id, 1, length(?1)) = ?1 \
                             ORDER BY persistence_id",
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

    pub(super) async fn append(
        &mut self,
        id: &PersistenceId,
        first: Sequence,
        events: NonEmpty<EventPayload>,
    ) -> Result<Sequence, PersistFailure> {
        match self {
            Self::Memory(streams) => {
                let stream = streams.entry(id.clone()).or_default();
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
                connection
                    .call(move |connection| {
                        append_rows(connection, &persistence_id, first_value, &rows)
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
        0 => connection.execute_batch(SCHEMA),
        SCHEMA_VERSION => Ok(()),
        other => Err(rusqlite::Error::InvalidParameterName(format!(
            "journal schema version {other} is newer than this binary's {SCHEMA_VERSION}"
        ))),
    }
}

fn replay_rows(
    connection: &mut rusqlite::Connection,
    persistence_id: &str,
) -> Result<Vec<StoredEvent>, StoreError> {
    let mut statement = connection
        .prepare_cached(
            "SELECT sequence, payload FROM events WHERE persistence_id = ? ORDER BY sequence",
        )
        .map_err(StoreError::of)?;
    let rows = statement
        .query_map([persistence_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(StoreError::of)?;
    let mut events = Vec::new();
    let mut expected = Some(Sequence::FIRST);
    for row in rows {
        let (sequence, payload) = row.map_err(StoreError::of)?;
        // Positions are contiguous from 1; anything else is a corrupt stream.
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
    Ok(events)
}

fn append_rows(
    connection: &mut rusqlite::Connection,
    persistence_id: &str,
    first: u64,
    payloads: &[String],
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
    let at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        });
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
