//! Stream storage behind the journal actor: SQLite, or memory for tests. Both enforce the same
//! fence: an append must start at the stream's next position.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use rustix::fs::{Mode, OFlags};
use tokio_rusqlite::rusqlite::{self, ErrorCode, TransactionBehavior, params};

use super::{
    EventPayload, PersistFailure, Persistence, PersistenceId, Seq, StoreError, StoredEvent,
};
use crate::nonempty::NonEmpty;
use crate::runtime::paths::RuntimePaths;

const SCHEMA_VERSION: i64 = 1;

const SCHEMA: &str = "
CREATE TABLE events (
  pid     TEXT NOT NULL,
  seq     INTEGER NOT NULL,
  payload TEXT NOT NULL,
  at      INTEGER NOT NULL,
  PRIMARY KEY (pid, seq)
) STRICT;
PRAGMA user_version = 1;
";

pub(super) enum Store {
    Sqlite(tokio_rusqlite::Connection),
    Memory(BTreeMap<PersistenceId, Vec<EventPayload>>),
}

impl StoreError {
    fn of(e: impl std::fmt::Display) -> Self {
        Self(e.to_string())
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
                    &path,
                    OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::from_raw_mode(0o600),
                )
                .map_err(StoreError::of)?;
                let conn = tokio_rusqlite::Connection::open(&path)
                    .await
                    .map_err(StoreError::of)?;
                conn.call(migrate).await.map_err(StoreError::of)?;
                Ok(Self::Sqlite(conn))
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
                    std::iter::successors(Some(Seq::FIRST), |seq| seq.next())
                        .zip(events)
                        .map(|(seq, payload)| StoredEvent {
                            seq,
                            payload: payload.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default()),
            Self::Sqlite(conn) => {
                let pid = id.as_str().to_owned();
                conn.call(move |conn| replay_rows(conn, &pid))
                    .await
                    .map_err(StoreError::of)
            }
        }
    }

    pub(super) async fn append(
        &mut self,
        id: &PersistenceId,
        first: Seq,
        events: NonEmpty<EventPayload>,
    ) -> Result<Seq, PersistFailure> {
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
            Self::Sqlite(conn) => {
                let pid = id.as_str().to_owned();
                let rows: Vec<String> = events.iter().map(|e| e.0.clone()).collect();
                let first_raw = first.get();
                conn.call(move |conn| append_rows(conn, &pid, first_raw, &rows))
                    .await
                    .map_err(|e| match e {
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

fn last_of(first: Seq, events: &NonEmpty<EventPayload>) -> Result<Seq, PersistFailure> {
    events
        .iter()
        .skip(1)
        .try_fold(first, |seq, _| seq.next())
        .ok_or(PersistFailure::Store(StoreError::of(
            "stream position overflow",
        )))
}

fn migrate(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    match version {
        0 => conn.execute_batch(SCHEMA),
        SCHEMA_VERSION => Ok(()),
        other => Err(rusqlite::Error::InvalidParameterName(format!(
            "journal schema version {other} is newer than this binary's {SCHEMA_VERSION}"
        ))),
    }
}

fn replay_rows(conn: &mut rusqlite::Connection, pid: &str) -> Result<Vec<StoredEvent>, StoreError> {
    let mut stmt = conn
        .prepare_cached("SELECT seq, payload FROM events WHERE pid = ? ORDER BY seq")
        .map_err(StoreError::of)?;
    let rows = stmt
        .query_map([pid], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(StoreError::of)?;
    let mut events = Vec::new();
    let mut expected = Some(Seq::FIRST);
    for row in rows {
        let (seq, payload) = row.map_err(StoreError::of)?;
        // Positions are contiguous from 1; anything else is a corrupt stream.
        let seq = expected
            .filter(|e| u64::try_from(seq).ok() == Some(e.get()))
            .ok_or_else(|| StoreError(format!("stream {pid} has a gap at position {seq}")))?;
        events.push(StoredEvent {
            seq,
            payload: EventPayload(payload),
        });
        expected = seq.next();
    }
    Ok(events)
}

fn append_rows(
    conn: &mut rusqlite::Connection,
    pid: &str,
    first: u64,
    payloads: &[String],
) -> Result<(), PersistFailure> {
    let store = |e: rusqlite::Error| PersistFailure::Store(StoreError::of(e));
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(store)?;
    let count: i64 = tx
        .query_row("SELECT count(*) FROM events WHERE pid = ?", [pid], |row| {
            row.get(0)
        })
        .map_err(store)?;
    let next = u64::try_from(count).unwrap_or(u64::MAX).saturating_add(1);
    if first != next {
        return Err(PersistFailure::Conflict { first, next });
    }
    let at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    {
        let mut insert = tx
            .prepare_cached("INSERT INTO events (pid, seq, payload, at) VALUES (?, ?, ?, ?)")
            .map_err(store)?;
        for (offset, payload) in payloads.iter().enumerate() {
            let seq = u64::try_from(offset)
                .ok()
                .and_then(|o| first.checked_add(o))
                .and_then(|s| i64::try_from(s).ok())
                .ok_or(PersistFailure::Store(StoreError::of(
                    "stream position overflow",
                )))?;
            insert
                .execute(params![pid, seq, payload, at])
                .map_err(|e| match e.sqlite_error_code() {
                    // The primary key is the last line of the fence.
                    Some(ErrorCode::ConstraintViolation) => {
                        PersistFailure::Conflict { first, next }
                    }
                    _ => store(e),
                })?;
        }
    }
    tx.commit().map_err(store)
}
