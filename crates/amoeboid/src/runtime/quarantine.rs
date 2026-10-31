//! Quarantine records (0002 §Unrecoverable streams, 0003 §Quarantine, §Supervision): one stream
//! per VM, `quarantine/<name>`, appended by Root, which is not a persistent actor. A VM is
//! quarantined while its stream has a live event; `vacuum_vm` retires it, and `release_vm`
//! retires a `SuspiciousHalt` one.

use serde::{Deserialize, Serialize};

use crate::diagnostic::Report;
use crate::journal::{EventPayload, StoredEvent};

/// Bumped when `QuarantineEvent`'s encoding changes. Version 1 recorded only a reason.
const VERSION: u16 = 2;

/// Why a VM is quarantined.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum QuarantineCause {
    /// The VM's stream can't be replayed by this binary; only `vacuum_vm` removes the VM.
    Unreplayable { reason: Report },
    /// The VM's twin kept failing until its restart budget was spent; `release_vm` lifts it.
    /// `failures` are the last ones, oldest first.
    SuspiciousHalt {
        restarts: u32,
        failures: Vec<Report>,
    },
}

impl QuarantineCause {
    /// One line for logs and status: the reason, or the latest failure.
    #[must_use]
    pub fn summary(&self) -> String {
        match self {
            Self::Unreplayable { reason } => reason.message.clone(),
            Self::SuspiciousHalt { restarts, failures } => format!(
                "failed after {restarts} restarts{}",
                failures
                    .last()
                    .map_or(String::new(), |last| format!(": {}", last.message))
            ),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuarantineEvent {
    /// Nothing runs for the VM's twin until the quarantine is lifted.
    Quarantined { cause: QuarantineCause },
}

/// Same envelope as persistent actors' events.
#[derive(Serialize)]
struct Envelope<'a> {
    version: u16,
    event: &'a QuarantineEvent,
}

#[derive(Deserialize)]
struct RawEnvelope {
    version: u16,
    event: serde_json::Value,
}

/// Version 1: `{"quarantined": {"reason": Report}}`, always an unreplayable stream.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum EventVersion1 {
    Quarantined { reason: Report },
}

/// # Errors
///
/// The event doesn't serialize.
pub fn encode(event: &QuarantineEvent) -> Result<EventPayload, serde_json::Error> {
    EventPayload::encode(&Envelope {
        version: VERSION,
        event,
    })
}

/// The cause recorded by the first live event; a record this binary can't read still
/// quarantines (its cause says so, and only `vacuum_vm` removes it).
#[must_use]
pub fn cause(events: &[StoredEvent]) -> Option<QuarantineCause> {
    let first = events.first()?;
    let decoded = serde_json::from_str::<RawEnvelope>(first.payload.as_str())
        .ok()
        .and_then(|raw| match raw.version {
            1 => serde_json::from_value::<EventVersion1>(raw.event).ok().map(
                |EventVersion1::Quarantined { reason }| QuarantineCause::Unreplayable { reason },
            ),
            VERSION => serde_json::from_value::<QuarantineEvent>(raw.event)
                .ok()
                .map(|QuarantineEvent::Quarantined { cause }| cause),
            _ => None,
        });
    Some(decoded.unwrap_or_else(|| QuarantineCause::Unreplayable {
        reason: Report {
            code: "runtime.quarantine.unreadable".into(),
            message: "the quarantine record doesn't decode".into(),
            hint: None,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{Replay, Sequence};

    fn stored(payload: EventPayload) -> StoredEvent {
        StoredEvent {
            sequence: Sequence::FIRST,
            payload,
        }
    }

    fn report(code: &str) -> Report {
        Report {
            code: code.into(),
            message: "x".into(),
            hint: None,
        }
    }

    #[test]
    fn a_recorded_cause_reads_back() {
        for cause in [
            QuarantineCause::Unreplayable {
                reason: report("persistent.unrecoverable.undecodable_event"),
            },
            QuarantineCause::SuspiciousHalt {
                restarts: 5,
                failures: vec![report("a"), report("b")],
            },
        ] {
            let payload = encode(&QuarantineEvent::Quarantined {
                cause: cause.clone(),
            })
            .unwrap();
            assert_eq!(super::cause(&[stored(payload)]), Some(cause));
        }
    }

    #[test]
    fn a_version_1_record_reads_as_unreplayable() {
        let payload = EventPayload::encode(&serde_json::json!({
            "version": 1,
            "event": {"quarantined": {"reason": {"code": "c", "message": "x", "hint": null}}}
        }))
        .unwrap();
        assert_eq!(
            cause(&[stored(payload)]),
            Some(QuarantineCause::Unreplayable {
                reason: report("c")
            })
        );
    }

    #[test]
    fn no_live_event_means_not_quarantined() {
        assert_eq!(cause(&Replay::empty().events), None);
    }

    #[test]
    fn an_unreadable_record_still_quarantines_as_unreplayable() {
        let payload =
            EventPayload::encode(&serde_json::json!({"version": 9, "event": {}})).unwrap();
        assert!(matches!(
            cause(&[stored(payload)]),
            Some(QuarantineCause::Unreplayable { reason }) if reason.code == "runtime.quarantine.unreadable"
        ));
    }
}
