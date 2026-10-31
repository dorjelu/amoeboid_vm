//! Quarantine records (0002 §Unrecoverable streams, 0003 §Quarantine): one stream per VM,
//! `quarantine/<name>`, appended by Root, which is not a persistent actor. A VM is quarantined
//! while its stream has a live event; `vacuum_vm` retires it.

use serde::{Deserialize, Serialize};

use crate::diagnostic::Report;
use crate::journal::{EventPayload, StoredEvent};

/// Bumped when `QuarantineEvent`'s encoding changes.
const VERSION: u16 = 1;

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuarantineEvent {
    /// The VM's twin can't replay its stream; nothing runs for the VM until it is vacuumed.
    Quarantined { reason: Report },
}

/// Same envelope as persistent actors' events.
#[derive(Serialize, Deserialize)]
struct Envelope {
    version: u16,
    event: QuarantineEvent,
}

/// # Errors
///
/// The event doesn't serialize.
pub fn encode(event: QuarantineEvent) -> Result<EventPayload, serde_json::Error> {
    EventPayload::encode(&Envelope {
        version: VERSION,
        event,
    })
}

/// The reason recorded by the first live event; a record this binary can't read still
/// quarantines (its reason says so).
#[must_use]
pub fn reason(events: &[StoredEvent]) -> Option<Report> {
    let first = events.first()?;
    let decoded = serde_json::from_str::<Envelope>(first.payload.as_str())
        .ok()
        .filter(|envelope| envelope.version == VERSION);
    Some(match decoded {
        Some(Envelope {
            event: QuarantineEvent::Quarantined { reason },
            ..
        }) => reason,
        None => Report {
            code: "runtime.quarantine.unreadable".into(),
            message: "the quarantine record doesn't decode".into(),
            hint: None,
        },
    })
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

    #[test]
    fn a_recorded_reason_reads_back() {
        let reason = Report {
            code: "persistent.unrecoverable.undecodable_event".into(),
            message: "event at position 3 doesn't decode".into(),
            hint: None,
        };
        let payload = encode(QuarantineEvent::Quarantined {
            reason: reason.clone(),
        })
        .unwrap();
        assert_eq!(super::reason(&[stored(payload)]), Some(reason));
    }

    #[test]
    fn no_live_event_means_not_quarantined() {
        assert_eq!(reason(&Replay::empty().events), None);
    }

    #[test]
    fn an_unreadable_record_still_quarantines() {
        let payload =
            EventPayload::encode(&serde_json::json!({"version": 9, "event": {}})).unwrap();
        assert_eq!(
            reason(&[stored(payload)]).map(|report| report.code),
            Some("runtime.quarantine.unreadable".into())
        );
    }
}
