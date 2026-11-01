//! Root's operations table (0003 §Protocol): per operation, the VM, its kind, the progress so
//! far, the final reply once there is one, and the sessions attached to it. A request repeating
//! a known operation id attaches instead of running again; `Subscribe` re-attaches. Kept in
//! memory until [`RETENTION`] after the operation finishes.

use std::collections::BTreeMap;
use std::time::Duration;

use ractor::DerivedActorRef;
use tokio::time::Instant;

use crate::domain::vm_name::VmName;
use crate::protocol::{EventSequence, OperationId, ProgressEvent, ReplyBody, ServerFrame};

/// How long a finished operation stays attachable.
pub const RETENTION: Duration = Duration::from_secs(600);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OperationKind {
    Start,
    Stop,
    Vacuum,
    Release,
}

pub struct Operation {
    pub vm: VmName,
    pub kind: OperationKind,
    log: Vec<ProgressEvent>,
    outcome: Option<ReplyBody>,
    subscribers: Vec<DerivedActorRef<ServerFrame>>,
    finished: Option<Instant>,
}

impl Operation {
    #[must_use]
    pub fn finished(&self) -> bool {
        self.outcome.is_some()
    }
}

#[derive(Default)]
pub struct Operations {
    table: BTreeMap<OperationId, Operation>,
}

impl Operations {
    #[must_use]
    pub fn get(&self, operation: OperationId) -> Option<&Operation> {
        self.table.get(&operation)
    }

    /// Registers a new operation with its requester attached.
    pub fn begin(
        &mut self,
        operation: OperationId,
        vm: VmName,
        kind: OperationKind,
        requester: DerivedActorRef<ServerFrame>,
    ) {
        self.table.insert(
            operation,
            Operation {
                vm,
                kind,
                log: Vec::new(),
                outcome: None,
                subscribers: vec![requester],
                finished: None,
            },
        );
    }

    /// Attaches `session`: the progress from `from` (all when `None`), then the reply if there
    /// is one, else everything that follows. `false` when the operation is unknown.
    pub fn attach(
        &mut self,
        operation: OperationId,
        from: Option<EventSequence>,
        session: DerivedActorRef<ServerFrame>,
    ) -> bool {
        let Some(entry) = self.table.get_mut(&operation) else {
            return false;
        };
        let start = from.map_or(0, EventSequence::index);
        for (index, event) in entry.log.iter().enumerate().skip(start) {
            if let Some(sequence) = EventSequence::of_index(index) {
                let _ = session.send_message(ServerFrame::Event {
                    operation,
                    sequence,
                    event: *event,
                });
            }
        }
        match &entry.outcome {
            Some(body) => {
                let _ = session.send_message(ServerFrame::Reply {
                    operation,
                    body: body.clone(),
                });
            }
            None => entry.subscribers.push(session),
        }
        true
    }

    /// Records a step and sends it to everyone attached; sessions that are gone are dropped.
    pub fn progress(&mut self, operation: OperationId, event: ProgressEvent) {
        let Some(entry) = self.table.get_mut(&operation) else {
            return;
        };
        if entry.outcome.is_some() {
            return;
        }
        let Some(sequence) = EventSequence::of_index(entry.log.len()) else {
            return;
        };
        entry.log.push(event);
        entry.subscribers.retain(|session| {
            session
                .send_message(ServerFrame::Event {
                    operation,
                    sequence,
                    event,
                })
                .is_ok()
        });
    }

    /// Records the reply and sends it to everyone attached. A second reply is ignored.
    pub fn finish(&mut self, operation: OperationId, body: ReplyBody, now: Instant) {
        let Some(entry) = self.table.get_mut(&operation) else {
            return;
        };
        if entry.outcome.is_some() {
            return;
        }
        for session in entry.subscribers.drain(..) {
            let _ = session.send_message(ServerFrame::Reply {
                operation,
                body: body.clone(),
            });
        }
        entry.outcome = Some(body);
        entry.finished = Some(now);
    }

    /// Unfinished operations of `vm` of one of `kinds`.
    #[must_use]
    pub fn unfinished(&self, vm: &VmName, kinds: &[OperationKind]) -> Vec<OperationId> {
        self.table
            .iter()
            .filter(|(_, entry)| {
                entry.outcome.is_none() && entry.vm == *vm && kinds.contains(&entry.kind)
            })
            .map(|(operation, _)| *operation)
            .collect()
    }

    /// Drops operations finished more than [`RETENTION`] before `now`.
    pub fn sweep(&mut self, now: Instant) {
        self.table.retain(|_, entry| {
            entry
                .finished
                .is_none_or(|finished| now.saturating_duration_since(finished) < RETENTION)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{RuntimeReply, StopStep};

    async fn session() -> (
        DerivedActorRef<ServerFrame>,
        tokio::sync::mpsc::UnboundedReceiver<ServerFrame>,
    ) {
        let (probe, frames) = crate::testing::probe::<ServerFrame>().await;
        (probe.get_derived(), frames)
    }

    fn vm() -> VmName {
        VmName::parse("work".into()).unwrap()
    }

    fn pong() -> ReplyBody {
        ReplyBody::Runtime(RuntimeReply::Pong)
    }

    #[tokio::test]
    async fn a_late_session_gets_the_progress_from_where_it_asks_then_the_reply() {
        let mut operations = Operations::default();
        let operation = OperationId::mint();
        let (requester, mut first) = session().await;
        operations.begin(operation, vm(), OperationKind::Stop, requester);
        operations.progress(operation, ProgressEvent::Stop(StopStep::PoweringOff));
        operations.progress(operation, ProgressEvent::Stop(StopStep::PoweringOff));
        operations.finish(operation, pong(), Instant::now());
        let (late, mut second) = session().await;
        assert!(operations.attach(operation, EventSequence::of_index(1), late));
        assert!(matches!(
            crate::testing::next(&mut second).await,
            ServerFrame::Event { sequence, .. } if sequence == EventSequence::of_index(1).unwrap()
        ));
        assert!(matches!(
            crate::testing::next(&mut second).await,
            ServerFrame::Reply { .. }
        ));
        for _ in 0..2 {
            assert!(matches!(
                crate::testing::next(&mut first).await,
                ServerFrame::Event { .. }
            ));
        }
        assert!(matches!(
            crate::testing::next(&mut first).await,
            ServerFrame::Reply { .. }
        ));
        assert!(!operations.attach(OperationId::mint(), None, session().await.0));
    }

    #[tokio::test(start_paused = true)]
    async fn finished_operations_are_dropped_after_the_retention_and_unfinished_ones_kept() {
        let mut operations = Operations::default();
        let (done, kept) = (OperationId::mint(), OperationId::mint());
        operations.begin(done, vm(), OperationKind::Stop, session().await.0);
        operations.begin(kept, vm(), OperationKind::Start, session().await.0);
        operations.finish(done, pong(), Instant::now());
        tokio::time::advance(RETENTION.checked_sub(Duration::from_secs(1)).unwrap()).await;
        operations.sweep(Instant::now());
        assert!(operations.get(done).is_some());
        tokio::time::advance(Duration::from_secs(2)).await;
        operations.sweep(Instant::now());
        assert!(operations.get(done).is_none());
        assert!(operations.get(kept).is_some());
    }
}
