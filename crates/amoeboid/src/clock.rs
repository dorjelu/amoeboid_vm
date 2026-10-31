//! Wall-clock time, injected (0003 §Time): the runtime reads it only through a [`Clock`] it is
//! given. Monotonic time and timers are tokio's, which tests pause and advance instead.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Timestamp(i64);

impl Timestamp {
    #[must_use]
    pub fn from_milliseconds(milliseconds: i64) -> Self {
        Self(milliseconds)
    }

    #[must_use]
    pub fn milliseconds(self) -> i64 {
        self.0
    }

    /// `self` moved forward by `by`, saturating.
    #[must_use]
    pub fn after(self, by: Duration) -> Self {
        Self(
            self.0
                .saturating_add(i64::try_from(by.as_millis()).unwrap_or(i64::MAX)),
        )
    }
}

/// Where wall time comes from.
#[derive(Clone, Debug)]
pub enum Clock {
    System,
    /// Set and advanced by whoever holds the [`ManualClock`]; tests.
    Manual(ManualClock),
}

impl Clock {
    #[must_use]
    pub fn now(&self) -> Timestamp {
        match self {
            Self::System => Timestamp(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |duration| {
                        i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
                    }),
            ),
            Self::Manual(manual) => manual.now(),
        }
    }
}

/// A clock that moves only when told; clones share the time.
#[derive(Clone, Debug)]
pub struct ManualClock(Arc<Mutex<Timestamp>>);

impl ManualClock {
    #[must_use]
    pub fn at(start: Timestamp) -> Self {
        Self(Arc::new(Mutex::new(start)))
    }

    #[must_use]
    pub fn now(&self) -> Timestamp {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn advance(&self, by: Duration) {
        let mut now = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        *now = now.after(by);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manual_clock_moves_only_when_advanced_and_clones_share_it() {
        let manual = ManualClock::at(Timestamp::from_milliseconds(1_000));
        let clock = Clock::Manual(manual.clone());
        assert_eq!(clock.now(), Timestamp::from_milliseconds(1_000));
        manual.advance(Duration::from_millis(250));
        assert_eq!(clock.now(), Timestamp::from_milliseconds(1_250));
    }

    #[test]
    fn the_system_clock_is_past_2026() {
        assert!(Clock::System.now() > Timestamp::from_milliseconds(1_767_225_600_000));
    }
}
