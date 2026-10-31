//! Process-level waits (0003 §Time, §Configuration): set by the code that builds the runtime,
//! never by a configuration file, so each test picks its own. The timeouts operators tune come
//! from the configuration files instead (status, persist and restart from
//! `daemon_config.toml`; boot and powerdown from `run_config.toml`).

use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ProcessTiming {
    /// How long qemu gets after SIGTERM before SIGKILL.
    pub terminate_grace: Duration,
    /// How long virtiofsd may take to create its sockets.
    pub socket_wait: Duration,
    /// How long a vacuumed VM's processes get after SIGTERM before SIGKILL.
    pub vacuum_terminate_grace: Duration,
    /// How long the kernel gets to take killed processes away.
    pub vacuum_kill_grace: Duration,
}

impl ProcessTiming {
    #[must_use]
    pub fn production() -> Self {
        Self {
            terminate_grace: Duration::from_secs(10),
            socket_wait: Duration::from_secs(10),
            vacuum_terminate_grace: Duration::from_secs(10),
            vacuum_kill_grace: Duration::from_secs(2),
        }
    }

    /// Short graces and waits for tests that run real (stand-in) processes.
    #[cfg(test)]
    #[must_use]
    pub fn quick() -> Self {
        Self {
            terminate_grace: Duration::from_secs(1),
            socket_wait: Duration::from_secs(2),
            vacuum_terminate_grace: Duration::from_secs(1),
            ..Self::production()
        }
    }
}
