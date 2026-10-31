//! How long the runtime waits for things (0003 §Time): values handed in, so each test picks its
//! own and production code has no test-only constants.

use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Timing {
    /// From qemu starting to the guest's sshd answering.
    pub boot_timeout: Duration,
    /// How long a status question waits for twins; later answers are dropped.
    pub status_timeout: Duration,
    /// No `Persisted` within this: a persistent actor fails and is restarted.
    pub persist_timeout: Duration,
    /// How long the guest gets to power off after the ACPI request, before qemu is terminated.
    pub powerdown_grace: Duration,
    /// How long qemu gets after SIGTERM before SIGKILL.
    pub terminate_grace: Duration,
    /// How long virtiofsd may take to create its sockets.
    pub socket_wait: Duration,
    /// How long a vacuumed VM's processes get after SIGTERM before SIGKILL.
    pub vacuum_terminate_grace: Duration,
    /// How long the kernel gets to take killed processes away.
    pub vacuum_kill_grace: Duration,
}

impl Timing {
    #[must_use]
    pub fn production() -> Self {
        Self {
            boot_timeout: Duration::from_secs(120),
            status_timeout: Duration::from_secs(5),
            persist_timeout: Duration::from_secs(10),
            powerdown_grace: Duration::from_secs(30),
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
            powerdown_grace: Duration::from_secs(1),
            terminate_grace: Duration::from_secs(1),
            socket_wait: Duration::from_secs(2),
            vacuum_terminate_grace: Duration::from_secs(1),
            ..Self::production()
        }
    }
}
