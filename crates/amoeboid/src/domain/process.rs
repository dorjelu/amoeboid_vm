//! Process identity: what proves a process is the one we started. A process id alone is not
//! enough (ids are reused); with the start time since boot and the boot id it is unique.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use super::vm_name::VmName;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProcessId(NonZeroU32);

impl ProcessId {
    #[must_use]
    pub fn new(id: NonZeroU32) -> Self {
        Self(id)
    }

    #[must_use]
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

impl std::fmt::Display for ProcessId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// `/proc/sys/kernel/random/boot_id`: a lowercase hyphenated UUID, new on every boot.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BootId(String);

#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
#[error("{0:?} is not a boot id")]
pub struct BadBootId(String);

impl BootId {
    /// # Errors
    ///
    /// Not 36 characters of lowercase hex and hyphens in UUID positions.
    pub fn parse(text: String) -> Result<Self, BadBootId> {
        let valid = text.len() == 36
            && text.char_indices().all(|(index, character)| match index {
                8 | 13 | 18 | 23 => character == '-',
                _ => character.is_ascii_digit() || ('a'..='f').contains(&character),
            });
        if valid {
            Ok(Self(text))
        } else {
            Err(BadBootId(text))
        }
    }
}

impl TryFrom<String> for BootId {
    type Error = BadBootId;
    fn try_from(text: String) -> Result<Self, BadBootId> {
        Self::parse(text)
    }
}

impl From<BootId> for String {
    fn from(id: BootId) -> String {
        id.0
    }
}

/// A process as observed: id, start time in clock ticks since boot (`/proc/PID/stat` field 22),
/// boot id. Equal identities are the same process.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pub process: ProcessId,
    pub start_time: u64,
    pub boot: BootId,
}

/// Marks one spawn: set as `AMOEBOID_NONCE` in the process environment and persisted by the
/// twin before the spawn, so a process whose identity was never recorded can still be found.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Nonce(uuid::Uuid);

impl Nonce {
    #[must_use]
    pub fn mint() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    /// The environment entry, `AMOEBOID_NONCE=<uuid>`.
    #[must_use]
    pub fn environment_entry(&self) -> String {
        format!("{}={}", Self::VARIABLE, self.0)
    }

    pub const VARIABLE: &'static str = "AMOEBOID_NONCE";

    #[must_use]
    pub fn value(&self) -> String {
        self.0.to_string()
    }
}

/// What a VM process does; names its scope unit.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum Role {
    Qemu,
    /// virtiofsd for a share socket (`ro-store`, `state`, `d1`, ...).
    Virtiofsd(String),
}

/// The transient scope unit of one VM process: `amoeboid-<vm>-<role>`, under `amoeboid.slice`.
/// Fixed per VM and role, so a second instance can't be started while the first runs.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ScopeName(String);

impl ScopeName {
    #[must_use]
    pub fn of(vm: &VmName, role: &Role) -> Self {
        let role = match role {
            Role::Qemu => "qemu".to_owned(),
            Role::Virtiofsd(socket) => format!("virtiofsd-{socket}"),
        };
        Self(format!("amoeboid-{vm}-{role}"))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub const SLICE: &str = "amoeboid.slice";

impl crate::diagnostic::Diagnostic for BadBootId {
    fn code(&self) -> &'static str {
        "process.boot_id.invalid"
    }
    fn hint(&self) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boot_ids_are_lowercase_uuids() {
        assert!(BootId::parse("0f3c2e1a-9b8d-4c7e-a6f5-0123456789ab".into()).is_ok());
        assert!(BootId::parse("0F3C2E1A-9B8D-4C7E-A6F5-0123456789AB".into()).is_err());
        assert!(BootId::parse("0f3c2e1a9b8d4c7ea6f50123456789ab".into()).is_err());
    }

    #[test]
    fn scope_names_join_vm_and_role() {
        let vm = VmName::parse("work".into()).unwrap();
        assert_eq!(
            ScopeName::of(&vm, &Role::Qemu).as_str(),
            "amoeboid-work-qemu"
        );
        assert_eq!(
            ScopeName::of(&vm, &Role::Virtiofsd("d1".into())).as_str(),
            "amoeboid-work-virtiofsd-d1"
        );
    }
}
