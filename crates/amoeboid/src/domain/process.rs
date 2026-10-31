//! Process identity: what proves a process is the one we started. A process id alone is not
//! enough (ids are reused); with the start time since boot and the boot id it is unique.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use super::lifecycle::{Generation, Incarnation};
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

/// Marks one spawn: `<vm>/<incarnation>/<generation>/<role>`, set as `AMOEBOID_NONCE` in the
/// process environment. Computed from the twin's history, so a process whose identity was never
/// recorded can be found again without anything persisted before its spawn.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Nonce(String);

impl Nonce {
    pub const VARIABLE: &'static str = "AMOEBOID_NONCE";

    #[must_use]
    pub fn of(
        vm: &VmName,
        incarnation: Incarnation,
        generation: Generation,
        role: &ProcessRole,
    ) -> Self {
        Self(format!("{vm}/{incarnation}/{generation}/{}", role.name()))
    }

    /// The environment entry, `AMOEBOID_NONCE=<nonce>`.
    #[must_use]
    pub fn environment_entry(&self) -> String {
        format!("{}={}", Self::VARIABLE, self.0)
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.0
    }
}

/// A process of a VM, typed per backend. The twin keeps roles as keys and never needs to look
/// inside a backend's variant; a new backend adds a variant here.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessRole {
    Local(LocalRole),
}

/// The processes of the local qemu backend.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalRole {
    Qemu,
    Virtiofsd(SharedDirectory),
}

/// What one virtiofsd serves; fixes its socket (`ro-store.sock`, `state.sock`, `d1.sock`) and
/// guest tag, both part of the contract with `nix/base.nix`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedDirectory {
    /// The host's `/nix/store`, read-only.
    NixStore,
    /// The VM's guest-writable state directory.
    GuestState,
    /// The requested share at this index (from 0), tag `d{index + 1}`.
    Requested(u16),
}

impl SharedDirectory {
    /// `ro-store`, `state`, `d1`, ...
    #[must_use]
    pub fn name(self) -> String {
        match self {
            Self::NixStore => "ro-store".into(),
            Self::GuestState => "state".into(),
            Self::Requested(index) => format!("d{}", u32::from(index) + 1),
        }
    }
}

impl ProcessRole {
    /// `qemu`, `virtiofsd-d1`, ...: names the scope unit and the nonce's last part.
    #[must_use]
    pub fn name(&self) -> String {
        match self {
            Self::Local(LocalRole::Qemu) => "qemu".into(),
            Self::Local(LocalRole::Virtiofsd(directory)) => {
                format!("virtiofsd-{}", directory.name())
            }
        }
    }
}

/// The transient scope unit of one VM process: `amoeboid-<vm>-<role>`, under `amoeboid.slice`.
/// Fixed per VM and role, so a second instance can't be started while the first runs.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ScopeName(String);

impl ScopeName {
    #[must_use]
    pub fn of(vm: &VmName, role: &ProcessRole) -> Self {
        Self(format!("amoeboid-{vm}-{}", role.name()))
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
    fn scope_names_and_nonces_join_vm_and_role() {
        let vm = VmName::parse("work".into()).unwrap();
        let share = ProcessRole::Local(LocalRole::Virtiofsd(SharedDirectory::Requested(0)));
        let qemu = ProcessRole::Local(LocalRole::Qemu);
        assert_eq!(ScopeName::of(&vm, &qemu).as_str(), "amoeboid-work-qemu");
        assert_eq!(
            ScopeName::of(&vm, &share).as_str(),
            "amoeboid-work-virtiofsd-d1"
        );
        let incarnation = Incarnation::mint();
        let nonce = Nonce::of(&vm, incarnation, Generation::FIRST, &share);
        assert_eq!(nonce.value(), format!("work/{incarnation}/1/virtiofsd-d1"));
    }

    #[test]
    fn roles_serialise_as_nested_tags() {
        let role = ProcessRole::Local(LocalRole::Virtiofsd(SharedDirectory::Requested(0)));
        assert_eq!(
            serde_json::to_value(&role).unwrap(),
            serde_json::json!({"local": {"virtiofsd": {"requested": 0}}})
        );
    }
}
