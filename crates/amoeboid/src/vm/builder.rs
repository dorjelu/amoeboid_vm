//! Builder: renders `host.nix`, prepares the VM's own Nix config (create or rebase), runs
//! `nix build`. Idempotent and stateless: each `Build` runs from scratch, and a newer one
//! replaces a running one (its result would carry an older generation anyway). The build
//! output's layout is fixed by `VmPaths` (`runner()`, `virtiofsd()`), so `Built` carries no
//! paths.

use ractor::{Actor, ActorProcessingErr, ActorRef, DerivedActorRef};
use tokio::task::JoinHandle;

use super::{Account, Generation, NixConfig, bundled};
use crate::adapters::files::{self, FileError};
use crate::adapters::nix::{self, NixError};
use crate::diagnostic::Report;
use crate::domain::path::AbsolutePath;
use crate::domain::relative_path::RelativePath;
use crate::plan;
use crate::runtime::paths::VmPaths;

#[derive(Debug)]
pub enum BuilderCommand {
    Build {
        generation: Generation,
        vm: VmPaths,
        account: Account,
        nix_config: NixConfig,
    },
}

#[derive(Debug)]
pub enum BuilderObservation {
    Built {
        generation: Generation,
    },
    BuildFailed {
        generation: Generation,
        reason: Report,
    },
}

/// How the build itself runs.
#[derive(Clone, Debug)]
pub enum BuildCommand {
    /// `nix build` of the VM's own config with its `host.nix`.
    Nix,
    /// A fixed command instead, for tests.
    #[cfg(test)]
    Scripted(crate::plan::argv::Argv),
}

pub struct BuilderSetup {
    pub report: DerivedActorRef<BuilderObservation>,
    pub command: BuildCommand,
}

/// What to do with the VM's own config directory on this build.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ConfigStep {
    KeepOwn,
    InstallBundled,
    Install(AbsolutePath),
}

/// Pure: `From` always installs (create or rebase); `Existing` keeps the VM's copy, or
/// installs the bundled one when there is none.
#[must_use]
pub fn config_step(nix_config: &NixConfig, own_exists: bool) -> ConfigStep {
    match (nix_config, own_exists) {
        (NixConfig::From(source), _) => ConfigStep::Install(source.clone()),
        (NixConfig::Existing, true) => ConfigStep::KeepOwn,
        (NixConfig::Existing, false) => ConfigStep::InstallBundled,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error(transparent)]
    File(#[from] FileError),
    #[error(transparent)]
    Nix(#[from] NixError),
    #[error("{path} has no flake.nix, so it is not a NixOS configuration amoeboid can build")]
    MissingFlake { path: String },
}

pub struct Builder;

pub struct BuilderState {
    setup: BuilderSetup,
    running: Option<JoinHandle<()>>,
}

impl Actor for Builder {
    type Msg = BuilderCommand;
    type State = BuilderState;
    type Arguments = BuilderSetup;

    fn pre_start(
        &self,
        _myself: ActorRef<BuilderCommand>,
        setup: BuilderSetup,
    ) -> impl Future<Output = Result<BuilderState, ActorProcessingErr>> + Send {
        std::future::ready(Ok(BuilderState {
            setup,
            running: None,
        }))
    }

    fn handle(
        &self,
        _myself: ActorRef<BuilderCommand>,
        message: BuilderCommand,
        state: &mut BuilderState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        let BuilderCommand::Build {
            generation,
            vm,
            account,
            nix_config,
        } = message;
        if let Some(running) = state.running.take() {
            running.abort();
        }
        let report = state.setup.report.clone();
        let command = state.setup.command.clone();
        state.running = Some(tokio::spawn(async move {
            let observation = match build(&vm, &account, &nix_config, &command).await {
                Ok(()) => BuilderObservation::Built { generation },
                Err(error) => BuilderObservation::BuildFailed {
                    generation,
                    reason: Report::of(&error),
                },
            };
            if report.send_message(observation).is_err() {
                tracing::debug!("build result undeliverable: the twin stopped");
            }
        }));
        std::future::ready(Ok(()))
    }

    fn post_stop(
        &self,
        _myself: ActorRef<BuilderCommand>,
        state: &mut BuilderState,
    ) -> impl Future<Output = Result<(), ActorProcessingErr>> + Send {
        if let Some(running) = state.running.take() {
            running.abort();
        }
        std::future::ready(Ok(()))
    }
}

/// `host.nix`, then the config directory, then the build.
async fn build(
    vm: &VmPaths,
    account: &Account,
    nix_config: &NixConfig,
    command: &BuildCommand,
) -> Result<(), BuilderError> {
    files::ensure_directory(vm.state(), 0o755)?;
    let host = plan::files::host_module(
        &account.user,
        account.user_id,
        account.group_id,
        &account.home,
    );
    files::write_atomically(&vm.host_module(), host.as_bytes(), 0o644)?;
    let own = vm.nix_config();
    match config_step(nix_config, own.as_path().is_dir()) {
        ConfigStep::KeepOwn => {}
        ConfigStep::InstallBundled => {
            tracing::info!(vm = %vm.name(), "creating its NixOS config from the bundled one");
            files::replace_directory(&own, &bundled::config())?;
        }
        ConfigStep::Install(source) => {
            tracing::info!(vm = %vm.name(), source = %source.display(), "installing its NixOS config");
            let tree = files::read_tree(&source)?;
            let flake =
                RelativePath::parse("flake.nix").map_err(|_| BuilderError::MissingFlake {
                    path: source.display().to_string(),
                })?;
            if !tree.contains(&flake) {
                return Err(BuilderError::MissingFlake {
                    path: source.display().to_string(),
                });
            }
            files::replace_directory(&own, &tree)?;
        }
    }
    let argv = match command {
        BuildCommand::Nix => plan::argv::nix_build(&own, &vm.host_module(), &vm.build_link()),
        #[cfg(test)]
        BuildCommand::Scripted(argv) => argv.clone(),
    };
    tracing::info!(vm = %vm.name(), "building");
    nix::build(&argv, vm.state(), &vm.build_log()).await?;
    Ok(())
}

impl crate::diagnostic::Diagnostic for BuilderError {
    fn code(&self) -> &'static str {
        match self {
            Self::File(inner) => inner.code(),
            Self::Nix(inner) => inner.code(),
            Self::MissingFlake { .. } => "builder.missing_flake",
        }
    }
    fn hint(&self) -> Option<String> {
        match self {
            Self::File(inner) => inner.hint(),
            Self::Nix(inner) => inner.hint(),
            Self::MissingFlake { .. } => Some(
                "give a directory holding a flake.nix, or leave it out for the bundled one".into(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_types,
        reason = "test fixtures build raw temp paths"
    )]

    use std::time::Duration;

    use super::*;
    use crate::domain::account::{GroupId, GuestHome, UserId, UserName};
    use crate::domain::vm_name::VmName;
    use crate::plan::argv::Argv;
    use crate::runtime::paths::RuntimePaths;

    #[test]
    fn existing_keeps_an_own_config_and_installs_the_bundled_one_otherwise() {
        let source = AbsolutePath::parse("/presets/x".into()).unwrap();
        let from = NixConfig::From(source.clone());
        assert_eq!(config_step(&NixConfig::Existing, true), ConfigStep::KeepOwn);
        assert_eq!(
            config_step(&NixConfig::Existing, false),
            ConfigStep::InstallBundled
        );
        assert_eq!(
            config_step(&from, true),
            ConfigStep::Install(source.clone())
        );
        assert_eq!(config_step(&from, false), ConfigStep::Install(source));
    }

    struct Fixture {
        _temporary: tempfile::TempDir,
        vm: VmPaths,
        builder: ActorRef<BuilderCommand>,
        observations: tokio::sync::mpsc::UnboundedReceiver<BuilderObservation>,
    }

    async fn fixture(script: &str) -> Fixture {
        let temporary = tempfile::tempdir().unwrap();
        let root = AbsolutePath::parse(temporary.path().to_path_buf()).unwrap();
        let paths = RuntimePaths::under(root.join("data"), root.join("run")).unwrap();
        let vm = paths.vm(&VmName::parse("work".into()).unwrap()).unwrap();
        let (twin, observations) = crate::testing::probe::<BuilderObservation>().await;
        let (builder, _) = Actor::spawn(
            None,
            Builder,
            BuilderSetup {
                report: twin.get_derived(),
                command: BuildCommand::Scripted(Argv::for_tests(&["sh", "-c", script])),
            },
        )
        .await
        .unwrap();
        Fixture {
            _temporary: temporary,
            vm,
            builder,
            observations,
        }
    }

    fn account() -> Account {
        Account {
            user: UserName::parse("alice".into()).unwrap(),
            user_id: UserId::new(1000),
            group_id: GroupId::new(100),
            home: GuestHome::parse("/home/alice".into()).unwrap(),
        }
    }

    async fn build_once(fixture: &mut Fixture, nix_config: NixConfig) -> BuilderObservation {
        fixture
            .builder
            .cast(BuilderCommand::Build {
                generation: Generation::FIRST,
                vm: fixture.vm.clone(),
                account: account(),
                nix_config,
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), fixture.observations.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn a_new_vm_gets_host_nix_and_the_bundled_config_then_is_built() {
        let mut fixture = fixture("echo building; exit 0").await;
        let observation = build_once(&mut fixture, NixConfig::Existing).await;
        assert!(
            matches!(observation, BuilderObservation::Built { generation } if generation == Generation::FIRST)
        );
        let host = files::read_to_string(&fixture.vm.host_module()).unwrap();
        assert!(host.contains("amoeboid.user = \"alice\";"));
        assert_eq!(
            files::read_tree(&fixture.vm.nix_config()).unwrap(),
            bundled::config()
        );
        let log = files::read_to_string(&fixture.vm.build_log()).unwrap();
        assert_eq!(log, "building\n");
    }

    #[tokio::test]
    async fn a_failed_build_is_reported_with_its_diagnostic() {
        let mut fixture = fixture("echo broken; exit 1").await;
        let observation = build_once(&mut fixture, NixConfig::Existing).await;
        assert!(matches!(
            observation,
            BuilderObservation::BuildFailed { reason, .. } if reason.code == "nix.build_failed"
        ));
    }

    #[tokio::test]
    async fn a_config_directory_without_a_flake_is_refused() {
        let mut fixture = fixture("exit 0").await;
        let empty = fixture.vm.state().join("empty-preset");
        files::ensure_directory(&empty, 0o755).unwrap();
        let observation = build_once(&mut fixture, NixConfig::From(empty)).await;
        assert!(matches!(
            observation,
            BuilderObservation::BuildFailed { reason, .. } if reason.code == "builder.missing_flake"
        ));
    }
}
