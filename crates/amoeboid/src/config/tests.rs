#![allow(clippy::disallowed_types, reason = "test fixtures build raw paths")]

use std::num::NonZeroU16;
use std::path::PathBuf;

use proptest::prelude::*;

use super::daemon::{self, DaemonConfig, RestartConfig, RuntimeTimeouts};
use super::run::{
    self, GuestConfig, NewVmConfig, NixConfigSource, PresetName, RunConfig, SshConfig,
    TimeoutsConfig,
};
use super::{ConfigErrors, Seconds};
use crate::domain::account::UserName;
use crate::domain::path::AbsolutePath;
use crate::domain::port::LoopbackPort;
use crate::domain::relative_path::RelativePath;
use crate::domain::share::{HostDirectory, Mode, ShareRequest};

fn home() -> AbsolutePath {
    AbsolutePath::parse(PathBuf::from("/home/someone")).unwrap()
}

/// Path segments, including `:` and names that look like modes, which the share syntax must
/// keep apart.
fn segment() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-z0-9_-]{1,8}",
        "[a-z]{1,4}:[a-z]{1,4}",
        Just("x:ro".to_owned()),
        Just(".hidden".to_owned()),
    ]
}

/// The home itself, a path under it, or a path elsewhere.
fn path() -> impl Strategy<Value = AbsolutePath> {
    (0..3_u8, prop::collection::vec(segment(), 1..4)).prop_map(|(kind, segments)| {
        let base = match kind {
            0 => return home(),
            1 => "/home/someone".to_owned(),
            _ => "/srv".to_owned(),
        };
        AbsolutePath::parse(PathBuf::from(format!("{base}/{}", segments.join("/")))).unwrap()
    })
}

fn relative_path() -> impl Strategy<Value = RelativePath> {
    prop::collection::vec(segment(), 1..3)
        .prop_map(|segments| RelativePath::parse(&segments.join("/")).unwrap())
}

fn seconds(low: u32, high: u32) -> impl Strategy<Value = Seconds> {
    (low..=high).prop_map(Seconds::new)
}

fn run_configs() -> impl Strategy<Value = RunConfig> {
    let source = prop_oneof![
        "[A-Za-z0-9_-][A-Za-z0-9._-]{0,10}"
            .prop_map(|name| NixConfigSource::Preset(PresetName::parse(name).unwrap())),
        path().prop_map(NixConfigSource::Directory),
    ];
    let share = (path(), prop::bool::ANY).prop_map(|(path, read_only)| ShareRequest {
        directory: HostDirectory::parse(path).unwrap(),
        mode: if read_only {
            Mode::ReadOnly
        } else {
            Mode::ReadWrite
        },
    });
    (
        prop::option::of("[a-z_][a-z0-9_-]{0,10}".prop_map(|name| UserName::parse(name).unwrap())),
        prop::collection::vec(relative_path(), 0..3),
        source,
        prop::collection::vec(share, 0..3),
        prop::option::of(1..=u16::MAX),
        seconds(1, 86_400),
        seconds(1, 3_600),
    )
        .prop_map(
            |(user, home_files, base_nix_config, default_shares, port, boot, powerdown)| {
                RunConfig {
                    guest: GuestConfig { user, home_files },
                    new_vm: NewVmConfig {
                        base_nix_config,
                        default_shares,
                    },
                    ssh: SshConfig {
                        port: port.and_then(NonZeroU16::new).map(LoopbackPort::new),
                    },
                    timeouts: TimeoutsConfig { boot, powerdown },
                }
            },
        )
}

fn daemon_configs() -> impl Strategy<Value = DaemonConfig> {
    (
        prop::bool::ANY,
        prop::option::of(path()),
        0..=100_u32,
        seconds(1, 86_400),
        (0..=3_600_u32, 0..=3_600_u32),
        seconds(1, 600),
        seconds(1, 600),
    )
        .prop_map(
            |(linger, journal, max_restarts, window, (first, max), status, persist)| DaemonConfig {
                linger,
                journal,
                restart: RestartConfig {
                    max_restarts,
                    window,
                    backoff_first: Seconds::new(first.min(max)),
                    backoff_max: Seconds::new(first.max(max)),
                },
                timeouts: RuntimeTimeouts { status, persist },
            },
        )
}

proptest! {
    #[test]
    fn a_run_config_survives_encode_then_decode(config in run_configs()) {
        let text = run::encode(&config, &home());
        prop_assert_eq!(run::decode(&text, &home()), Ok(config));
    }

    #[test]
    fn a_daemon_config_survives_encode_then_decode(config in daemon_configs()) {
        let text = daemon::encode(&config, &home());
        prop_assert_eq!(daemon::decode(&text, &home()), Ok(config));
    }

    #[test]
    fn encoding_is_canonical(config in run_configs()) {
        let text = run::encode(&config, &home());
        let again = run::encode(&run::decode(&text, &home()).unwrap(), &home());
        prop_assert_eq!(again, text);
    }
}

fn problems(result: Result<impl std::fmt::Debug, ConfigErrors>) -> Vec<(String, String)> {
    match result {
        Ok(value) => vec![("unexpectedly valid".into(), format!("{value:?}"))],
        Err(errors) => errors
            .problems
            .iter()
            .map(|problem| (problem.key.clone(), problem.message.clone()))
            .collect(),
    }
}

#[test]
fn an_empty_file_is_the_defaults() {
    assert_eq!(run::decode("", &home()), Ok(RunConfig::default()));
    assert_eq!(daemon::decode("", &home()), Ok(DaemonConfig::default()));
}

#[test]
fn every_problem_is_reported_at_once_with_its_key() {
    let text = r#"
        surprise = 1
        [guest]
        user = "Root"
        home_files = ["/etc/passwd", ".gitconfig", "../x"]
        [new_vm]
        base_nix_config = "not a name"
        default_shares = ["relative/dir", "~/fine:ro"]
        [ssh]
        port = 70000
        [timeouts]
        boot_seconds = 0
        powerdown_seconds = "30"
    "#;
    let keys: Vec<String> = problems(run::decode(text, &home()))
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(
        keys,
        [
            "guest.user",
            "guest.home_files[0]",
            "guest.home_files[2]",
            "new_vm.base_nix_config",
            "new_vm.default_shares[0]",
            "ssh.port",
            "timeouts.boot_seconds",
            "timeouts.powerdown_seconds",
            "surprise",
        ]
    );
}

#[test]
fn a_syntax_error_is_one_problem_without_a_key() {
    let found = problems(run::decode("[guest\nuser = 1", &home()));
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].0, "");
}

#[test]
fn paths_under_home_are_written_with_a_tilde_and_others_as_they_are() {
    let config = RunConfig {
        new_vm: NewVmConfig {
            base_nix_config: NixConfigSource::Directory(
                AbsolutePath::parse(PathBuf::from("/home/someone/presets/mine")).unwrap(),
            ),
            default_shares: vec![
                ShareRequest {
                    directory: HostDirectory::parse(home()).unwrap(),
                    mode: Mode::ReadOnly,
                },
                ShareRequest {
                    directory: HostDirectory::parse(
                        AbsolutePath::parse(PathBuf::from("/srv/data")).unwrap(),
                    )
                    .unwrap(),
                    mode: Mode::ReadWrite,
                },
            ],
        },
        ..RunConfig::default()
    };
    let text = run::encode(&config, &home());
    assert!(
        text.contains(r#"base_nix_config = "~/presets/mine""#),
        "{text}"
    );
    assert!(
        text.contains(r#"default_shares = ["~:ro", "/srv/data:rw"]"#),
        "{text}"
    );
    assert!(!text.contains("/home/someone"), "{text}");
}

#[test]
fn a_bare_name_is_a_preset_and_anything_with_a_slash_or_tilde_a_directory() {
    let decoded = |text: &str| {
        run::decode(&format!("[new_vm]\nbase_nix_config = {text:?}"), &home())
            .map(|config| config.new_vm.base_nix_config)
    };
    assert_eq!(
        decoded("work"),
        Ok(NixConfigSource::Preset(
            PresetName::parse("work".into()).unwrap()
        ))
    );
    assert_eq!(
        decoded("~/nix/mine"),
        Ok(NixConfigSource::Directory(
            AbsolutePath::parse(PathBuf::from("/home/someone/nix/mine")).unwrap()
        ))
    );
    assert!(decoded("~other/nix").is_err());
}

#[test]
fn backoff_first_may_not_exceed_backoff_max() {
    let text = "[restart]\nbackoff_first_seconds = 40\nbackoff_max_seconds = 30";
    assert_eq!(
        problems(daemon::decode(text, &home())),
        [(
            "restart.backoff_first_seconds".to_owned(),
            "40 is more than backoff_max_seconds (30)".to_owned()
        )]
    );
}

#[test]
fn preset_names_are_one_directory_name() {
    for bad in ["", ".", "..", "a/b", "a b", &"x".repeat(65)] {
        assert!(PresetName::parse(bad.to_owned()).is_err(), "{bad:?}");
    }
    for good in ["default", "rust-1.9", "_x"] {
        assert!(PresetName::parse(good.to_owned()).is_ok(), "{good:?}");
    }
}

#[test]
fn the_canonical_files_read_as_documented() {
    let config = RunConfig {
        guest: GuestConfig {
            user: None,
            home_files: vec![RelativePath::parse(".gitconfig").unwrap()],
        },
        new_vm: NewVmConfig {
            base_nix_config: NixConfigSource::Preset(PresetName::default_preset()),
            default_shares: vec![ShareRequest {
                directory: HostDirectory::parse(
                    AbsolutePath::parse(PathBuf::from("/home/someone/dev/app")).unwrap(),
                )
                .unwrap(),
                mode: Mode::ReadWrite,
            }],
        },
        ..RunConfig::default()
    };
    assert_eq!(
        run::encode(&config, &home()),
        "# amoeboid run configuration. The runtime reads it at every start; a change applies \
         to the next one.\n# Paths: ~/… is your home, or absolute.\n\
         [guest]\nhome_files = [\".gitconfig\"]\n\n\
         [new_vm]\nbase_nix_config = \"default\"\ndefault_shares = [\"~/dev/app:rw\"]\n\n\
         [ssh]\nport = 0\n\n\
         [timeouts]\nboot_seconds = 120\npowerdown_seconds = 30\n"
    );
}
