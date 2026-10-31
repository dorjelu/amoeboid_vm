//! Byte-exact output against the goldens carried over from the Python launcher.

#![allow(
    clippy::disallowed_types,
    reason = "test fixtures list golden files by raw path"
)]

use std::num::NonZeroU16;

use serde_json::Value;

use super::argv::{self, Argv, IdMap};
use super::files::{host_module, known_hosts, mounts_table, nix_string};
use super::qemu;
use super::share::virtiofs_shares;
use crate::domain::account::{GroupId, GuestHome, UserId, UserName};
use crate::domain::host_key::HostPublicKey;
use crate::domain::path::AbsolutePath;
use crate::domain::port::LoopbackPort;
use crate::domain::share::{HostDirectory, ShareRequest, Shares};
use crate::domain::vm_name::VmName;
use crate::testing::{golden_json, golden_text};

fn absolute(value: &Value) -> AbsolutePath {
    AbsolutePath::parse(value.as_str().unwrap().into()).unwrap()
}

fn strings(argv: &Argv) -> Vec<String> {
    argv.words()
        .map(|word| word.to_str().unwrap().to_owned())
        .collect()
}

fn expected(value: &Value) -> Vec<String> {
    serde_json::from_value(value.clone()).unwrap()
}

/// The golden's mounts as validated shares, keeping the golden's order (they are already
/// ordered and valid).
fn shares(mounts: &Value) -> Shares {
    let requests = mounts
        .as_array()
        .unwrap()
        .iter()
        .map(|mount| ShareRequest {
            directory: HostDirectory::parse(absolute(&mount["path"])).unwrap(),
            mode: serde_json::from_value(mount["mode"].clone()).unwrap(),
        })
        .collect();
    Shares::validate(requests, &[], |_| true).unwrap()
}

fn port(value: &Value) -> LoopbackPort {
    let number = u16::try_from(value.as_u64().unwrap()).unwrap();
    LoopbackPort::new(NonZeroU16::new(number).unwrap())
}

/// Golden case names in `directory` that have an input file with `extension`.
fn cases(directory: &str, extension: &str) -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(directory);
    let mut names: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter_map(|name| name.strip_suffix(extension).map(str::to_owned))
        .collect();
    names.sort();
    assert_ne!(names, Vec::<String>::new());
    names
}

#[test]
fn nix_build_argv_matches_the_golden() {
    for case in golden_json("nix_build_argv.json").as_array().unwrap() {
        let input = &case["input"];
        let argv = argv::nix_build(
            &absolute(&input["flake"]),
            &absolute(&input["host_module"]),
            &absolute(&input["out_link"]),
        );
        assert_eq!(
            strings(&argv),
            expected(&case["output"]),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn ssh_argv_matches_the_golden() {
    for case in golden_json("ssh_argv.json").as_array().unwrap() {
        let input = &case["input"];
        let alias = input["alias"].as_str().unwrap();
        let vm = VmName::parse(alias.strip_prefix("amoeboid-").unwrap().into()).unwrap();
        let command: Vec<String> = serde_json::from_value(input["command"].clone()).unwrap();
        let argv = argv::ssh(
            &vm,
            &UserName::parse(input["user"].as_str().unwrap().into()).unwrap(),
            port(&input["port"]),
            &absolute(&input["known_hosts"]),
            &command,
        );
        assert_eq!(
            strings(&argv),
            expected(&case["output"]),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn shares_and_virtiofsd_argv_match_the_golden() {
    let id = |value: &Value| u32::try_from(value.as_u64().unwrap()).unwrap();
    for case in golden_json("shares.json").as_array().unwrap() {
        let input = &case["input"];
        let list = virtiofs_shares(&absolute(&input["guest_state"]), &shares(&input["mounts"]));
        let actual: Vec<Value> = list
            .iter()
            .map(|share| {
                serde_json::json!({
                    "role": share.role().name(), "socket": share.socket(),
                    "source": share.source.to_str(), "mode": share.mode.as_str(),
                })
            })
            .collect();
        assert_eq!(
            Value::from(actual),
            case["output"]["shares"],
            "{}",
            case["name"]
        );
        let ids = IdMap {
            guest_user: UserId::new(id(&input["guest_uid"])),
            guest_group: GroupId::new(id(&input["guest_gid"])),
            host_user: UserId::new(id(&input["host_uid"])),
            host_group: GroupId::new(id(&input["host_gid"])),
        };
        let argvs: Vec<Vec<String>> = list
            .iter()
            .map(|share| {
                strings(&argv::virtiofsd(
                    &absolute(&input["binary"]),
                    share,
                    &absolute(&input["runtime"]),
                    ids,
                ))
            })
            .collect();
        let expected: Vec<Vec<String>> =
            serde_json::from_value(case["output"]["virtiofsd_argv"].clone()).unwrap();
        assert_eq!(argvs, expected, "{}", case["name"]);
    }
}

#[test]
fn qemu_arguments_match_the_golden() {
    for name in cases("qemu_args", ".json") {
        let input = golden_json(&format!("qemu_args/{name}.json"));
        let text = qemu::arguments(&shares(&input["mounts"]), port(&input["ssh_port"]));
        assert_eq!(
            text,
            golden_text(&format!("qemu_args/{name}.txt")),
            "{name}"
        );
    }
}

#[test]
fn qemu_arguments_never_contain_whitespace_inside_a_word() {
    let input = golden_json("qemu_args/spaces_unicode_port1024.json");
    let text = qemu::arguments(&shares(&input["mounts"]), port(&input["ssh_port"]));
    assert!(
        text.trim_end()
            .split(' ')
            .all(|word| !word.is_empty() && !word.contains(char::is_whitespace))
    );
}

#[test]
fn mounts_table_matches_the_golden() {
    for name in cases("mounts_table", ".json") {
        let input = golden_json(&format!("mounts_table/{name}.json"));
        let table = mounts_table(&shares(&input["mounts"]));
        assert_eq!(
            table,
            golden_text(&format!("mounts_table/{name}.tsv")),
            "{name}"
        );
    }
}

#[test]
fn host_module_matches_the_golden() {
    for name in cases("host_nix", ".json") {
        let input = golden_json(&format!("host_nix/{name}.json"));
        let id = |value: &Value| u32::try_from(value.as_u64().unwrap()).unwrap();
        let text = host_module(
            &UserName::parse(input["user"].as_str().unwrap().into()).unwrap(),
            UserId::new(id(&input["uid"])),
            GroupId::new(id(&input["gid"])),
            &GuestHome::parse(input["home"].as_str().unwrap().into()).unwrap(),
        );
        assert_eq!(text, golden_text(&format!("host_nix/{name}.nix")), "{name}");
    }
}

#[test]
fn nix_strings_escape_backslash_quote_and_interpolation() {
    assert_eq!(nix_string(r#"a\b"c${d}$e"#), r#""a\\b\"c\${d}$e""#);
}

#[test]
fn known_hosts_pins_the_key_under_the_vm_alias() {
    let key = HostPublicKey::parse("ssh-ed25519 AAAAkey comment").unwrap();
    let vm = VmName::parse("work".into()).unwrap();
    assert_eq!(
        known_hosts(&vm, &key),
        "amoeboid-work ssh-ed25519 AAAAkey\n"
    );
}
