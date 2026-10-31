//! Extra qemu arguments, written to `amoeboid-qemu-args` and word-split by the runner script.
//! Built from numbers and fixed words only, so no argument can contain whitespace.

use super::share::tag;
use super::{GUEST_MAC, GUEST_SSH_PORT};
use crate::domain::port::LoopbackPort;
use crate::domain::share::Shares;

/// The file's content, newline-terminated.
#[must_use]
pub fn arguments(shares: &Shares, ssh_port: LoopbackPort) -> String {
    let mut words = vec![
        "-netdev".to_owned(),
        format!("user,id=net0,hostfwd=tcp:127.0.0.1:{ssh_port}-:{GUEST_SSH_PORT}"),
        "-device".to_owned(),
        format!("virtio-net-pci,netdev=net0,mac={GUEST_MAC}"),
    ];
    for index in 0..shares.len() {
        let tag = tag(index);
        let number = index.saturating_add(1);
        words.extend([
            "-chardev".to_owned(),
            format!("socket,id=amo{number},path={tag}.sock"),
            "-device".to_owned(),
            format!("vhost-user-fs-pci,chardev=amo{number},tag={tag}"),
        ]);
    }
    words.join(" ") + "\n"
}
