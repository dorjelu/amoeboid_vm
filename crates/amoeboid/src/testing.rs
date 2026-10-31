//! Test support shared across modules.

#![allow(
    clippy::disallowed_types,
    reason = "test fixtures read files by raw path"
)]

/// A golden file under `tests/golden/`, parsed as JSON.
pub fn golden_json(name: &str) -> serde_json::Value {
    serde_json::from_str(&golden_text(name)).unwrap()
}

/// A golden file under `tests/golden/`, as text.
pub fn golden_text(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    std::fs::read_to_string(path).unwrap()
}

/// Held by `adapters::process::spawn` around each fork (in test builds) and by every test that
/// holds a lock fd. A fork copies all open fds until its child
/// execs (`O_CLOEXEC` acts at exec, not fork), so a parallel fork can keep a dropped lock alive.
pub fn forks() -> std::sync::MutexGuard<'static, ()> {
    static FORKS: std::sync::Mutex<()> = std::sync::Mutex::new(());
    FORKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// An actor that forwards every message it gets into a channel: stands in for a parent (the
/// twin, Root) in tests of a child actor.
pub struct Probe<M>(std::marker::PhantomData<fn() -> M>);

impl<M: ractor::Message> ractor::Actor for Probe<M> {
    type Msg = M;
    type State = tokio::sync::mpsc::UnboundedSender<M>;
    type Arguments = tokio::sync::mpsc::UnboundedSender<M>;

    fn pre_start(
        &self,
        _: ractor::ActorRef<M>,
        sender: Self::Arguments,
    ) -> impl Future<Output = Result<Self::State, ractor::ActorProcessingErr>> + Send {
        std::future::ready(Ok(sender))
    }

    fn handle(
        &self,
        _: ractor::ActorRef<M>,
        message: M,
        sender: &mut Self::State,
    ) -> impl Future<Output = Result<(), ractor::ActorProcessingErr>> + Send {
        std::future::ready(
            sender
                .send(message)
                .map_err(|_| "probe receiver dropped".into()),
        )
    }
}

/// A probe and the receiving end of its channel.
pub async fn probe<M: ractor::Message>()
-> (ractor::ActorRef<M>, tokio::sync::mpsc::UnboundedReceiver<M>) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let (actor, _) = ractor::Actor::spawn(None, Probe(std::marker::PhantomData), sender)
        .await
        .unwrap();
    (actor, receiver)
}

/// The next message, or a failed test after 10 seconds.
pub async fn next<M>(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<M>) -> M {
    tokio::time::timeout(std::time::Duration::from_secs(10), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

/// Writes an executable script at `path`, creating its parents. Held under `forks()`: a fork
/// while the write descriptor is open would make executing it fail with ETXTBSY.
pub fn executable(path: &std::path::Path, script: &str) {
    use std::os::unix::fs::PermissionsExt as _;
    let _forks = forks();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, script).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}
