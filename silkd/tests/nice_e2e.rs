//! A silkd running at a raised priority starts its children at nice 0; its own binary, so no other test reads the raised flag first.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use common::{roundtrip, stdout_body};

#[tokio::test(flavor = "current_thread")]
#[ignore = "needs root"]
async fn a_raised_silkd_starts_exec_children_at_nice_zero() {
    // SAFETY: setpriority only changes this thread's nice value.
    let raised = unsafe { libc::setpriority(libc::PRIO_PROCESS as _, 0, -10) };
    assert_eq!(raised, 0, "raise the test thread");
    let inherited = std::process::Command::new("nice").output().unwrap();
    assert_eq!(String::from_utf8_lossy(&inherited.stdout).trim(), "-10");
    let frames = roundtrip(r#"{"op":"exec","argv":["nice"]}"#).await;
    assert_eq!(stdout_body(&frames).trim(), "0");
}
