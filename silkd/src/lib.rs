//! silkd library surface, exposed so integration tests (and, later, the
//! sandboxd relay's conformance checks) can drive the server in-process.
//! The binary in `main.rs` is a thin wrapper over `vsock::serve`.

pub mod exec;
pub mod find;
pub mod forward;
pub mod fs;
pub mod git;
pub mod net;
pub mod proc;
pub mod proto;
pub mod pty;
pub mod server;
pub mod session;
pub mod sysutil;
pub mod tree;
pub mod vsock;
pub mod watch;
