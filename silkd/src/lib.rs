//! silkd library surface, exposed so integration tests (and, later, the
//! sandboxd relay's conformance checks) can drive the server in-process.
//! The binary in `main.rs` is a thin wrapper over `vsock::serve`.

pub mod exec;
pub mod proc;
pub mod proto;
pub mod server;
pub mod sysutil;
pub mod vsock;
