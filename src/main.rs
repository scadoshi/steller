//! Binary entry point. Delegates to [`Server::run`]; any error surfaces as a single
//! line on stderr and a failing exit code, which is what a service manager reads to
//! decide whether to restart. A clean shutdown via signal, stdin EOF, `quit`, or
//! `exit` returns success.

use std::process::ExitCode;
use steller::inbound::server::Server;

fn main() -> ExitCode {
    if let Err(e) = Server::run() {
        eprintln!("Failed to run: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
