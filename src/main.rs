//! Binary entry point. Delegates to [`Server::run`]; any error surfaces as a single
//! line on stderr. A non-zero exit is not used: the loop returns `Ok` on clean
//! shutdown via stdin EOF, `quit`, or `exit`.

use steller::inbound::server::Server;

fn main() {
    if let Err(e) = Server::run() {
        eprintln!("Failed to run: {e}");
    }
}
