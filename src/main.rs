//! Any error from [`Server::run`] prints one line to stderr. The exit code is 0 either
//! way; a clean shutdown (stdin EOF, `quit`, or `exit`) returns `Ok`.

use steller::inbound::server::Server;

fn main() {
    if let Err(e) = Server::run() {
        eprintln!("Failed to run: {e}");
    }
}
