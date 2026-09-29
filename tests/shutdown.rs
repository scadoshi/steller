//! Runs the real binary the way a service manager does: no terminal on stdin, stopped
//! with SIGTERM. Both are outside what the in-process tests can reach.
//!
//! Binds the server's fixed port, so this cannot run beside a live steller.

// clippy.toml's in-tests allowances only reach `#[test]` and `#[cfg(test)]`, and the
// helpers below are neither. The whole file is test code, so allow them here.
#![allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]

use std::{
    io::{Read, Write},
    net::TcpStream,
    process::{Child, Command, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

const ADDRESS: &str = "127.0.0.1:3000";

/// Polls until the server answers a PING.
///
/// Connecting is not enough: the listener is bound before the shutdown threads spawn,
/// so the backlog accepts connections even while the server is on its way out. Only a
/// reply proves the accept loop is alive and spawning sessions.
fn wait_until_serving(within: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < within {
        if ping() {
            return true;
        }
        sleep(Duration::from_millis(50));
    }
    false
}

fn ping() -> bool {
    let Ok(mut stream) = TcpStream::connect(ADDRESS) else {
        return false;
    };
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    if stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_err() {
        return false;
    }
    let mut buf = [0u8; 16];
    match stream.read(&mut buf) {
        Ok(n) => buf[..n].starts_with(b"+PONG"),
        Err(_) => false,
    }
}

/// Polls until the process exits, killing it and failing if it never does.
fn wait_for_exit(child: &mut Child, within: Duration) -> std::process::ExitStatus {
    let start = Instant::now();
    loop {
        match child.try_wait().unwrap() {
            Some(status) => return status,
            None if start.elapsed() > within => {
                child.kill().ok();
                child.wait().ok();
                panic!("still running {within:?} after SIGTERM");
            }
            None => sleep(Duration::from_millis(50)),
        }
    }
}

#[test]
fn starts_without_a_terminal_and_stops_cleanly_on_sigterm() {
    assert!(
        !ping(),
        "something already answers on {ADDRESS}; stop it and run again"
    );

    let dir = std::env::temp_dir().join(format!("steller-sigterm-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("cache")).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_steller"))
        .current_dir(&dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // stdin is /dev/null, so an unguarded `read_line` returns `Ok(0)` and the server
    // shuts itself down before it ever listens.
    let serving = wait_until_serving(Duration::from_secs(10));
    if !serving {
        child.kill().ok();
        child.wait().ok();
    }
    assert!(serving, "never served a PING");

    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();

    let status = wait_for_exit(&mut child, Duration::from_secs(10));
    assert!(status.success(), "exited with {status}");

    // Only the clean path snapshots on the way out, so this line is the evidence that
    // SIGTERM went through shutdown rather than killing the process where it stood.
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    assert!(
        stdout.contains("cache persisted"),
        "no final snapshot; stdout was:\n{stdout}"
    );

    std::fs::remove_dir_all(&dir).ok();
}
