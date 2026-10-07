//! Regression test for #65: a crawl must exit on its own once the site is
//! exhausted, instead of idling until --duration or a kill.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Tiny HTTP server: "/" links to /a and /b, which link back to "/".
fn spawn_site() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                if reader.read_line(&mut request_line).is_err() {
                    return;
                }
                let path = request_line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("/")
                    .to_string();
                let mut line = String::new();
                while reader.read_line(&mut line).map(|n| n > 2).unwrap_or(false) {
                    line.clear();
                }
                let (status, body) = match path.as_str() {
                    "/" => (
                        "200 OK",
                        r#"<html><head><title>home</title></head><body><a href="/a">a</a><a href="/b">b</a></body></html>"#,
                    ),
                    "/a" | "/b" => (
                        "200 OK",
                        r#"<html><head><title>leaf</title></head><body><a href="/">home</a></body></html>"#,
                    ),
                    _ => ("404 Not Found", "nope"),
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            });
        }
    });
    port
}

#[test]
fn crawl_exits_after_frontier_drains() {
    let port = spawn_site();
    let data = tempfile::tempdir().unwrap();
    let started = Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_rust_sitemap"))
        .args([
            "crawl",
            "--start-url",
            &format!("http://127.0.0.1:{port}/"),
            "--data-dir",
            data.path().to_str().unwrap(),
            "--workers",
            "4",
            "--ignore-robots",
            "--idle-plateau-secs",
            "1",
            "--idle-grace-secs",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn crawler");

    let deadline = Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "crawler exited with {status}");
            break;
        }
        if started.elapsed() > deadline {
            let _ = child.kill();
            panic!("crawl did not exit within {deadline:?} after the site was exhausted (#65)");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        data.path().join("sitemap.jsonl").exists(),
        "crawl should finalize and export on its own"
    );
}
