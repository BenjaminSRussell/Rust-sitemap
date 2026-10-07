//! Optional Prometheus `/metrics` endpoint and live operator report (#34, #36).
//!
//! Enabled with `--metrics-addr 127.0.0.1:9100`. Off by default so library and
//! Python embeds stay quiet. Serves:
//! - `GET /metrics`: Prometheus text exposition (version 0.0.4)
//! - `GET /` or `/report`: the HTML crawl report, regenerated on every request
//!   (live per-host budget table)
//! - `GET /healthz`: `ok`

use crate::metrics::{HostStatusCounts, Metrics};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Render all exported metrics in Prometheus text format.
pub fn render_prometheus(m: &Metrics) -> String {
    let s = m.snapshot_totals();
    let mut out = String::with_capacity(2048);

    let counter = |out: &mut String, name: &str, help: &str, v: u64| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {v}");
    };
    let gauge = |out: &mut String, name: &str, help: &str, v: f64| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} gauge");
        let _ = writeln!(out, "{name} {v}");
    };

    counter(
        &mut out,
        "rustmapper_urls_discovered_total",
        "URLs discovered and queued.",
        s.urls_discovered,
    );
    counter(
        &mut out,
        "rustmapper_urls_fetched_total",
        "HTTP fetches completed.",
        s.urls_fetched,
    );
    counter(
        &mut out,
        "rustmapper_urls_processed_total",
        "URLs fully processed.",
        s.urls_processed,
    );
    counter(
        &mut out,
        "rustmapper_urls_failed_total",
        "URLs that failed (non-timeout errors).",
        s.urls_failed,
    );
    counter(
        &mut out,
        "rustmapper_urls_timeout_total",
        "URLs that timed out.",
        s.urls_timeout,
    );
    counter(
        &mut out,
        "rustmapper_throttle_adjustments_total",
        "Governor concurrency adjustments.",
        s.throttle_adjustments,
    );
    gauge(
        &mut out,
        "rustmapper_commit_ewma_ms",
        "Writer commit latency EWMA used by the governor (ms).",
        s.commit_ewma_ms,
    );
    gauge(
        &mut out,
        "rustmapper_throttle_permits_available",
        "Crawler permits available to the governor.",
        m.throttle_permits_held.lock().value(),
    );
    counter(
        &mut out,
        "rustmapper_writer_batches_total",
        "Batches committed by the writer thread.",
        m.writer_batch_count.lock().value,
    );
    counter(
        &mut out,
        "rustmapper_writer_batch_bytes_total",
        "Bytes committed by the writer thread.",
        m.writer_batch_bytes.lock().value,
    );
    counter(
        &mut out,
        "rustmapper_wal_appends_total",
        "WAL records appended.",
        m.wal_append_count.lock().value,
    );
    if let Some(secs) = m.seconds_since_last_discovery() {
        gauge(
            &mut out,
            "rustmapper_seconds_since_last_discovery",
            "Seconds since a new URL was discovered (plateau signal).",
            secs as f64,
        );
    }

    let _ = writeln!(
        out,
        "# HELP rustmapper_http_responses_total Responses by HTTP version."
    );
    let _ = writeln!(out, "# TYPE rustmapper_http_responses_total counter");
    for (ver, v) in [
        ("1.1", m.http_version_h1.lock().value),
        ("2", m.http_version_h2.lock().value),
        ("3", m.http_version_h3.lock().value),
    ] {
        let _ = writeln!(
            out,
            "rustmapper_http_responses_total{{version=\"{ver}\"}} {v}"
        );
    }

    render_host_gauges(&mut out, &m.host_status_counts());
    out
}

fn render_host_gauges(out: &mut String, c: &HostStatusCounts) {
    let _ = writeln!(
        out,
        "# HELP rustmapper_hosts Tracked hosts by politeness status."
    );
    let _ = writeln!(out, "# TYPE rustmapper_hosts gauge");
    for (status, v) in [
        ("ready", c.ready),
        ("delayed", c.delayed),
        ("saturated", c.saturated),
        ("backoff", c.backoff),
        ("blocked", c.blocked),
    ] {
        let _ = writeln!(out, "rustmapper_hosts{{status=\"{status}\"}} {v}");
    }
}

/// Context for the live HTML report.
#[derive(Clone)]
pub struct ReportContext {
    pub start_url: String,
    pub data_dir: String,
}

fn live_report_html(m: &Metrics, ctx: &ReportContext) -> String {
    m.render_html_report(&ctx.start_url, &ctx.data_dir)
        .replacen(
            "<head>",
            "<head><meta http-equiv=\"refresh\" content=\"5\"/>",
            1,
        )
}

fn response(status: &str, content_type: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// Route one request path to a full HTTP response.
pub fn handle_path(path: &str, m: &Metrics, ctx: &ReportContext) -> Vec<u8> {
    let path = path.split('?').next().unwrap_or("/");
    match path {
        "/metrics" => response(
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            &render_prometheus(m),
        ),
        "/" | "/report" => response(
            "200 OK",
            "text/html; charset=utf-8",
            &live_report_html(m, ctx),
        ),
        "/healthz" => response("200 OK", "text/plain", "ok"),
        _ => response("404 Not Found", "text/plain", "not found"),
    }
}

/// Bind and serve in the background. Returns the bound address (useful with port 0).
pub async fn spawn(
    addr: &str,
    metrics: Arc<Metrics>,
    ctx: ReportContext,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                continue;
            };
            let metrics = Arc::clone(&metrics);
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    sock.read(&mut buf),
                )
                .await
                {
                    Ok(Ok(n)) if n > 0 => n,
                    _ => return,
                };
                let req = String::from_utf8_lossy(&buf[..n]);
                let mut parts = req.lines().next().unwrap_or("").split_whitespace();
                let method = parts.next().unwrap_or("");
                let path = parts.next().unwrap_or("/");
                let resp = if method == "GET" || method == "HEAD" {
                    handle_path(path, &metrics, &ctx)
                } else {
                    response("405 Method Not Allowed", "text/plain", "GET only")
                };
                let _ = sock.write_all(&resp).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    Ok(local)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::HostStateSource;
    use crate::state::HostState;
    use dashmap::DashMap;

    fn ctx() -> ReportContext {
        ReportContext {
            start_url: "https://example.com/".into(),
            data_dir: "./data".into(),
        }
    }

    #[test]
    fn exposition_contains_core_metrics() {
        let m = Metrics::new();
        m.urls_processed_total.lock().add(42);
        m.urls_timeout_total.lock().add(3);
        m.throttle_adjustments.lock().add(2);
        m.writer_commit_ewma.lock().update(12.5);
        let src: HostStateSource = Arc::new(DashMap::new());
        let mut bad = HostState::new("bad.test".into());
        bad.failures = 9;
        src.insert("bad.test".into(), bad);
        src.insert("ok.test".into(), HostState::new("ok.test".into()));
        m.register_host_states(src);

        let text = render_prometheus(&m);
        assert!(text.contains("rustmapper_urls_processed_total 42"));
        assert!(text.contains("rustmapper_urls_timeout_total 3"));
        assert!(text.contains("rustmapper_throttle_adjustments_total 2"));
        assert!(text.contains("rustmapper_commit_ewma_ms 12.5"));
        assert!(text.contains("rustmapper_hosts{status=\"blocked\"} 1"));
        assert!(text.contains("rustmapper_hosts{status=\"ready\"} 1"));
        assert!(text.contains("# TYPE rustmapper_urls_processed_total counter"));
        // Every sample line must be `name{labels} value` or `name value`.
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let (_, v) = line.rsplit_once(' ').unwrap();
            assert!(v.parse::<f64>().is_ok(), "bad sample: {line}");
        }
    }

    #[test]
    fn routes() {
        let m = Metrics::new();
        let ok = String::from_utf8(handle_path("/metrics", &m, &ctx())).unwrap();
        assert!(ok.starts_with("HTTP/1.1 200 OK"));
        assert!(ok.contains("text/plain; version=0.0.4"));
        let report = String::from_utf8(handle_path("/report?x=1", &m, &ctx())).unwrap();
        assert!(report.contains("Per-host rate-limit budget"));
        assert!(report.contains("http-equiv=\"refresh\""));
        let nf = String::from_utf8(handle_path("/nope", &m, &ctx())).unwrap();
        assert!(nf.starts_with("HTTP/1.1 404"));
    }

    #[tokio::test]
    async fn serves_over_tcp() {
        let m = Arc::new(Metrics::new());
        m.urls_fetched_total.lock().add(5);
        let addr = spawn("127.0.0.1:0", Arc::clone(&m), ctx()).await.unwrap();
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut body = String::new();
        s.read_to_string(&mut body).await.unwrap();
        assert!(body.contains("rustmapper_urls_fetched_total 5"), "{body}");
    }
}
