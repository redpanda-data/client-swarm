// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

use log::error;
use rdkafka::client::ClientContext;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

pub const DEFAULT_AUTH_FAILURE_TIMEOUT: Duration = Duration::from_secs(60);

// librdkafka reports a repeated identical error about once every 30s and
// suppresses the rest, so a quiet gap shorter than this proves nothing.
const QUIET_GAP: Duration = Duration::from_secs(60);

/// How long SASL authentication has failed with no progress in between.
///
/// A streak starts at the first authentication error. Progress (a message
/// delivered or consumed) ends it, and so does a gap of `timeout` (at least
/// `QUIET_GAP`) with no authentication error, so that two short failures far
/// apart on an idle client do not add up. The exit check runs when librdkafka
/// reports an error, so it can fire up to ~30s after `timeout`.
struct AuthFailureStreak {
    timeout: Duration,
    // Lets progress() skip the lock on the hot path when nothing failed.
    failing: AtomicBool,
    // (first, latest) authentication error of the current streak.
    errors: Mutex<Option<(Instant, Instant)>>,
}

impl AuthFailureStreak {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            failing: AtomicBool::new(false),
            errors: Mutex::new(None),
        }
    }

    /// Records an authentication error at `now`; returns the streak length.
    fn failed(&self, now: Instant) -> Duration {
        let mut errors = self.errors.lock().unwrap();
        let first = match *errors {
            Some((first, latest)) if now - latest < self.timeout.max(QUIET_GAP) => first,
            _ => now,
        };
        *errors = Some((first, now));
        self.failing.store(true, Ordering::Relaxed);
        now - first
    }

    fn progressed(&self) {
        if self.failing.swap(false, Ordering::Relaxed) {
            *self.errors.lock().unwrap() = None;
        }
    }
}

static AUTH_FAILURES: OnceLock<AuthFailureStreak> = OnceLock::new();

fn auth_failures() -> &'static AuthFailureStreak {
    AUTH_FAILURES.get_or_init(|| AuthFailureStreak::new(DEFAULT_AUTH_FAILURE_TIMEOUT))
}

/// Sets how long authentication may fail before the process exits. Call it
/// before any client starts; later calls have no effect.
pub fn set_auth_failure_timeout(timeout: Duration) {
    let _ = AUTH_FAILURES.set(AuthFailureStreak::new(timeout));
}

/// Tells the authentication watch that a client got a message through.
pub fn auth_progressed() {
    auth_failures().progressed();
}

/// Client context that exits the process when SASL authentication keeps
/// failing.
///
/// librdkafka retries a failed handshake forever, so a swarm with bad
/// credentials would otherwise sit idle while it looks healthy. Failures
/// shorter than the timeout are tolerated: a broker restart, or a new user
/// whose SCRAM credentials have not reached every broker yet. Other errors
/// are only logged, as by the default context.
pub struct ExitOnAuthFailure;

impl ClientContext for ExitOnAuthFailure {
    fn error(&self, error: KafkaError, reason: &str) {
        error!("librdkafka: {}: {}", error, reason);
        if error.rdkafka_error_code() != Some(RDKafkaErrorCode::Authentication) {
            return;
        }
        let watch = auth_failures();
        let lasted = watch.failed(Instant::now());
        if lasted >= watch.timeout {
            error!(
                "SASL authentication has failed for {:?} with no progress: check \
                 the credentials, or raise --auth-failure-timeout",
                lasted
            );
            process::exit(-1);
        }
    }
}

impl ConsumerContext for ExitOnAuthFailure {}

pub fn split_properties(properties: Vec<String>) -> Vec<(String, String)> {
    properties
        .iter()
        .map(|p| {
            // split_once: a value may itself contain '=' (base64 passwords do).
            let (k, v) = p.split_once('=').expect("properties must be key=value");
            (k.to_string(), v.to_string())
        })
        .collect()
}

/// Reads librdkafka properties from `path`, one `key=value` per line.
///
/// Blank lines and lines starting with `#` are skipped. A file keeps
/// secrets such as `sasl.password` off the command line, where `ps` shows
/// them to every user on the host. Errors name the line, never its content.
pub fn read_properties_file(path: &str) -> Result<Vec<String>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read properties file {path}: {e}"))?;
    let mut properties = vec![];
    for (i, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if !line.contains('=') {
            return Err(format!("{path}:{}: expected key=value", i + 1));
        }
        properties.push(line.to_string());
    }
    Ok(properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_temp(name: &str, content: &str) -> String {
        let path = std::env::temp_dir().join(format!("client-swarm-{}-{name}", std::process::id()));
        std::fs::write(&path, content).unwrap();
        path.to_str().unwrap().to_string()
    }

    const T: Duration = Duration::from_secs(60);

    #[test]
    fn auth_streak_grows_while_errors_keep_coming() {
        let s = AuthFailureStreak::new(T);
        let t0 = Instant::now();
        assert_eq!(s.failed(t0), Duration::ZERO);
        assert_eq!(
            s.failed(t0 + Duration::from_secs(30)),
            Duration::from_secs(30)
        );
        assert_eq!(
            s.failed(t0 + Duration::from_secs(61)),
            Duration::from_secs(61)
        );
    }

    #[test]
    fn auth_streak_ends_on_progress() {
        let s = AuthFailureStreak::new(T);
        let t0 = Instant::now();
        s.failed(t0);
        s.progressed();
        let later = t0 + Duration::from_secs(59);
        assert_eq!(s.failed(later), Duration::ZERO);
    }

    #[test]
    fn auth_streak_ends_after_a_quiet_timeout() {
        let s = AuthFailureStreak::new(T);
        let t0 = Instant::now();
        s.failed(t0);
        assert_eq!(s.failed(t0 + T), Duration::ZERO);
    }

    #[test]
    fn short_timeout_streak_survives_librdkafka_error_suppression() {
        let s = AuthFailureStreak::new(Duration::from_secs(10));
        let t0 = Instant::now();
        s.failed(t0);
        let next = t0 + Duration::from_secs(33);
        assert_eq!(s.failed(next), Duration::from_secs(33));
    }

    #[test]
    fn zero_timeout_exits_on_the_first_error() {
        let s = AuthFailureStreak::new(Duration::ZERO);
        assert!(s.failed(Instant::now()) >= s.timeout);
    }

    #[test]
    fn split_keeps_equals_in_value() {
        let props = split_properties(vec!["sasl.password=YWJj==".to_string()]);
        assert_eq!(
            props,
            vec![("sasl.password".to_string(), "YWJj==".to_string())]
        );
    }

    #[test]
    fn file_skips_blank_and_comment_lines() {
        let path = write_temp(
            "ok",
            "# SASL\nsecurity.protocol=SASL_SSL\n\n  \nsasl.password=p=w d\r\n",
        );
        assert_eq!(
            read_properties_file(&path).unwrap(),
            vec!["security.protocol=SASL_SSL", "sasl.password=p=w d"]
        );
    }

    #[test]
    fn file_line_without_equals_names_the_line_only() {
        let path = write_temp("bad", "security.protocol=SASL_SSL\nhunter2\n");
        let err = read_properties_file(&path).unwrap_err();
        assert!(err.ends_with(":2: expected key=value"), "{err}");
        assert!(!err.contains("hunter2"), "{err}");
    }

    #[test]
    fn missing_file_is_an_error() {
        let err = read_properties_file("/nonexistent/client-swarm.properties").unwrap_err();
        assert!(err.starts_with("cannot read properties file"), "{err}");
    }
}
