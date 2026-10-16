//! The failure circuit breaker: `loop_restart_signature_limit` as routing
//! middleware.
//!
//! A run that keeps failing the same way is not going to succeed by looping.
//! After every node's final outcome the breaker classifies a failure into a
//! category and a signature (`<node>|<category>|<normalized reason>`), and
//! counts the signature when the category is tracked (`deterministic` or
//! `structural`). When one signature reaches the limit, the failed firing's
//! route is blocked with the reason, which fails the run.
//!
//! A `loop_restart` edge gets two more rules, as the reference applies them:
//! only a `transient_infra` failure may take one, and a tracked failure's
//! restart signature has its own map with the same limit. Success never
//! clears a count. Both maps live in the middleware state, so a restart
//! successor inherits them and a resumed run restores them from its
//! checkpoint.
//!
//! The category rules are the reference's heuristics over the failure class
//! and message ([`classify`]); a host with its own taxonomy supplies a
//! [`FailureClassifier`].

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::{Arc, LazyLock};

use engine::{MiddlewareKey, RouteDecision};
use ir::{EdgeTransition, Outcome, Status};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use smol_str::SmolStr;

use crate::{FoldEvent, Middleware, MiddlewareError, RouteCall, RouteNext};

/// The chain key the breaker records itself under.
pub const KEY: &str = "circuit-breaker";

/// The reference's failure categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCategory {
    TransientInfra,
    Deterministic,
    BudgetExhausted,
    CompilationLoop,
    Canceled,
    Structural,
}

impl FailureCategory {
    /// Whether repeats of this category count toward the limit.
    pub fn is_signature_tracked(self) -> bool {
        matches!(self, Self::Deterministic | Self::Structural)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::TransientInfra => "transient_infra",
            Self::Deterministic => "deterministic",
            Self::BudgetExhausted => "budget_exhausted",
            Self::CompilationLoop => "compilation_loop",
            Self::Canceled => "canceled",
            Self::Structural => "structural",
        }
    }
}

impl fmt::Display for FailureCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One classified failure, as the breaker records it between a node's final
/// outcome and its route.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassifiedFailure {
    pub category: FailureCategory,
    pub reason:   String,
}

/// How a host maps an outcome onto the breaker's categories.
pub trait FailureClassifier: Send + Sync {
    /// The failure a final outcome carries, or `None` for a success-like
    /// outcome (success, partial success, skipped).
    fn classify(&self, outcome: &Outcome) -> Option<ClassifiedFailure>;
}

/// The reference's heuristics: the failure class first, then hints in the
/// message. Unknown failures are `deterministic`.
#[derive(Debug, Default)]
pub struct ReferenceClassifier;

const TRANSIENT_INFRA_HINTS: &[&str] = &[
    "timeout",
    "timed out",
    "rate limit",
    "rate limited",
    "connection refused",
    "connection reset",
    "500",
    "502",
    "503",
    "504",
    "context deadline exceeded",
    "could not resolve host",
    "could not resolve hostname",
    "temporary failure",
    "network is unreachable",
    "broken pipe",
    "tls handshake timeout",
    "i/o timeout",
    "no route to host",
    "temporarily unavailable",
    "try again",
    "too many requests",
    "service unavailable",
    "gateway timeout",
    "econnrefused",
    "econnreset",
    "dial tcp",
    "transport is closing",
    "stream disconnected",
    "stream closed before",
    "index.crates.io",
    "download of config.json failed",
    "toolchain_or_dependency_registry_unavailable",
    "toolchain dependency resolution blocked by network",
    "toolchain_workspace_io",
    "cross-device link",
    "invalid cross-device link",
    "os error 18",
    "state change in progress",
    "sandbox stop still in progress",
];

const BUDGET_EXHAUSTED_HINTS: &[&str] = &[
    "turn limit",
    "token limit",
    "context length",
    "budget",
    "quota exceeded",
    "max_tokens",
    "max tokens",
    "context window exceeded",
    "budget exhausted",
    "token limit exceeded",
];

const STRUCTURAL_HINTS: &[&str] = &[
    "write_scope_violation",
    "write scope violation",
    "scope violation",
];

static HEX_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b[0-9a-f]{7,64}\b").expect("a fixed pattern compiles"));

/// The reference's category for a failure reason.
pub fn classify(reason: &str) -> FailureCategory {
    // Commit hashes are hex, and hex holds "500" or "503" often enough to
    // read as a transient hint; mask them first.
    let lowered = reason.to_lowercase();
    let lower = HEX_RE.replace_all(&lowered, "<hex>");
    if lower.contains("interrupt")
        || (lower.contains("cancel")
            && !lower.contains("cancelling due to test failure")
            && !lower.contains("canceling due to test failure"))
    {
        return FailureCategory::Canceled;
    }
    if TRANSIENT_INFRA_HINTS
        .iter()
        .any(|hint| lower.contains(hint))
    {
        return FailureCategory::TransientInfra;
    }
    if BUDGET_EXHAUSTED_HINTS
        .iter()
        .any(|hint| lower.contains(hint))
    {
        return FailureCategory::BudgetExhausted;
    }
    if STRUCTURAL_HINTS.iter().any(|hint| lower.contains(hint)) {
        return FailureCategory::Structural;
    }
    FailureCategory::Deterministic
}

/// The reference's signature normalization: variable data (hex, digits,
/// spacing) replaced so the same failure reads the same on every repeat.
pub fn normalize_reason(reason: &str) -> String {
    static DIGITS_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\b\d+\b").expect("a fixed pattern compiles"));
    static COMMA_SPACE_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r",\s+").expect("a fixed pattern compiles"));
    static WHITESPACE_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\s+").expect("a fixed pattern compiles"));
    let s = reason.trim().to_lowercase();
    if s.is_empty() {
        return String::new();
    }
    let s = HEX_RE.replace_all(&s, "<hex>");
    let s = DIGITS_RE.replace_all(&s, "<n>");
    let s = COMMA_SPACE_RE.replace_all(&s, ",");
    let s = WHITESPACE_RE.replace_all(&s, " ");
    let s = s.trim();
    if s.len() <= 240 {
        return s.to_string();
    }
    // The reference cuts at 240 bytes on a char boundary.
    let mut cut = 240;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s[..cut].to_string()
}

/// `<node>|<category>|<normalized reason>`, `unknown` for an empty reason.
pub fn signature(node: &str, category: FailureCategory, reason: &str) -> String {
    let reason = Some(normalize_reason(reason))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());
    format!("{}|{category}|{reason}", node.trim())
}

impl FailureClassifier for ReferenceClassifier {
    fn classify(&self, outcome: &Outcome) -> Option<ClassifiedFailure> {
        let (reason, class) = match &outcome.status {
            Status::Success | Status::PartialSuccess { .. } | Status::Skipped => return None,
            Status::Cancelled => {
                return Some(ClassifiedFailure {
                    category: FailureCategory::Canceled,
                    reason:   "cancelled".to_owned(),
                });
            }
            Status::TimedOut => {
                return Some(ClassifiedFailure {
                    category: FailureCategory::TransientInfra,
                    reason:   "timed out".to_owned(),
                });
            }
            Status::Failure(info) => (info.message.as_str(), info.class.as_str()),
        };
        let category = if class.contains("timeout") || class.contains("timed_out") {
            FailureCategory::TransientInfra
        } else if class.contains("interrupt") || class.contains("cancel") {
            FailureCategory::Canceled
        } else if class.starts_with("exit_status:") {
            // A command's output is diagnostic evidence, not a category hint.
            // Keep it in the reason for users and signature tracking, but do
            // not let line numbers or identifiers override its exit status.
            FailureCategory::Deterministic
        } else {
            classify(reason)
        };
        Some(ClassifiedFailure {
            category,
            reason: reason.to_owned(),
        })
    }
}

/// The breaker's checkpointed state.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct State {
    /// Repeats per signature after nodes.
    #[serde(default)]
    loop_signatures:    BTreeMap<String, u32>,
    /// Repeats per signature on taken restart edges.
    #[serde(default)]
    restart_signatures: BTreeMap<String, u32>,
    /// Final failures whose route has not been applied yet, by firing.
    #[serde(default)]
    pending:            BTreeMap<String, PendingFailure>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PendingFailure {
    node:      String,
    failure:   ClassifiedFailure,
    signature: String,
    /// The block reason, once the loop signature reached the limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tripped:   Option<String>,
}

/// The middleware. Install it in the coordinator's chain when the graph's
/// policy names a limit; a resumed run must install the same chain.
pub struct CircuitBreaker {
    limit:      NonZeroU32,
    classifier: Arc<dyn FailureClassifier>,
}

impl CircuitBreaker {
    pub fn new(limit: NonZeroU32, classifier: Arc<dyn FailureClassifier>) -> Self {
        Self { limit, classifier }
    }

    /// The breaker with the reference's heuristics.
    pub fn reference(limit: NonZeroU32) -> Self {
        Self::new(limit, Arc::new(ReferenceClassifier))
    }

    fn read(state: &Value) -> Result<State, MiddlewareError> {
        serde_json::from_value(state.clone())
            .map_err(|error| MiddlewareError::new(format!("circuit breaker state: {error}")))
    }

    fn write(state: &mut Value, next: &State) -> Result<(), MiddlewareError> {
        *state = serde_json::to_value(next)
            .map_err(|error| MiddlewareError::new(format!("circuit breaker state: {error}")))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Middleware for CircuitBreaker {
    fn key(&self) -> MiddlewareKey {
        MiddlewareKey::new(KEY)
    }

    fn state_version(&self) -> u32 {
        1
    }

    fn initial_state(&self) -> Value {
        json!(State::default())
    }

    fn fold(&self, state: &mut Value, event: &FoldEvent<'_>) -> Result<(), MiddlewareError> {
        match event {
            FoldEvent::FinalOutcome {
                firing,
                node,
                outcome,
            } => {
                let Some(failure) = self.classifier.classify(outcome) else {
                    return Ok(());
                };
                let mut current = Self::read(state)?;
                let name = format!("node-{}", node.raw());
                let signature = signature(&name, failure.category, &failure.reason);
                let mut tripped = None;
                if failure.category.is_signature_tracked() {
                    let count = current
                        .loop_signatures
                        .entry(signature.clone())
                        .or_insert(0);
                    *count += 1;
                    if *count >= self.limit.get() {
                        tripped = Some(format!(
                            "deterministic failure cycle detected: signature {signature} repeated \
                             {count} times (limit {})",
                            self.limit
                        ));
                    }
                }
                current
                    .pending
                    .insert(firing.raw().to_string(), PendingFailure {
                        node: name,
                        failure,
                        signature,
                        tripped,
                    });
                Self::write(state, &current)
            }
            FoldEvent::RouteApplied {
                firing, restart, ..
            } => {
                let mut current = Self::read(state)?;
                let pending = current.pending.remove(&firing.raw().to_string());
                if *restart
                    && let Some(pending) = pending
                    && pending.failure.category.is_signature_tracked()
                {
                    *current
                        .restart_signatures
                        .entry(pending.signature)
                        .or_insert(0) += 1;
                }
                Self::write(state, &current)
            }
            FoldEvent::ExecutionStarted => Ok(()),
        }
    }

    async fn route(
        &self,
        call: RouteCall,
        next: RouteNext<'_>,
    ) -> Result<RouteDecision, MiddlewareError> {
        let decision = next.run().await?;
        let current = Self::read(&call.state)?;
        let Some(pending) = current.pending.get(&call.firing.raw().to_string()) else {
            return Ok(decision);
        };
        if let Some(reason) = &pending.tripped {
            return Ok(RouteDecision::Block {
                reason: SmolStr::new(reason),
            });
        }
        let RouteDecision::Emit(edge) = decision else {
            return Ok(decision);
        };
        let restart = call.proposal.candidates.iter().any(|candidate| {
            candidate.edge == edge && candidate.transition == EdgeTransition::Restart
        });
        if !restart {
            return Ok(decision);
        }
        let category = pending.failure.category;
        if category != FailureCategory::TransientInfra {
            return Ok(RouteDecision::Block {
                reason: SmolStr::new(format!(
                    "loop_restart blocked: failure_class={category} (requires transient_infra), \
                     failure_reason={}",
                    pending.failure.reason
                )),
            });
        }
        if category.is_signature_tracked() {
            let count = current
                .restart_signatures
                .get(&pending.signature)
                .copied()
                .unwrap_or(0)
                + 1;
            if count >= self.limit.get() {
                return Ok(RouteDecision::Block {
                    reason: SmolStr::new(format!(
                        "loop_restart circuit breaker: signature {} repeated {count} times \
                         (limit {})",
                        pending.signature, self.limit
                    )),
                });
            }
        }
        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use ir::{FailureClass, FailureInfo};

    use super::*;

    #[test]
    fn reasons_classify_as_the_reference_does() {
        assert_eq!(classify("exit status 1"), FailureCategory::Deterministic);
        assert_eq!(
            classify("curl: (7) connection refused"),
            FailureCategory::TransientInfra
        );
        assert_eq!(
            classify("token limit exceeded"),
            FailureCategory::BudgetExhausted
        );
        assert_eq!(
            classify("write scope violation"),
            FailureCategory::Structural
        );
        assert_eq!(classify("run was interrupted"), FailureCategory::Canceled);
        // A commit hash holding "503" is not a transient hint.
        assert_eq!(
            classify("commit 0a503fb9c is bad"),
            FailureCategory::Deterministic
        );
    }

    #[test]
    fn command_diagnostics_do_not_override_exit_status() {
        for diagnostic in [
            "src/domain/order.py:42:5: F401 unused import",
            "src/domain/order.py:500:5: F401 unused import",
            "src/domain/order.py:502:5: F401 unused import",
            "src/domain/order.py:503:5: F401 unused import",
            "src/domain/order.py:504:5: F401 unused import",
            "error: this argument is passed by value\n --> crates/domain/src/lib.rs:500:13",
            "FAILED tests/test_retry.py::test_timeout",
            "error: undefined name `budget`",
            "error: undefined name `cancel`",
            "assertion failed: expected HTTP 503 Service Unavailable",
        ] {
            for code in [1, 101] {
                let reason = format!("script exited with status {code}\n{diagnostic}");
                let outcome = Outcome::new(
                    Status::Failure(
                        FailureInfo::new(&reason).with_class(FailureClass::exit_status(code)),
                    ),
                    Value::Null,
                );
                let failure = ReferenceClassifier.classify(&outcome).expect("a failure");
                assert_eq!(failure.category, FailureCategory::Deterministic, "{reason}");
                assert!(failure.category.is_signature_tracked());
                assert_eq!(failure.reason, reason, "diagnostics remain available");
            }
        }
    }

    #[test]
    fn structured_and_infrastructure_failures_keep_their_categories() {
        for (status, category) in [
            (Status::TimedOut, FailureCategory::TransientInfra),
            (Status::Cancelled, FailureCategory::Canceled),
            (
                Status::Failure(
                    FailureInfo::new("budget").with_class(FailureClass::new("timeout")),
                ),
                FailureCategory::TransientInfra,
            ),
            (
                Status::Failure(
                    FailureInfo::new("HTTP 503").with_class(FailureClass::new("cancelled")),
                ),
                FailureCategory::Canceled,
            ),
            (
                Status::failure("HTTP 502 Bad Gateway"),
                FailureCategory::TransientInfra,
            ),
            (
                Status::Failure(
                    FailureInfo::new("connection refused")
                        .with_class(FailureClass::new("spawn_failed")),
                ),
                FailureCategory::TransientInfra,
            ),
            (
                Status::failure("token budget exhausted"),
                FailureCategory::BudgetExhausted,
            ),
            (
                Status::failure("commit 0a503fb9c is bad"),
                FailureCategory::Deterministic,
            ),
        ] {
            let outcome = Outcome::new(status, Value::Null);
            assert_eq!(
                ReferenceClassifier.classify(&outcome).map(|f| f.category),
                Some(category)
            );
        }
    }

    #[test]
    fn signatures_normalize_variable_data() {
        assert_eq!(
            signature(
                "b",
                FailureCategory::Deterministic,
                "line 12 of abc1234def failed,  again"
            ),
            "b|deterministic|line <n> of <hex> failed,again"
        );
        assert_eq!(
            signature("b", FailureCategory::Structural, "   "),
            "b|structural|unknown"
        );
    }

    #[test]
    fn the_class_outranks_the_message() {
        let timeout = Outcome::new(
            Status::Failure(
                FailureInfo::new("Script timed out").with_class(FailureClass::new("timeout")),
            ),
            Value::Null,
        );
        assert_eq!(
            ReferenceClassifier.classify(&timeout).map(|f| f.category),
            Some(FailureCategory::TransientInfra)
        );
        let plain = Outcome::new(
            Status::Failure(
                FailureInfo::new("boom").with_class(FailureClass::new("exit_status:3")),
            ),
            Value::Null,
        );
        assert_eq!(
            ReferenceClassifier.classify(&plain).map(|f| f.category),
            Some(FailureCategory::Deterministic)
        );
        assert_eq!(
            ReferenceClassifier.classify(&Outcome::new(Status::Success, Value::Null)),
            None
        );
    }
}
