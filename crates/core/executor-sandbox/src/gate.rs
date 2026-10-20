//! A run's admission to its sandbox capabilities.
//!
//! An environment and a one-shot runner hold their sandbox directly, so a
//! step or a host that keeps one after the run finishes could still reach
//! the sandbox. A plugin's transport closing used to be what stopped that; a
//! provider linked into this process has no transport to close. The router's
//! gate is the explicit boundary instead: every operation is admitted
//! through it, and [`RunGate::close`] stops new admissions, then waits a
//! bounded time for admitted work to settle.
//!
//! The gate stops access, not sandboxes. Lease release still ends the
//! sandboxes by retention before the run closes its gate.

use std::time::Duration;

use executor::EnvError;
use tokio::time::timeout;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

use crate::BACKEND;

/// How long closing waits for admitted operations before giving up on them.
pub(crate) const DRAIN_BUDGET: Duration = Duration::from_secs(10);

/// Admission to one run's sandboxes. Clones share the gate.
#[derive(Clone, Default)]
pub(crate) struct RunGate {
    admitted: TaskTracker,
}

/// One admitted operation. The gate cannot finish closing while it lives.
pub(crate) struct Admission {
    _token: TaskTrackerToken,
}

impl RunGate {
    /// Admits one operation, or refuses once the run has closed its gate.
    pub(crate) fn admit(&self, operation: &str) -> Result<Admission, EnvError> {
        let token = self.admitted.token();
        // Checked after taking the token, so a close that began first
        // refuses this call and one that begins later waits for it.
        if self.admitted.is_closed() {
            return Err(EnvError::backend(
                BACKEND,
                operation,
                "the run has finished; its sandbox is no longer reachable",
            ));
        }
        Ok(Admission { _token: token })
    }

    /// Admits work the gate refuses once closed but that closing does not
    /// wait for: a run's held service execs. They have no reason to settle
    /// on their own; lease release stops them, and a run that closes
    /// without releasing its leases (interrupted, to be resumed) leaves them
    /// to its retained sandbox rather than spending the drain budget on them.
    pub(crate) fn admit_unawaited(&self, operation: &str) -> Result<Admission, EnvError> {
        if self.admitted.is_closed() {
            return Err(EnvError::backend(
                BACKEND,
                operation,
                "the run has finished; its sandbox is no longer reachable",
            ));
        }
        Ok(Admission {
            _token: TaskTracker::new().token(),
        })
    }

    /// Stops new admissions and waits up to `budget` for admitted
    /// operations to settle. Returns `false` when some were still running
    /// at the deadline: they finish on their own, and no new one starts. A
    /// second close returns `true` at once.
    pub(crate) async fn close(&self, budget: Duration) -> bool {
        if !self.admitted.close() {
            return true;
        }
        timeout(budget, self.admitted.wait()).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn a_closed_gate_refuses_new_work() {
        let gate = RunGate::default();
        drop(gate.admit("read").expect("open gate admits"));
        assert!(gate.close(DRAIN_BUDGET).await);
        let error = gate.admit("read").err().expect("closed gate refuses");
        assert!(error.to_string().contains("finished"), "{error}");
        assert!(gate.close(DRAIN_BUDGET).await, "closing twice is safe");
    }

    #[tokio::test]
    async fn closing_waits_for_admitted_work() {
        let gate = RunGate::default();
        let admitted = gate.admit("exec").expect("admitted");
        let closing = tokio::spawn({
            let gate = gate.clone();
            async move { gate.close(DRAIN_BUDGET).await }
        });
        sleep(Duration::from_millis(20)).await;
        assert!(
            !closing.is_finished(),
            "an admitted operation holds the drain"
        );
        assert!(
            gate.admit("exec").is_err(),
            "no new admissions while draining"
        );
        drop(admitted);
        assert!(closing.await.expect("close task"));
    }

    #[tokio::test]
    async fn closing_does_not_wait_for_unawaited_work_but_refuses_more() {
        let gate = RunGate::default();
        let _held = gate.admit_unawaited("service").expect("admitted");
        assert!(gate.close(Duration::from_millis(10)).await);
        assert!(gate.admit_unawaited("service").is_err());
    }

    #[tokio::test]
    async fn closing_gives_up_on_work_past_its_budget() {
        let gate = RunGate::default();
        let _stuck = gate.admit("exec").expect("admitted");
        assert!(!gate.close(Duration::from_millis(10)).await);
        assert!(gate.admit("exec").is_err());
    }
}
