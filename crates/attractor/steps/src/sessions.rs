//! Retained native sessions: the run-scoped map from a resolved Fabro thread
//! to the conversation a later node may continue.
//!
//! A node at effective `full` fidelity with a resolved thread takes the
//! thread's retained session, continues it, and puts it back when it
//! succeeds. Any other node starts fresh. A failed node discards the
//! session, as Fabro does ("on error, discard the session"). Pebble owns the
//! conversation: what is retained is its warm export
//! (`CodingAgentExport`), taken after the node's last prompt and before the
//! predecessor agent shut down, so nothing live outlives the step that
//! opened it and the workspace can be released without a teardown here.
//! The successor rebinds its event sink, human input, tool middleware and
//! metrics when it resumes from the export. ACP never reuses threads.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, PoisonError};

use pebble_coding_agent::CodingAgentExport;
use runtime::driver::ResumeInfo;
use runtime::engine::Event;
use steps::StepCtx;

use crate::agent::THREAD_EVENT;
use crate::fallback::Plan;

/// A resume loses the in-memory exports, including threads used by stages
/// that already finished. Their thread events survive in the execution log.
pub(crate) fn lost_on_resume(ctx: &StepCtx, thread: &str) -> bool {
    ctx.capability::<ResumeInfo>().is_some_and(|resume| {
        resume.log.events().any(|event| {
            matches!(event, Event::StepProgressRecorded {
                ev: ir::StepEvent::Custom(value), ..
            } if value["kind"] == THREAD_EVENT
                && value["backend"] == "api"
                && value["thread"].as_str() == Some(thread)
                && (value["fidelity"] == "full" || value["fidelity_source"] == "resume"))
        })
    })
}

/// One retained conversation.
pub struct Retained {
    pub export:   CodingAgentExport,
    /// The node that last used it, for events.
    pub node:     String,
    /// The route the thread was requested on (the plan's original),
    /// `provider/model`, for the warning when a later node names another
    /// model.
    pub selector: String,
    /// How many nodes have used this thread.
    pub uses:     u32,
    /// The thread's fallback plan, at the position reached: a later node
    /// continues on the route the thread runs on and never activates that
    /// route's own chain.
    pub plan:     Plan,
}

/// The run's retained sessions by thread id. Registered as a capability by
/// [`crate::register`] through a per-run provisioner.
#[derive(Default)]
pub struct SessionService {
    retained: Mutex<HashMap<String, Retained>>,
    /// Threads whose conversation existed and was discarded: a later node on
    /// the thread degrades `full` to `summary:high`.
    lost:     Mutex<HashSet<String>>,
}

impl SessionService {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take the thread's retained session. The caller owns it until it
    /// puts it back with [`SessionService::retain`]; a caller that fails
    /// simply does not, which disposes of it.
    pub fn take(&self, thread: &str) -> Option<Retained> {
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(thread)
    }

    /// Keep the session for the next node on this thread.
    pub fn retain(&self, thread: &str, retained: Retained) {
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(thread.to_owned(), retained);
        self.lost
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(thread);
    }

    /// The thread's conversation was discarded (the node failed or was
    /// cancelled).
    pub fn mark_lost(&self, thread: &str) {
        self.lost
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(thread.to_owned());
    }

    /// Whether the thread had a conversation that is gone.
    pub fn lost(&self, thread: &str) -> bool {
        self.lost
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(thread)
    }

    /// How many nodes have used the thread so far.
    pub fn uses(&self, thread: &str) -> u32 {
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(thread)
            .map_or(0, |r| r.uses)
    }

    /// The threads with a retained session, for inspection.
    pub fn threads(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect();
        out.sort();
        out
    }
}
