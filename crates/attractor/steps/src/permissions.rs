//! Agent permissions and the embedding host's immutable run ceiling.

pub use frontend_attractor::AgentPermissions;
use pebble_coding_agent::events::PermissionLevel;
use steps::StepCtx;

/// Register with `Runtime::capability` to cap all native agents and local
/// agent hooks. Workflows cannot raise this ceiling. Custom backends and hook
/// services supplied by the host are responsible for their own enforcement.
#[derive(Clone, Copy, Debug)]
pub struct AgentPermissionCeiling(pub AgentPermissions);

pub(crate) fn effective(requested: AgentPermissions, ctx: &StepCtx) -> AgentPermissions {
    ctx.capability::<AgentPermissionCeiling>()
        .map_or(requested, |ceiling| requested.min(ceiling.0))
}

pub(crate) fn pebble(level: AgentPermissions) -> PermissionLevel {
    match level {
        AgentPermissions::ReadOnly => PermissionLevel::ReadOnly,
        AgentPermissions::ReadWrite => PermissionLevel::ReadWrite,
        AgentPermissions::Full => PermissionLevel::Full,
    }
}
