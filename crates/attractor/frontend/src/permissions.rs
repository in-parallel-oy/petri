//! Agent tool permissions, using Pebble's three permission levels.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Agent tool access. Omission preserves Petri's historical full access.
/// These levels govern agents, not workflow scripts or runtime persistence.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum AgentPermissions {
    /// Native reads and subagents; no arbitrary shell commands.
    ReadOnly,
    /// Native reads, file edits, and subagents; no arbitrary shell commands.
    ReadWrite,
    /// All agent tools, including shell, MCP, and host tools.
    #[default]
    Full,
}

impl AgentPermissions {
    /// Every level, most restrictive first.
    pub const ALL: &'static [Self] = &[Self::ReadOnly, Self::ReadWrite, Self::Full];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::ReadWrite => "read-write",
            Self::Full => "full",
        }
    }
}

impl fmt::Display for AgentPermissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The text was not a permission level.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("`{0}` is not a permission level (read-only, read-write, or full)")]
pub struct UnknownPermissions(pub String);

impl FromStr for AgentPermissions {
    type Err = UnknownPermissions;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|level| level.as_str() == text)
            .ok_or_else(|| UnknownPermissions(text.to_owned()))
    }
}
