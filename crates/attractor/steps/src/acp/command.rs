//! How an ACP agent is launched: the command line or stdio server config a
//! node names, and the environment it gets.

use std::collections::BTreeMap;
use std::iter;

use executor::{ProcessSpec, SECRET_REF_KEY, SecretProvider, StdinMode};
use ir::Value;
use serde::Deserialize;
use serde::de::Error as _;
use smol_str::SmolStr;

/// A value in the agent command's environment: a literal, or a reference
/// to one of the run's secrets, resolved at launch and never written down.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvValue {
    Literal(String),
    Secret(String),
}

impl<'de> Deserialize<'de> for EnvValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if let Value::String(text) = &value {
            return Ok(Self::Literal(text.clone()));
        }
        if let Value::Object(map) = &value
            && map.len() == 1
            && let Some(Value::String(name)) = map.get(SECRET_REF_KEY)
        {
            return Ok(Self::Secret(name.clone()));
        }
        Err(D::Error::custom(format!(
            "an env value is a string or {{\"{SECRET_REF_KEY}\": \"NAME\"}}, not {value}"
        )))
    }
}

/// How an agent is launched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentCommand {
    pub program: String,
    pub args:    Vec<String>,
    pub env:     BTreeMap<String, EnvValue>,
}

impl AgentCommand {
    /// `acp.command`: one shell-quoted command line.
    pub fn from_command_line(line: &str) -> Result<Self, String> {
        let words = shlex::split(line.trim()).ok_or_else(|| "unbalanced quotes".to_string())?;
        let mut words = words.into_iter();
        let program = words
            .next()
            .ok_or_else(|| "the command line is empty".to_string())?;
        Ok(Self {
            program,
            args: words.collect(),
            env: BTreeMap::new(),
        })
    }

    /// `acp.config`: the JSON stdio server shape `{command, args, env}`.
    pub fn from_config(config: &Value) -> Result<Self, String> {
        let config: StdioConfig = serde_json::from_value(config.clone())
            .map_err(|error| format!("invalid `acp.config`: {error}"))?;
        if config.command.is_empty() {
            return Err("`acp.config` needs a non-empty `command`".into());
        }
        let env = match config.env {
            None => BTreeMap::new(),
            Some(ConfigEnv::Map(env)) => env,
            Some(ConfigEnv::Pairs(pairs)) => pairs
                .into_iter()
                .map(|pair| (pair.name, pair.value))
                .collect(),
        };
        Ok(Self {
            program: config.command,
            args: config.args,
            env,
        })
    }

    /// Workflow secret references fill the launch environment beneath the
    /// agent's explicit `acp.config.env` overrides. Values resolve only at
    /// spawn.
    #[must_use]
    pub fn with_workflow_env(mut self, env: &BTreeMap<String, EnvValue>) -> Self {
        let mut inherited = env.clone();
        inherited.append(&mut self.env);
        self.env = inherited;
        self
    }

    /// Whether the command launches codex-acp (by path, or as an npm
    /// package), the one agent offered native sub-agent sessions.
    pub fn is_codex_acp(&self) -> bool {
        iter::once(&self.program).chain(&self.args).any(|word| {
            word.rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with("codex-acp"))
        })
    }

    /// The process to start: the command with its environment resolved. Only
    /// the secrets its `env` names reach the agent. A product's API key is
    /// never added on its own, because some products bill an API key ahead
    /// of the subscription they are signed in to. A `$secret` reference the
    /// provider cannot supply is an error naming the secret.
    pub fn spec(&self, secrets: &dyn SecretProvider) -> Result<ProcessSpec, String> {
        let env = resolve_env(&self.env, secrets)?;
        let args: Vec<&str> = self.args.iter().map(String::as_str).collect();
        Ok(ProcessSpec::new(&self.program, &args)
            .with_env(env)
            .with_stdin(StdinMode::Piped))
    }
}

/// Resolve an environment's `$secret` references; a reference the provider
/// cannot supply is an error naming the secret and the variable.
pub fn resolve_env(
    env: &BTreeMap<String, EnvValue>,
    secrets: &dyn SecretProvider,
) -> Result<BTreeMap<SmolStr, SmolStr>, String> {
    env.iter()
        .map(|(key, value)| {
            let value = match value {
                EnvValue::Literal(text) => SmolStr::new(text),
                EnvValue::Secret(name) => secrets
                    .resolve(name)
                    .map_err(|error| format!("secret `{name}` for env `{key}`: {error}"))?
                    .expose(),
            };
            Ok((SmolStr::new(key), value))
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StdioConfig {
    command: String,
    #[serde(default)]
    args:    Vec<String>,
    #[serde(default)]
    env:     Option<ConfigEnv>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ConfigEnv {
    Map(BTreeMap<String, EnvValue>),
    Pairs(Vec<EnvPair>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvPair {
    name:  String,
    value: EnvValue,
}

#[cfg(test)]
mod tests {
    use executor::MapSecrets;
    use serde_json::json;

    use super::*;

    #[test]
    fn command_lines_split_like_a_shell() {
        let command =
            AgentCommand::from_command_line("python3 -c 'print(1)' --flag").expect("splits");
        assert_eq!(command.program, "python3");
        assert_eq!(command.args, ["-c", "print(1)", "--flag"]);
        assert!(AgentCommand::from_command_line("").is_err());
        assert!(AgentCommand::from_command_line("a 'b").is_err());
    }

    #[test]
    fn codex_acp_is_known_by_its_program_or_package() {
        for line in [
            "codex-acp",
            "/usr/local/bin/codex-acp",
            "npx -y @agentclientprotocol/codex-acp@1.13.0",
        ] {
            let command = AgentCommand::from_command_line(line).expect("splits");
            assert!(command.is_codex_acp(), "{line}");
        }
        for line in [
            "claude-agent-acp",
            "npx -y @agentclientprotocol/claude-agent-acp",
            "gemini --acp",
        ] {
            let command = AgentCommand::from_command_line(line).expect("splits");
            assert!(!command.is_codex_acp(), "{line}");
        }
    }

    #[test]
    fn configs_carry_args_and_env() {
        let command = AgentCommand::from_config(&json!({
            "command": "agent",
            "args": ["--acp"],
            "env": [{ "name": "K", "value": "v" }],
        }))
        .expect("parses");
        assert_eq!(command.args, ["--acp"]);
        assert_eq!(command.env.get("K"), Some(&EnvValue::Literal("v".into())));
        assert!(AgentCommand::from_config(&json!({ "args": [] })).is_err());
        assert!(AgentCommand::from_config(&json!({ "command": ["agent"], "args": [] })).is_err());
        assert!(
            AgentCommand::from_config(&json!({ "command": "agent", "args": "--acp" })).is_err()
        );
        assert!(
            AgentCommand::from_config(&json!({ "command": "agent", "unknown": true })).is_err()
        );
    }

    #[test]
    fn config_env_values_may_reference_secrets() {
        let command = AgentCommand::from_config(&json!({
            "command": "agent",
            "env": { "KEY": { "$secret": "AGENT_KEY" }, "MODE": "test" },
        }))
        .expect("parses");
        assert_eq!(
            command.env.get("KEY"),
            Some(&EnvValue::Secret("AGENT_KEY".into()))
        );
        assert!(
            AgentCommand::from_config(&json!({
                "command": "agent",
                "env": { "KEY": { "$secret": "A", "extra": 1 } },
            }))
            .is_err()
        );
        assert!(
            AgentCommand::from_config(&json!({ "command": "agent", "env": { "KEY": 1 } })).is_err()
        );
    }

    #[test]
    fn the_spec_resolves_named_secrets_and_adds_no_product_credential() {
        let secrets = MapSecrets::new(BTreeMap::from([
            ("AGENT_KEY".into(), "agent-secret-value".into()),
            ("GEMINI_API_KEY".into(), "gemini-secret-value".into()),
        ]));
        let command = AgentCommand::from_config(&json!({
            "command": "agent",
            "env": { "KEY": { "$secret": "AGENT_KEY" }, "MODE": "test" },
        }))
        .expect("parses");
        let spec = command.spec(&secrets).expect("resolves");
        assert_eq!(
            spec.env.get("KEY").map(SmolStr::as_str),
            Some("agent-secret-value")
        );
        assert_eq!(spec.env.get("MODE").map(SmolStr::as_str), Some("test"));
        assert!(
            !spec.env.contains_key("GEMINI_API_KEY"),
            "a product credential the provider knows is not added unnamed"
        );
        assert_eq!(spec.stdin, StdinMode::Piped);
        // Resolving registered both values for masking.
        assert_eq!(secrets.masker().mask("agent-secret-value"), "***");

        let missing = AgentCommand::from_config(&json!({
            "command": "agent",
            "env": { "KEY": { "$secret": "NOT_THERE" } },
        }))
        .expect("parses");
        let error = missing.spec(&secrets).expect_err("the secret is unknown");
        assert!(error.contains("NOT_THERE"), "{error}");
    }
}
