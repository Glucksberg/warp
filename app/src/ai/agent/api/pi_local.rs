use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{anyhow, Context as _};
use async_process::Command;
use futures::FutureExt as _;
use futures_lite::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use futures_lite::StreamExt as _;
use serde_json::Value;
use uuid::Uuid;
use warp_multi_agent_api as api;

use crate::ai::agent::{AIAgentContext, AIAgentInput, UserQueryMode};
use crate::server::server_api::AIApiError;

use super::{RequestParams, ResponseStream, ServerConversationToken};

const RUNTIME_ENV: &str = "WARP_AGENT_RUNTIME";
const LEGACY_ENABLE_ENV: &str = "WARP_USE_PI_AGENT";
const PI_COMMAND_ENV: &str = "WARP_PI_COMMAND";
const PI_PROVIDER_ENV: &str = "WARP_PI_PROVIDER";
const PI_MODEL_ENV: &str = "WARP_PI_MODEL";
const PI_THINKING_ENV: &str = "WARP_PI_THINKING";
const PI_TOOLS_ENV: &str = "WARP_PI_TOOLS";
const PI_DISABLE_TOOL_GATE_ENV: &str = "WARP_PI_DISABLE_TOOL_GATE";
const PI_SESSION_DIR_ENV: &str = "WARP_PI_SESSION_DIR";
const LOCAL_CONVERSATION_PREFIX: &str = "pi-local-";
const DEFAULT_PI_PROVIDER: &str = "openai-codex";
const DEFAULT_PI_MODEL: &str = "gpt-5.5";
const DEFAULT_PI_THINKING: &str = "high";
const READONLY_PI_TOOLS: &str = "read,grep,find,ls";
const ALL_PI_TOOLS: &str = "read,grep,find,ls,bash,edit,write";
const PI_TOOL_GATE_EXTENSION_SOURCE: &str =
    include_str!("../../../../resources/pi-runtime/warp-tool-gate.ts");

pub fn is_enabled() -> bool {
    std::env::var(RUNTIME_ENV)
        .map(|value| value.eq_ignore_ascii_case("pi_local"))
        .unwrap_or_default()
        || std::env::var(LEGACY_ENABLE_ENV)
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or_default()
}

pub async fn generate_multi_agent_output(
    params: RequestParams,
    cancellation_rx: futures::channel::oneshot::Receiver<()>,
) -> ResponseStream {
    Box::pin(async_stream::stream! {
        match run_pi_agent(params, cancellation_rx).await {
            Ok(events) => {
                for event in events {
                    yield Ok(event);
                }
            }
            Err(err) => {
                yield Err(Arc::new(AIApiError::Other(err)));
            }
        }
    })
}

async fn run_pi_agent(
    params: RequestParams,
    cancellation_rx: futures::channel::oneshot::Receiver<()>,
) -> anyhow::Result<Vec<api::ResponseEvent>> {
    let mut events = Vec::new();
    let request_id = Uuid::new_v4().to_string();
    let conversation_id = params
        .conversation_token
        .as_ref()
        .map(ServerConversationToken::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("pi-local-{}", Uuid::new_v4()));
    let run_id = format!("pi-run-{}", Uuid::new_v4());
    let task_id = params
        .tasks
        .first()
        .map(|task| task.id.clone())
        .unwrap_or_else(|| format!("pi-root-task-{conversation_id}"));
    let should_create_root_task = params.tasks.is_empty();

    validate_one_shot_request(&params)?;

    let prompt = prompt_from_params(&params)
        .filter(|prompt| !prompt.trim().is_empty())
        .ok_or_else(|| anyhow!("Pi local agent requires a user prompt"))?;

    events.push(init_event(
        request_id.clone(),
        conversation_id.clone(),
        run_id,
    ));

    let mut child = pi_command(&params, &conversation_id)?
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| {
            format!(
                "Failed to start Pi runtime. Install @mariozechner/pi-coding-agent or set {PI_COMMAND_ENV}"
            )
        })?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("Pi runtime stdin was unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("Pi runtime stdout was unavailable"))?;

    let prompt_command = serde_json::json!({
        "id": request_id.clone(),
        "type": "prompt",
        "message": prompt,
    });
    stdin
        .write_all(prompt_command.to_string().as_bytes())
        .await
        .context("Failed to write prompt to Pi runtime")?;
    stdin
        .write_all(b"\n")
        .await
        .context("Failed to finish Pi prompt frame")?;
    stdin.flush().await.context("Failed to flush Pi stdin")?;

    let mut lines = BufReader::new(stdout).lines();
    let mut pi_output = PiOutput::default();
    let mut cancellation_rx = cancellation_rx.fuse();

    loop {
        futures::select! {
            _ = cancellation_rx => {
                let _ = stdin.write_all(b"{\"type\":\"abort\"}\n").await;
                let _ = stdin.flush().await;
                let _ = child.kill();
                break;
            }
            line = lines.next().fuse() => {
                let Some(line) = line else {
                    break;
                };
                let line = line.context("Failed reading Pi runtime output")?;
                if line.trim().is_empty() {
                    continue;
                }
                let event: Value = serde_json::from_str(&line)
                    .with_context(|| format!("Pi runtime emitted invalid JSON: {line}"))?;
                match handle_pi_event(&event, &mut pi_output)? {
                    PiEventAction::Continue => {}
                    PiEventAction::Finish => break,
                    PiEventAction::Error(message) => return Err(pi_runtime_error(message)),
                }
            }
        }
    }

    if pi_output.assistant_text.trim().is_empty() {
        let _ = child.kill();
        return Err(anyhow!(
            "Pi runtime completed without assistant output. Check Pi authentication, selected model, and local Pi logs."
        ));
    }

    if should_create_root_task {
        events.push(create_root_task_event(task_id.clone()));
    }
    if let Some(tool_summary) = pi_output.tool_summary() {
        events.push(add_agent_output_event(
            task_id.clone(),
            request_id.clone(),
            tool_summary,
        ));
    }
    events.push(add_agent_output_event(
        task_id,
        request_id.clone(),
        pi_output.assistant_text,
    ));

    events.push(finished_event());
    let _ = child.kill();
    Ok(events)
}

fn pi_command(params: &RequestParams, conversation_id: &str) -> anyhow::Result<Command> {
    let executable = std::env::var(PI_COMMAND_ENV).unwrap_or_else(|_| {
        if cfg!(windows) {
            "pi.cmd".to_string()
        } else {
            "pi".to_string()
        }
    });

    let mut command = Command::new(executable);
    command
        .arg("--mode")
        .arg("rpc")
        .arg("--session")
        .arg(pi_session_path(conversation_id)?);

    command
        .arg("--provider")
        .arg(env_or_default(PI_PROVIDER_ENV, DEFAULT_PI_PROVIDER));

    command
        .arg("--model")
        .arg(env_or_default(PI_MODEL_ENV, DEFAULT_PI_MODEL));

    command
        .arg("--thinking")
        .arg(env_or_default(PI_THINKING_ENV, DEFAULT_PI_THINKING));

    let tools = configure_tools(&mut command);
    if tools.is_enabled() && !env_flag_is_enabled(PI_DISABLE_TOOL_GATE_ENV) {
        command
            .arg("--extension")
            .arg(pi_tool_gate_extension_path()?);
    }

    if let Some(cwd) = params.session_context.current_working_directory() {
        command.current_dir(cwd);
    }

    if let Some(api_keys) = &params.api_keys {
        set_secret_env(&mut command, "OPENAI_API_KEY", &api_keys.openai);
        set_secret_env(&mut command, "ANTHROPIC_API_KEY", &api_keys.anthropic);
        set_secret_env(&mut command, "GOOGLE_API_KEY", &api_keys.google);
        set_secret_env(&mut command, "OPENROUTER_API_KEY", &api_keys.open_router);
    }

    Ok(command)
}

fn pi_session_path(conversation_id: &str) -> anyhow::Result<PathBuf> {
    let session_dir = std::env::var_os(PI_SESSION_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| warp_core::paths::data_dir().join("pi-agent-sessions"));
    std::fs::create_dir_all(&session_dir).with_context(|| {
        format!(
            "Failed to create Pi session directory at {}",
            session_dir.display()
        )
    })?;

    let safe_id = sanitize_session_file_stem(conversation_id);
    Ok(session_dir.join(format!("{safe_id}.jsonl")))
}

fn sanitize_session_file_stem(value: &str) -> String {
    let mut sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();

    if sanitized.is_empty() {
        sanitized = format!("pi-local-{}", Uuid::new_v4());
    }

    sanitized
}

fn configure_tools(command: &mut Command) -> PiToolsConfig {
    match std::env::var(PI_TOOLS_ENV)
        .ok()
        .map(|value| value.trim().to_string())
    {
        None => {
            command.arg("--tools").arg(READONLY_PI_TOOLS);
            PiToolsConfig::Enabled
        }
        Some(value) if value.is_empty() || value.eq_ignore_ascii_case("none") => {
            command.arg("--no-tools");
            PiToolsConfig::Disabled
        }
        Some(value) => {
            let normalized = if value.eq_ignore_ascii_case("readonly") {
                READONLY_PI_TOOLS
            } else if value.eq_ignore_ascii_case("all") {
                ALL_PI_TOOLS
            } else {
                value.as_str()
            };
            command.arg("--tools").arg(normalized);
            PiToolsConfig::Enabled
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PiToolsConfig {
    Disabled,
    Enabled,
}

impl PiToolsConfig {
    fn is_enabled(self) -> bool {
        self == Self::Enabled
    }
}

fn pi_tool_gate_extension_path() -> anyhow::Result<PathBuf> {
    let extension_dir = warp_core::paths::data_dir().join("pi-runtime");
    std::fs::create_dir_all(&extension_dir).with_context(|| {
        format!(
            "Failed to create Pi runtime directory at {}",
            extension_dir.display()
        )
    })?;

    let extension_path = extension_dir.join("warp-tool-gate.ts");
    std::fs::write(&extension_path, PI_TOOL_GATE_EXTENSION_SOURCE).with_context(|| {
        format!(
            "Failed to write Pi tool gate extension at {}",
            extension_path.display()
        )
    })?;
    Ok(extension_path)
}

fn env_or_default(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_flag_is_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or_default()
}

fn set_secret_env(command: &mut Command, name: &str, value: &str) {
    if !value.is_empty() && std::env::var_os(name).is_none() {
        command.env(name, value);
    }
}

fn validate_one_shot_request(params: &RequestParams) -> anyhow::Result<()> {
    if params
        .conversation_token
        .as_ref()
        .is_some_and(|token| !is_pi_local_conversation_token(token))
        || params.forked_from_conversation_token.is_some()
        || params.input.len() != 1
    {
        return Err(anyhow!(
            "Pi local runtime currently supports only direct prompt submissions in pi-local \
conversations. Forked server conversations and action-result continuations require a richer \
session bridge before they can be routed safely."
        ));
    }

    validate_plain_prompt_input(params.input.first().expect("checked input length above"))
}

fn is_pi_local_conversation_token(token: &ServerConversationToken) -> bool {
    token.as_str().starts_with(LOCAL_CONVERSATION_PREFIX)
}

fn prompt_from_params(params: &RequestParams) -> Option<String> {
    params.input.iter().rev().find_map(prompt_from_input)
}

fn prompt_from_input(input: &AIAgentInput) -> Option<String> {
    let AIAgentInput::UserQuery { query, context, .. } = input else {
        return None;
    };

    let mut context_lines = Vec::new();
    for context in context.iter() {
        add_context_lines(context, &mut context_lines);
    }

    if context_lines.is_empty() {
        return Some(query.clone());
    }

    Some(format!(
        "<warp_context>\n{}\n</warp_context>\n\n{}",
        context_lines.join("\n"),
        query
    ))
}

fn add_context_lines(context: &AIAgentContext, lines: &mut Vec<String>) {
    match context {
        AIAgentContext::Directory {
            pwd,
            home_dir,
            are_file_symbols_indexed,
        } => {
            if let Some(pwd) = pwd {
                lines.push(format!("Current directory: {pwd}"));
            }
            if let Some(home_dir) = home_dir {
                lines.push(format!("Home directory: {home_dir}"));
            }
            lines.push(format!(
                "File symbols indexed: {}",
                if *are_file_symbols_indexed {
                    "yes"
                } else {
                    "no"
                }
            ));
        }
        AIAgentContext::ExecutionEnvironment(execution_context) => {
            let mut parts = Vec::new();
            if let Some(category) = &execution_context.os.category {
                parts.push(format!("os={category}"));
            }
            if let Some(distribution) = &execution_context.os.distribution {
                parts.push(format!("distribution={distribution}"));
            }
            parts.push(format!("shell={}", execution_context.shell_name));
            if let Some(version) = &execution_context.shell_version {
                parts.push(format!("shell_version={version}"));
            }
            lines.push(format!("Execution environment: {}", parts.join(", ")));
        }
        AIAgentContext::CurrentTime { current_time } => {
            lines.push(format!("Current time: {}", current_time.to_rfc3339()));
        }
        AIAgentContext::Codebase { path, name } => {
            lines.push(format!("Codebase: {name} ({path})"));
        }
        AIAgentContext::Git { head, branch } => {
            let branch = branch.as_deref().unwrap_or("unknown");
            lines.push(format!("Git: branch={branch}, head={head}"));
        }
        AIAgentContext::Skills { skills } => {
            if skills.is_empty() {
                return;
            }

            let skill_names = skills
                .iter()
                .take(30)
                .map(|skill| {
                    format!(
                        "{} [{}:{}] - {}",
                        skill.name, skill.provider, skill.scope, skill.description
                    )
                })
                .collect::<Vec<_>>();
            let suffix = if skills.len() > skill_names.len() {
                format!("; and {} more", skills.len() - skill_names.len())
            } else {
                String::new()
            };
            lines.push(format!(
                "Available skills: {}{}",
                skill_names.join("; "),
                suffix
            ));
        }
        _ => {}
    }
}

fn validate_plain_prompt_input(input: &AIAgentInput) -> anyhow::Result<()> {
    match input {
        AIAgentInput::UserQuery {
            query,
            context,
            static_query_type,
            referenced_attachments,
            user_query_mode,
            running_command,
            intended_agent,
        } => {
            if query.trim().is_empty() {
                return Err(anyhow!("Pi local runtime requires a user prompt."));
            }

            if !context.iter().all(is_ignorable_base_context)
                || !referenced_attachments.is_empty()
                || running_command.is_some()
                || static_query_type.is_some()
                || matches!(intended_agent, Some(api::AgentType::Cli))
                || !matches!(user_query_mode, UserQueryMode::Normal)
            {
                return Err(anyhow!(
                    "Pi local runtime currently supports only plain text Agent Mode prompts. \
Prompts with selected blocks, file/project-rule context, attachments, images, running command \
context, CLI-agent routing, or mode-specific behavior require a context bridge before they can \
be routed safely."
                ));
            }

            Ok(())
        }
        _ => Err(anyhow!(
            "Pi local runtime currently supports only plain text Agent Mode prompts. \
Code reviews, skills, generated project flows, action continuations, and other structured \
agent inputs require a context bridge before they can be routed safely."
        )),
    }
}

#[cfg(test)]
fn plain_prompt_from_input(input: &AIAgentInput) -> Option<&str> {
    match input {
        AIAgentInput::UserQuery { query, .. } => Some(query),
        _ => None,
    }
}

fn is_ignorable_base_context(context: &AIAgentContext) -> bool {
    matches!(
        context,
        AIAgentContext::Directory { .. }
            | AIAgentContext::ExecutionEnvironment(_)
            | AIAgentContext::CurrentTime { .. }
            | AIAgentContext::Codebase { .. }
            | AIAgentContext::Git { .. }
            | AIAgentContext::Skills { .. }
    )
}

enum PiEventAction {
    Continue,
    Finish,
    Error(String),
}

#[derive(Default)]
struct PiOutput {
    assistant_text: String,
    tool_events: Vec<PiToolEvent>,
}

impl PiOutput {
    fn tool_summary(&self) -> Option<String> {
        if self.tool_events.is_empty() {
            return None;
        }

        let mut lines = Vec::with_capacity(self.tool_events.len() + 1);
        lines.push("Pi tool activity:".to_string());
        for event in &self.tool_events {
            lines.push(event.to_summary_line());
        }
        Some(lines.join("\n"))
    }
}

struct PiToolEvent {
    tool_call_id: String,
    tool_name: String,
    args: Option<Value>,
    outcome: PiToolOutcome,
}

enum PiToolOutcome {
    Started,
    Finished {
        is_error: bool,
        result_preview: Option<String>,
    },
}

impl PiToolEvent {
    fn to_summary_line(&self) -> String {
        let call = format!("{} ({})", self.tool_name, self.tool_call_id);
        match &self.outcome {
            PiToolOutcome::Started => {
                format!(
                    "- started {call}{}",
                    format_args_preview(self.args.as_ref())
                )
            }
            PiToolOutcome::Finished {
                is_error,
                result_preview,
            } => {
                let status = if *is_error { "failed" } else { "completed" };
                let result = result_preview
                    .as_ref()
                    .map(|preview| format!(": {preview}"))
                    .unwrap_or_default();
                format!("- {status} {call}{result}")
            }
        }
    }
}

fn pi_runtime_error(message: String) -> anyhow::Error {
    if message.contains("No API key found for openai-codex") {
        anyhow!(
            "Pi runtime requires OpenAI Codex OAuth before Warp can use the local agent. \
Run `pi`, type `/login`, select ChatGPT Plus/Pro (Codex), finish the browser login, \
then restart Warp with \
{RUNTIME_ENV}=pi_local. Original Pi error: {message}"
        )
    } else {
        anyhow!("Pi runtime error: {message}")
    }
}

fn handle_pi_event(event: &Value, output: &mut PiOutput) -> anyhow::Result<PiEventAction> {
    match event.get("type").and_then(Value::as_str) {
        Some("response") => {
            if event.get("success").and_then(Value::as_bool) == Some(false) {
                return Ok(PiEventAction::Error(
                    event
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("prompt was rejected")
                        .to_string(),
                ));
            }
            Ok(PiEventAction::Continue)
        }
        Some("message_update") => {
            let assistant_event = event.get("assistantMessageEvent");
            if let Some(delta) = assistant_event
                .filter(|event| {
                    event
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|event_type| event_type == "text_delta")
                })
                .and_then(|event| event.get("delta"))
                .and_then(Value::as_str)
            {
                output.assistant_text.push_str(delta);
            } else if output.assistant_text.is_empty() {
                if let Some(text) = assistant_event
                    .filter(|event| {
                        event
                            .get("type")
                            .and_then(Value::as_str)
                            .is_some_and(|event_type| event_type == "text_end")
                    })
                    .and_then(|event| event.get("content"))
                    .and_then(Value::as_str)
                {
                    output.assistant_text.push_str(text);
                }
            }
            Ok(PiEventAction::Continue)
        }
        Some("message_end") | Some("turn_end") => {
            if output.assistant_text.is_empty() {
                if let Some(text) = extract_message_text(event.get("message")) {
                    output.assistant_text.push_str(&text);
                }
            }
            Ok(PiEventAction::Continue)
        }
        Some("agent_end") => {
            if output.assistant_text.is_empty() {
                if let Some(text) =
                    event
                        .get("messages")
                        .and_then(Value::as_array)
                        .and_then(|messages| {
                            messages
                                .iter()
                                .rev()
                                .find_map(|m| extract_message_text(Some(m)))
                        })
                {
                    output.assistant_text.push_str(&text);
                }
            }
            Ok(PiEventAction::Finish)
        }
        Some("tool_execution_start") => {
            output.tool_events.push(PiToolEvent {
                tool_call_id: event_string(event, "toolCallId").unwrap_or_else(|| "unknown".into()),
                tool_name: event_string(event, "toolName").unwrap_or_else(|| "tool".into()),
                args: event.get("args").cloned(),
                outcome: PiToolOutcome::Started,
            });
            Ok(PiEventAction::Continue)
        }
        Some("tool_execution_end") => {
            output.tool_events.push(PiToolEvent {
                tool_call_id: event_string(event, "toolCallId").unwrap_or_else(|| "unknown".into()),
                tool_name: event_string(event, "toolName").unwrap_or_else(|| "tool".into()),
                args: event.get("args").cloned(),
                outcome: PiToolOutcome::Finished {
                    is_error: event
                        .get("isError")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    result_preview: extract_tool_result_preview(event.get("result")),
                },
            });
            Ok(PiEventAction::Continue)
        }
        Some("auto_retry_end") if event.get("success").and_then(Value::as_bool) == Some(false) => {
            Ok(PiEventAction::Error(
                event
                    .get("finalError")
                    .and_then(Value::as_str)
                    .unwrap_or("retry failed")
                    .to_string(),
            ))
        }
        Some("extension_error") => Ok(PiEventAction::Error(
            event
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("extension error")
                .to_string(),
        )),
        _ => Ok(PiEventAction::Continue),
    }
}

fn event_string(event: &Value, key: &str) -> Option<String> {
    event.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn format_args_preview(args: Option<&Value>) -> String {
    let Some(args) = args else {
        return String::new();
    };

    compact_json_preview(args)
        .map(|preview| format!(" with {preview}"))
        .unwrap_or_default()
}

fn extract_tool_result_preview(result: Option<&Value>) -> Option<String> {
    let result = result?;
    let content_text = result
        .get("content")
        .and_then(Value::as_array)
        .map(|content| {
            content
                .iter()
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|text| !text.trim().is_empty());

    content_text
        .or_else(|| compact_json_preview(result))
        .map(|preview| truncate_preview(&preview, 300))
}

fn compact_json_preview(value: &Value) -> Option<String> {
    serde_json::to_string(value)
        .ok()
        .filter(|preview| !preview.trim().is_empty())
        .map(|preview| truncate_preview(&preview, 220))
}

fn truncate_preview(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let preview = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

fn extract_message_text(message: Option<&Value>) -> Option<String> {
    let message = message?;
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return None;
    }

    match message.get("content") {
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Array(content)) => {
            let text = content
                .iter()
                .filter_map(|block| {
                    block
                        .get("text")
                        .and_then(Value::as_str)
                        .or_else(|| block.get("content").and_then(Value::as_str))
                })
                .collect::<String>();
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
}

fn init_event(request_id: String, conversation_id: String, run_id: String) -> api::ResponseEvent {
    api::ResponseEvent {
        r#type: Some(api::response_event::Type::Init(
            api::response_event::StreamInit {
                request_id,
                conversation_id,
                run_id,
            },
        )),
    }
}

fn add_agent_output_event(task_id: String, request_id: String, text: String) -> api::ResponseEvent {
    api::ResponseEvent {
        r#type: Some(api::response_event::Type::ClientActions(
            api::response_event::ClientActions {
                actions: vec![api::ClientAction {
                    action: Some(api::client_action::Action::AddMessagesToTask(
                        api::client_action::AddMessagesToTask {
                            task_id: task_id.clone(),
                            messages: vec![api::Message {
                                id: format!("pi-message-{}", Uuid::new_v4()),
                                task_id,
                                server_message_data: String::new(),
                                citations: vec![],
                                message: Some(api::message::Message::AgentOutput(
                                    api::message::AgentOutput { text },
                                )),
                                request_id,
                                timestamp: None,
                            }],
                        },
                    )),
                }],
            },
        )),
    }
}

fn create_root_task_event(task_id: String) -> api::ResponseEvent {
    api::ResponseEvent {
        r#type: Some(api::response_event::Type::ClientActions(
            api::response_event::ClientActions {
                actions: vec![api::ClientAction {
                    action: Some(api::client_action::Action::CreateTask(
                        api::client_action::CreateTask {
                            task: Some(api::Task {
                                id: task_id,
                                description: String::new(),
                                dependencies: None,
                                messages: vec![],
                                summary: String::new(),
                                server_data: String::new(),
                            }),
                        },
                    )),
                }],
            },
        )),
    }
}

fn finished_event() -> api::ResponseEvent {
    api::ResponseEvent {
        r#type: Some(api::response_event::Type::Finished(
            api::response_event::StreamFinished {
                reason: Some(api::response_event::stream_finished::Reason::Done(
                    api::response_event::stream_finished::Done {},
                )),
                conversation_usage_metadata: None,
                token_usage: vec![],
                should_refresh_model_config: false,
                request_cost: None,
            },
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::Local;
    use serde_json::json;
    use warp_multi_agent_api as api;

    use crate::ai::agent::{
        AIAgentAttachment, AIAgentContext, AIAgentInput, StaticQueryType, UserQueryMode,
    };

    use super::{
        handle_pi_event, is_pi_local_conversation_token, plain_prompt_from_input,
        prompt_from_input, sanitize_session_file_stem, validate_plain_prompt_input, PiEventAction,
        PiOutput,
    };
    use crate::ai::agent::api::ServerConversationToken;

    fn plain_user_query(query: &str) -> AIAgentInput {
        AIAgentInput::UserQuery {
            query: query.to_string(),
            context: Vec::<AIAgentContext>::new().into(),
            static_query_type: None,
            referenced_attachments: HashMap::new(),
            user_query_mode: UserQueryMode::Normal,
            running_command: None,
            intended_agent: None,
        }
    }

    #[test]
    fn accepts_plain_user_query() {
        let input = plain_user_query("explain this repo");

        validate_plain_prompt_input(&input).unwrap();
        assert_eq!(plain_prompt_from_input(&input), Some("explain this repo"));
        assert_eq!(
            prompt_from_input(&input),
            Some("explain this repo".to_string())
        );
    }

    #[test]
    fn accepts_pi_local_conversation_token() {
        let token = ServerConversationToken::new("pi-local-123".to_string());

        assert!(is_pi_local_conversation_token(&token));
    }

    #[test]
    fn rejects_non_pi_local_conversation_token() {
        let token = ServerConversationToken::new("server-conversation-token".to_string());

        assert!(!is_pi_local_conversation_token(&token));
    }

    #[test]
    fn accepts_user_query_with_base_context_and_primary_agent() {
        let input = AIAgentInput::UserQuery {
            query: "explain this repo".to_string(),
            context: Arc::from([
                AIAgentContext::CurrentTime {
                    current_time: Local::now(),
                },
                AIAgentContext::Directory {
                    pwd: Some("C:\\workspace".to_string()),
                    home_dir: Some("C:\\Users\\Markus".to_string()),
                    are_file_symbols_indexed: false,
                },
            ]),
            static_query_type: None,
            referenced_attachments: HashMap::new(),
            user_query_mode: UserQueryMode::Normal,
            running_command: None,
            intended_agent: Some(api::AgentType::Primary),
        };

        validate_plain_prompt_input(&input).unwrap();
        let prompt = prompt_from_input(&input).unwrap();
        assert!(prompt.contains("<warp_context>"));
        assert!(prompt.contains("Current directory: C:\\workspace"));
        assert!(prompt.contains("Current time:"));
        assert!(prompt.ends_with("explain this repo"));
    }

    #[test]
    fn rejects_user_query_with_context() {
        let input = AIAgentInput::UserQuery {
            query: "explain this".to_string(),
            context: Arc::from([AIAgentContext::SelectedText("selected code".to_string())]),
            static_query_type: None,
            referenced_attachments: HashMap::new(),
            user_query_mode: UserQueryMode::Normal,
            running_command: None,
            intended_agent: None,
        };

        assert!(validate_plain_prompt_input(&input).is_err());
    }

    #[test]
    fn rejects_user_query_with_attachment() {
        let mut referenced_attachments = HashMap::new();
        referenced_attachments.insert(
            "plan".to_string(),
            AIAgentAttachment::PlainText("hidden attachment text".to_string()),
        );

        let input = AIAgentInput::UserQuery {
            query: "use the plan".to_string(),
            context: Vec::<AIAgentContext>::new().into(),
            static_query_type: None,
            referenced_attachments,
            user_query_mode: UserQueryMode::Normal,
            running_command: None,
            intended_agent: None,
        };

        assert!(validate_plain_prompt_input(&input).is_err());
    }

    #[test]
    fn rejects_mode_and_static_query_metadata() {
        let input = AIAgentInput::UserQuery {
            query: "make a plan".to_string(),
            context: Vec::<AIAgentContext>::new().into(),
            static_query_type: Some(StaticQueryType::Code),
            referenced_attachments: HashMap::new(),
            user_query_mode: UserQueryMode::Plan,
            running_command: None,
            intended_agent: None,
        };

        assert!(validate_plain_prompt_input(&input).is_err());
    }

    #[test]
    fn rejects_cli_agent_routing() {
        let input = AIAgentInput::UserQuery {
            query: "inspect this running command".to_string(),
            context: Vec::<AIAgentContext>::new().into(),
            static_query_type: None,
            referenced_attachments: HashMap::new(),
            user_query_mode: UserQueryMode::Normal,
            running_command: None,
            intended_agent: Some(api::AgentType::Cli),
        };

        assert!(validate_plain_prompt_input(&input).is_err());
    }

    #[test]
    fn rejects_structured_inputs_even_if_they_have_display_queries() {
        let input = AIAgentInput::CreateNewProject {
            query: "create a rust cli".to_string(),
            context: Vec::<AIAgentContext>::new().into(),
        };

        assert!(input.user_query().is_some());
        assert!(validate_plain_prompt_input(&input).is_err());
        assert_eq!(plain_prompt_from_input(&input), None);
        assert_eq!(prompt_from_input(&input), None);
    }

    #[test]
    fn sanitizes_session_file_stems() {
        assert_eq!(
            sanitize_session_file_stem("pi-local-abc/..\\def:ghi"),
            "pi-local-abc____def_ghi"
        );
    }

    #[test]
    fn records_pi_tool_lifecycle_summary() {
        let mut output = PiOutput::default();

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_1",
                "toolName": "read",
                "args": { "path": "README.md" }
            }),
            &mut output,
        )
        .unwrap();
        assert!(matches!(action, PiEventAction::Continue));

        handle_pi_event(
            &json!({
                "type": "tool_execution_end",
                "toolCallId": "call_1",
                "toolName": "read",
                "isError": false,
                "result": {
                    "content": [
                        { "type": "text", "text": "file contents" }
                    ]
                }
            }),
            &mut output,
        )
        .unwrap();

        let summary = output.tool_summary().unwrap();
        assert!(summary.contains("started read (call_1)"));
        assert!(summary.contains("{\"path\":\"README.md\"}"));
        assert!(summary.contains("completed read (call_1): file contents"));
    }

    #[test]
    fn pi_agent_text_and_tool_events_are_kept_separate() {
        let mut output = PiOutput::default();

        handle_pi_event(
            &json!({
                "type": "tool_execution_end",
                "toolCallId": "call_1",
                "toolName": "grep",
                "isError": true,
                "result": {
                    "content": [
                        { "type": "text", "text": "grep failed" }
                    ]
                }
            }),
            &mut output,
        )
        .unwrap();
        handle_pi_event(
            &json!({
                "type": "message_update",
                "assistantMessageEvent": {
                    "type": "text_delta",
                    "delta": "hello"
                }
            }),
            &mut output,
        )
        .unwrap();

        assert_eq!(output.assistant_text, "hello");
        assert!(output.tool_summary().unwrap().contains("failed grep"));
    }
}
