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

use crate::ai::agent::{
    AIAgentAttachment, AIAgentContext, AIAgentInput, AnyFileContent, RunningCommand, UserQueryMode,
};
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

        if let Err(err) = validate_one_shot_request(&params) {
            yield Err(Arc::new(AIApiError::Other(err)));
            return;
        }

        let prompt = match prompt_from_params(&params)
            .filter(|prompt| !prompt.trim().is_empty())
            .ok_or_else(|| anyhow!("Pi local agent requires a user prompt")) {
            Ok(prompt) => prompt,
            Err(err) => {
                yield Err(Arc::new(AIApiError::Other(err)));
                return;
            }
        };

        let mut child = match pi_command(&params, &conversation_id)
            .and_then(|mut command| {
                command
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .spawn()
                    .with_context(|| {
                        format!(
                            "Failed to start Pi runtime. Install @mariozechner/pi-coding-agent or set {PI_COMMAND_ENV}"
                        )
                    })
            }) {
            Ok(child) => child,
            Err(err) => {
                yield Err(Arc::new(AIApiError::Other(err)));
                return;
            }
        };

        let mut stdin = match child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("Pi runtime stdin was unavailable")) {
            Ok(stdin) => stdin,
            Err(err) => {
                yield Err(Arc::new(AIApiError::Other(err)));
                let _ = child.kill();
                return;
            }
        };
        let stdout = match child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("Pi runtime stdout was unavailable")) {
            Ok(stdout) => stdout,
            Err(err) => {
                yield Err(Arc::new(AIApiError::Other(err)));
                let _ = child.kill();
                return;
            }
        };

        yield Ok(init_event(
            request_id.clone(),
            conversation_id.clone(),
            run_id,
        ));
        if should_create_root_task {
            yield Ok(create_root_task_event(task_id.clone()));
        }

        let prompt_command = serde_json::json!({
            "id": request_id.clone(),
            "type": "prompt",
            "message": prompt,
        });
        if let Err(err) = write_pi_prompt(&mut stdin, &prompt_command).await {
            yield Err(Arc::new(AIApiError::Other(err)));
            let _ = child.kill();
            return;
        }

        let mut lines = BufReader::new(stdout).lines();
        let mut stream_state = PiStreamState::new(task_id.clone(), request_id.clone());
        let mut cancellation_rx = cancellation_rx.fuse();

        loop {
            futures::select! {
                _ = cancellation_rx => {
                    let _ = stdin.write_all(b"{\"type\":\"abort\"}\n").await;
                    let _ = stdin.flush().await;
                    let _ = child.kill();
                    return;
                }
                line = lines.next().fuse() => {
                    let Some(line) = line else {
                        break;
                    };
                    let line = match line.context("Failed reading Pi runtime output") {
                        Ok(line) => line,
                        Err(err) => {
                            yield Err(Arc::new(AIApiError::Other(err)));
                            let _ = child.kill();
                            return;
                        }
                    };
                    if line.trim().is_empty() {
                        continue;
                    }
                    let event: Value = match serde_json::from_str(&line)
                        .with_context(|| format!("Pi runtime emitted invalid JSON: {line}")) {
                        Ok(event) => event,
                        Err(err) => {
                            yield Err(Arc::new(AIApiError::Other(err)));
                            let _ = child.kill();
                            return;
                        }
                    };

                    if let Some((response, events)) = handle_extension_ui_request(&event, &mut stream_state) {
                        for event in events {
                            yield Ok(event);
                        }
                        if let Some(response) = response {
                            if let Err(err) = write_pi_prompt(&mut stdin, &response).await {
                                yield Err(Arc::new(AIApiError::Other(err)));
                                let _ = child.kill();
                                return;
                            }
                        }
                        continue;
                    }

                    match handle_pi_event(&event, &mut stream_state) {
                        Ok(PiEventAction::Continue(events)) => {
                            for event in events {
                                yield Ok(event);
                            }
                        }
                        Ok(PiEventAction::Finish(events)) => {
                            for event in events {
                                yield Ok(event);
                            }
                            break;
                        }
                        Ok(PiEventAction::Error(message)) => {
                            yield Err(Arc::new(AIApiError::Other(pi_runtime_error(message))));
                            let _ = child.kill();
                            return;
                        }
                        Err(err) => {
                            yield Err(Arc::new(AIApiError::Other(err)));
                            let _ = child.kill();
                            return;
                        }
                    }
                }
            }
        }

        if !stream_state.has_assistant_output() {
            yield Err(Arc::new(AIApiError::Other(anyhow!(
                "Pi runtime completed without assistant output. Check Pi authentication, selected model, and local Pi logs."
            ))));
            let _ = child.kill();
            return;
        }

        yield Ok(finished_event());
        let _ = child.kill();
    })
}

async fn write_pi_prompt(
    stdin: &mut async_process::ChildStdin,
    prompt_command: &Value,
) -> anyhow::Result<()> {
    stdin
        .write_all(prompt_command.to_string().as_bytes())
        .await
        .context("Failed to write prompt to Pi runtime")?;
    stdin
        .write_all(b"\n")
        .await
        .context("Failed to finish Pi prompt frame")?;
    stdin.flush().await.context("Failed to flush Pi stdin")
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
            "Pi local runtime supports only pi-local conversations with one input frame per \
request. Forked server conversations require a conversation-history bridge before they can be \
routed safely."
        ));
    }

    validate_prompt_input(params.input.first().expect("checked input length above"))
}

fn is_pi_local_conversation_token(token: &ServerConversationToken) -> bool {
    token.as_str().starts_with(LOCAL_CONVERSATION_PREFIX)
}

fn prompt_from_params(params: &RequestParams) -> Option<String> {
    params.input.iter().rev().find_map(prompt_from_input)
}

fn prompt_from_input(input: &AIAgentInput) -> Option<String> {
    let mut context_lines = Vec::new();
    let body = match input {
        AIAgentInput::UserQuery {
            query,
            context,
            static_query_type,
            referenced_attachments,
            user_query_mode,
            running_command,
            ..
        } => {
            for context in context.iter() {
                add_context_lines(context, &mut context_lines);
            }
            if let Some(static_query_type) = static_query_type {
                context_lines.push(format!("Warp entrypoint query type: {static_query_type:?}"));
            }
            if !matches!(user_query_mode, UserQueryMode::Normal) {
                context_lines.push(format!("Warp query mode: {user_query_mode:?}"));
            }
            if let Some(running_command) = running_command {
                add_running_command_lines(running_command, &mut context_lines);
            }
            add_attachment_lines(referenced_attachments, &mut context_lines);
            query.clone()
        }
        AIAgentInput::ActionResult { result, context } => {
            for context in context.iter() {
                add_context_lines(context, &mut context_lines);
            }
            format!(
                "<warp_action_result id=\"{}\" task_id=\"{}\">\n{}\n</warp_action_result>\n\nContinue from the Warp action result above.",
                result.id, result.task_id, result
            )
        }
        _ => return None,
    };

    if context_lines.is_empty() {
        return Some(body);
    }

    Some(format!(
        "<warp_context>\n{}\n</warp_context>\n\n{}",
        context_lines.join("\n"),
        body
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
        AIAgentContext::SelectedText(text) => {
            lines.push(format!(
                "Selected text:\n```text\n{}\n```",
                truncate_preview(text, 8_000)
            ));
        }
        AIAgentContext::Image(image) => {
            lines.push(format!(
                "Attached image: file_name={}, mime_type={}, figma={}",
                image.file_name, image.mime_type, image.is_figma
            ));
        }
        AIAgentContext::Codebase { path, name } => {
            lines.push(format!("Codebase: {name} ({path})"));
        }
        AIAgentContext::ProjectRules {
            root_path,
            active_rules,
            additional_rule_paths,
        } => {
            lines.push(format!("Project rules root: {root_path}"));
            for rule in active_rules.iter().take(20) {
                let content = file_context_content_preview(rule, 4_000);
                lines.push(format!(
                    "Active project rule: {}{}",
                    rule,
                    content
                        .map(|content| format!("\n```text\n{content}\n```"))
                        .unwrap_or_default()
                ));
            }
            if !additional_rule_paths.is_empty() {
                lines.push(format!(
                    "Additional project rule paths: {}",
                    additional_rule_paths.join(", ")
                ));
            }
        }
        AIAgentContext::File(file) => {
            let content = file_context_content_preview(file, 8_000);
            lines.push(format!(
                "Relevant file: {}{}",
                file,
                content
                    .map(|content| format!("\n```text\n{content}\n```"))
                    .unwrap_or_default()
            ));
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
        AIAgentContext::Block(block) => {
            lines.push(format!(
                "Terminal block: command=`{}` exit_code={}{}{}{}{}{}\n```text\n{}\n```",
                block.command,
                block.exit_code.value(),
                block
                    .pwd
                    .as_ref()
                    .map(|pwd| format!(", cwd={pwd}"))
                    .unwrap_or_default(),
                block
                    .shell
                    .as_ref()
                    .map(|shell| format!(", shell={shell}"))
                    .unwrap_or_default(),
                block
                    .git_branch
                    .as_ref()
                    .map(|branch| format!(", git_branch={branch}"))
                    .unwrap_or_default(),
                block
                    .started_ts
                    .as_ref()
                    .map(|ts| format!(", started={}", ts.to_rfc3339()))
                    .unwrap_or_default(),
                block
                    .finished_ts
                    .as_ref()
                    .map(|ts| format!(", finished={}", ts.to_rfc3339()))
                    .unwrap_or_default(),
                truncate_preview(&block.output, 12_000)
            ));
        }
    }
}

fn file_context_content_preview(
    file: &crate::ai::agent::FileContext,
    max_chars: usize,
) -> Option<String> {
    match &file.content {
        AnyFileContent::StringContent(content) if !content.trim().is_empty() => {
            Some(truncate_preview(content, max_chars))
        }
        AnyFileContent::BinaryContent(content) if !content.is_empty() => Some(format!(
            "<binary content: {} bytes, {} total lines>",
            content.len(),
            file.line_count
        )),
        _ => None,
    }
}

fn add_attachment_lines(
    attachments: &std::collections::HashMap<String, AIAgentAttachment>,
    lines: &mut Vec<String>,
) {
    for (name, attachment) in attachments.iter() {
        match attachment {
            AIAgentAttachment::PlainText(text) => {
                lines.push(format!(
                    "Attachment {name}:\n```text\n{}\n```",
                    truncate_preview(text, 12_000)
                ));
            }
            AIAgentAttachment::DocumentContent {
                document_id,
                content,
                source,
                line_range,
            } => {
                lines.push(format!(
                    "Document attachment {name}: id={document_id}, source={source:?}, line_range={line_range:?}\n```text\n{}\n```",
                    truncate_preview(content, 12_000)
                ));
            }
            AIAgentAttachment::DiffHunk {
                file_path,
                diff_content,
                lines_added,
                lines_removed,
                ..
            } => {
                lines.push(format!(
                    "Diff attachment {name}: file={file_path}, +{lines_added}/-{lines_removed}\n```diff\n{}\n```",
                    truncate_preview(diff_content, 12_000)
                ));
            }
            AIAgentAttachment::DiffSet { file_diffs, .. } => {
                lines.push(format!(
                    "Diff set attachment {name}: files={}",
                    file_diffs.keys().cloned().collect::<Vec<_>>().join(", ")
                ));
            }
            AIAgentAttachment::FilePathReference {
                file_name,
                file_path,
                ..
            } => {
                lines.push(format!(
                    "File path attachment {name}: file_name={file_name}, path={file_path}"
                ));
            }
            AIAgentAttachment::DriveObject { uid, payload } => {
                lines.push(format!(
                    "Drive object attachment {name}: uid={uid}, payload={}",
                    payload
                        .as_ref()
                        .and_then(|payload| serde_json::to_string(payload).ok())
                        .map(|payload| truncate_preview(&payload, 4_000))
                        .unwrap_or_else(|| "<none>".to_string())
                ));
            }
            AIAgentAttachment::Block(block) => {
                lines.push(format!(
                    "Block attachment {name}: command=`{}` exit_code={}\n```text\n{}\n```",
                    block.command,
                    block.exit_code.value(),
                    truncate_preview(&block.output, 12_000)
                ));
            }
        }
    }
}

fn add_running_command_lines(command: &RunningCommand, lines: &mut Vec<String>) {
    lines.push(format!(
        "Running command: command=`{}`, block_id={}, alt_screen={}, requested_action_id={}",
        command.command,
        command.block_id,
        command.is_alt_screen_active,
        command
            .requested_command_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "<none>".to_string())
    ));
    if !command.grid_contents.trim().is_empty() {
        lines.push(format!(
            "Running command terminal contents:\n```text\n{}\n```",
            truncate_preview(&command.grid_contents, 12_000)
        ));
    }
    if !command.cursor.trim().is_empty() {
        lines.push(format!(
            "Running command cursor context:\n```text\n{}\n```",
            truncate_preview(&command.cursor, 2_000)
        ));
    }
}

fn validate_prompt_input(input: &AIAgentInput) -> anyhow::Result<()> {
    match input {
        AIAgentInput::UserQuery {
            query,
            intended_agent,
            ..
        } => {
            if query.trim().is_empty() {
                return Err(anyhow!("Pi local runtime requires a user prompt."));
            }

            if matches!(intended_agent, Some(api::AgentType::Cli)) {
                return Err(anyhow!(
                    "Pi local runtime handles Warp Agent Mode prompts. CLI-agent routing still \
belongs to the terminal-native CLI integration."
                ));
            }

            Ok(())
        }
        AIAgentInput::ActionResult { .. } => Ok(()),
        _ => Err(anyhow!(
            "Pi local runtime currently supports user prompts and Warp action-result \
continuations. Other structured agent inputs still require dedicated prompt adapters."
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

enum PiEventAction {
    Continue(Vec<api::ResponseEvent>),
    Finish(Vec<api::ResponseEvent>),
    Error(String),
}

struct PiStreamState {
    task_id: String,
    request_id: String,
    assistant_message_id: String,
    tool_message_id: String,
    assistant_message_started: bool,
    tool_message_started: bool,
    assistant_output_seen: bool,
}

impl PiStreamState {
    fn new(task_id: String, request_id: String) -> Self {
        Self {
            task_id,
            request_id,
            assistant_message_id: format!("pi-message-{}", Uuid::new_v4()),
            tool_message_id: format!("pi-tool-message-{}", Uuid::new_v4()),
            assistant_message_started: false,
            tool_message_started: false,
            assistant_output_seen: false,
        }
    }

    fn has_assistant_output(&self) -> bool {
        self.assistant_output_seen
    }

    fn push_assistant_delta(&mut self, text: &str) -> Vec<api::ResponseEvent> {
        if text.is_empty() {
            return Vec::new();
        }

        self.assistant_output_seen = true;
        if self.assistant_message_started {
            vec![append_agent_output_event(
                self.task_id.clone(),
                self.request_id.clone(),
                self.assistant_message_id.clone(),
                text.to_string(),
            )]
        } else {
            self.assistant_message_started = true;
            vec![add_agent_output_event_with_id(
                self.task_id.clone(),
                self.request_id.clone(),
                self.assistant_message_id.clone(),
                text.to_string(),
            )]
        }
    }

    fn push_tool_event(&mut self, event: PiToolEvent) -> Vec<api::ResponseEvent> {
        let line = event.to_summary_line();
        if self.tool_message_started {
            vec![append_agent_output_event(
                self.task_id.clone(),
                self.request_id.clone(),
                self.tool_message_id.clone(),
                format!("\n{line}"),
            )]
        } else {
            self.tool_message_started = true;
            vec![add_agent_output_event_with_id(
                self.task_id.clone(),
                self.request_id.clone(),
                self.tool_message_id.clone(),
                format!("Pi tool activity:\n{line}"),
            )]
        }
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

fn handle_extension_ui_request(
    event: &Value,
    state: &mut PiStreamState,
) -> Option<(Option<Value>, Vec<api::ResponseEvent>)> {
    if event.get("type").and_then(Value::as_str) != Some("extension_ui_request") {
        return None;
    }

    let id = event.get("id").and_then(Value::as_str)?.to_string();
    let method = event
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let title = event.get("title").and_then(Value::as_str);
    let message = event
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| event.get("statusText").and_then(Value::as_str))
        .or_else(|| event.get("text").and_then(Value::as_str));

    let summary = PiToolEvent {
        tool_call_id: id.clone(),
        tool_name: format!("extension_ui.{method}"),
        args: Some(serde_json::json!({
            "title": title,
            "message": message.map(|message| truncate_preview(message, 300)),
        })),
        outcome: PiToolOutcome::Finished {
            is_error: false,
            result_preview: match method {
                "select" | "confirm" | "input" | "editor" => {
                    Some("cancelled by Warp OSS fallback UI bridge".to_string())
                }
                _ => Some("noted".to_string()),
            },
        },
    };
    let events = state.push_tool_event(summary);

    let response = match method {
        "select" | "input" | "editor" => Some(serde_json::json!({
            "type": "extension_ui_response",
            "id": id,
            "cancelled": true,
        })),
        "confirm" => Some(serde_json::json!({
            "type": "extension_ui_response",
            "id": id,
            "confirmed": false,
        })),
        _ => None,
    };

    Some((response, events))
}

fn handle_pi_event(event: &Value, state: &mut PiStreamState) -> anyhow::Result<PiEventAction> {
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
            Ok(PiEventAction::Continue(Vec::new()))
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
                Ok(PiEventAction::Continue(state.push_assistant_delta(delta)))
            } else if !state.has_assistant_output() {
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
                    Ok(PiEventAction::Continue(state.push_assistant_delta(text)))
                } else {
                    Ok(PiEventAction::Continue(Vec::new()))
                }
            } else {
                Ok(PiEventAction::Continue(Vec::new()))
            }
        }
        Some("message_end") | Some("turn_end") => {
            if !state.has_assistant_output() {
                if let Some(text) = extract_message_text(event.get("message")) {
                    return Ok(PiEventAction::Continue(state.push_assistant_delta(&text)));
                }
            }
            Ok(PiEventAction::Continue(Vec::new()))
        }
        Some("agent_end") => {
            let mut events = Vec::new();
            if !state.has_assistant_output() {
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
                    events.extend(state.push_assistant_delta(&text));
                }
            }
            Ok(PiEventAction::Finish(events))
        }
        Some("tool_execution_start") => {
            let event = PiToolEvent {
                tool_call_id: event_string(event, "toolCallId").unwrap_or_else(|| "unknown".into()),
                tool_name: event_string(event, "toolName").unwrap_or_else(|| "tool".into()),
                args: event.get("args").cloned(),
                outcome: PiToolOutcome::Started,
            };
            Ok(PiEventAction::Continue(state.push_tool_event(event)))
        }
        Some("tool_execution_end") => {
            let event = PiToolEvent {
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
            };
            Ok(PiEventAction::Continue(state.push_tool_event(event)))
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
        _ => Ok(PiEventAction::Continue(Vec::new())),
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

fn add_agent_output_event_with_id(
    task_id: String,
    request_id: String,
    message_id: String,
    text: String,
) -> api::ResponseEvent {
    api::ResponseEvent {
        r#type: Some(api::response_event::Type::ClientActions(
            api::response_event::ClientActions {
                actions: vec![api::ClientAction {
                    action: Some(api::client_action::Action::AddMessagesToTask(
                        api::client_action::AddMessagesToTask {
                            task_id: task_id.clone(),
                            messages: vec![api::Message {
                                id: message_id,
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

fn append_agent_output_event(
    task_id: String,
    request_id: String,
    message_id: String,
    text: String,
) -> api::ResponseEvent {
    api::ResponseEvent {
        r#type: Some(api::response_event::Type::ClientActions(
            api::response_event::ClientActions {
                actions: vec![api::ClientAction {
                    action: Some(api::client_action::Action::AppendToMessageContent(
                        api::client_action::AppendToMessageContent {
                            task_id: task_id.clone(),
                            message: Some(api::Message {
                                id: message_id,
                                task_id,
                                server_message_data: String::new(),
                                citations: vec![],
                                message: Some(api::message::Message::AgentOutput(
                                    api::message::AgentOutput { text },
                                )),
                                request_id,
                                timestamp: None,
                            }),
                            mask: Some(prost_types::FieldMask {
                                paths: vec!["agent_output.text".to_string()],
                            }),
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
    use field_mask::FieldMaskOperation;
    use serde_json::json;
    use warp_multi_agent_api as api;

    use crate::ai::agent::{
        AIAgentActionId, AIAgentActionResult, AIAgentActionResultType, AIAgentAttachment,
        AIAgentContext, AIAgentInput, RequestCommandOutputResult, StaticQueryType, TaskId,
        UserQueryMode,
    };
    use warp_core::command::ExitCode;

    use super::{
        append_agent_output_event, handle_extension_ui_request, handle_pi_event,
        is_pi_local_conversation_token, plain_prompt_from_input, prompt_from_input,
        sanitize_session_file_stem, validate_prompt_input, PiEventAction, PiStreamState,
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

    fn event_agent_output_text(event: &api::ResponseEvent) -> Option<&str> {
        let api::response_event::Type::ClientActions(actions) = event.r#type.as_ref()? else {
            return None;
        };
        let action = actions.actions.first()?.action.as_ref()?;
        let message = match action {
            api::client_action::Action::AddMessagesToTask(add) => add.messages.first(),
            api::client_action::Action::AppendToMessageContent(append) => append.message.as_ref(),
            _ => None,
        }?;
        let api::message::Message::AgentOutput(output) = message.message.as_ref()? else {
            return None;
        };
        Some(&output.text)
    }

    fn agent_output_message(id: &str, text: &str) -> api::Message {
        api::Message {
            id: id.to_string(),
            task_id: "task".to_string(),
            server_message_data: String::new(),
            citations: vec![],
            message: Some(api::message::Message::AgentOutput(
                api::message::AgentOutput {
                    text: text.to_string(),
                },
            )),
            request_id: "request".to_string(),
            timestamp: None,
        }
    }

    #[test]
    fn accepts_plain_user_query() {
        let input = plain_user_query("explain this repo");

        validate_prompt_input(&input).unwrap();
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

        validate_prompt_input(&input).unwrap();
        let prompt = prompt_from_input(&input).unwrap();
        assert!(prompt.contains("<warp_context>"));
        assert!(prompt.contains("Current directory: C:\\workspace"));
        assert!(prompt.contains("Current time:"));
        assert!(prompt.ends_with("explain this repo"));
    }

    #[test]
    fn accepts_user_query_with_selected_text_context() {
        let input = AIAgentInput::UserQuery {
            query: "explain this".to_string(),
            context: Arc::from([AIAgentContext::SelectedText("selected code".to_string())]),
            static_query_type: None,
            referenced_attachments: HashMap::new(),
            user_query_mode: UserQueryMode::Normal,
            running_command: None,
            intended_agent: None,
        };

        validate_prompt_input(&input).unwrap();
        let prompt = prompt_from_input(&input).unwrap();
        assert!(prompt.contains("Selected text:"));
        assert!(prompt.contains("selected code"));
    }

    #[test]
    fn accepts_user_query_with_text_attachment() {
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

        validate_prompt_input(&input).unwrap();
        let prompt = prompt_from_input(&input).unwrap();
        assert!(prompt.contains("Attachment plan:"));
        assert!(prompt.contains("hidden attachment text"));
    }

    #[test]
    fn accepts_mode_and_static_query_metadata() {
        let input = AIAgentInput::UserQuery {
            query: "make a plan".to_string(),
            context: Vec::<AIAgentContext>::new().into(),
            static_query_type: Some(StaticQueryType::Code),
            referenced_attachments: HashMap::new(),
            user_query_mode: UserQueryMode::Plan,
            running_command: None,
            intended_agent: None,
        };

        validate_prompt_input(&input).unwrap();
        let prompt = prompt_from_input(&input).unwrap();
        assert!(prompt.contains("Warp entrypoint query type: Code"));
        assert!(prompt.contains("Warp query mode: Plan"));
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

        assert!(validate_prompt_input(&input).is_err());
    }

    #[test]
    fn rejects_structured_inputs_even_if_they_have_display_queries() {
        let input = AIAgentInput::CreateNewProject {
            query: "create a rust cli".to_string(),
            context: Vec::<AIAgentContext>::new().into(),
        };

        assert!(input.user_query().is_some());
        assert!(validate_prompt_input(&input).is_err());
        assert_eq!(plain_prompt_from_input(&input), None);
        assert_eq!(prompt_from_input(&input), None);
    }

    #[test]
    fn accepts_action_result_continuation_prompt() {
        let input = AIAgentInput::ActionResult {
            result: AIAgentActionResult {
                id: AIAgentActionId::from("action-1".to_string()),
                task_id: TaskId::new("task-1".to_string()),
                result: AIAgentActionResultType::RequestCommandOutput(
                    RequestCommandOutputResult::Completed {
                        block_id: Default::default(),
                        command: "echo hi".to_string(),
                        output: "hi".to_string(),
                        exit_code: ExitCode::from(0),
                    },
                ),
            },
            context: Vec::<AIAgentContext>::new().into(),
        };

        validate_prompt_input(&input).unwrap();
        let prompt = prompt_from_input(&input).unwrap();
        assert!(prompt.contains("<warp_action_result id=\"action-1\" task_id=\"task-1\">"));
        assert!(prompt.contains("echo hi"));
        assert!(prompt.contains("Continue from the Warp action result above."));
    }

    #[test]
    fn sanitizes_session_file_stems() {
        assert_eq!(
            sanitize_session_file_stem("pi-local-abc/..\\def:ghi"),
            "pi-local-abc____def_ghi"
        );
    }

    #[test]
    fn records_pi_tool_lifecycle_events_as_streamed_messages() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_1",
                "toolName": "read",
                "args": { "path": "README.md" }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Continue(events) = action else {
            panic!("expected continue action");
        };
        assert_eq!(events.len(), 1);
        let text = event_agent_output_text(&events[0]).unwrap();
        assert!(text.contains("Pi tool activity:"));
        assert!(text.contains("started read (call_1)"));
        assert!(text.contains("{\"path\":\"README.md\"}"));

        let action = handle_pi_event(
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
            &mut state,
        )
        .unwrap();
        let PiEventAction::Continue(events) = action else {
            panic!("expected continue action");
        };
        assert_eq!(events.len(), 1);
        let text = event_agent_output_text(&events[0]).unwrap();
        assert!(text.contains("completed read (call_1): file contents"));
    }

    #[test]
    fn extension_ui_dialog_requests_are_cancelled_without_deadlock() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());
        let (response, events) = handle_extension_ui_request(
            &json!({
                "type": "extension_ui_request",
                "id": "dialog-1",
                "method": "confirm",
                "title": "Allow?",
                "message": "Run command?"
            }),
            &mut state,
        )
        .expect("expected extension ui handling");

        assert_eq!(
            response,
            Some(json!({
                "type": "extension_ui_response",
                "id": "dialog-1",
                "confirmed": false,
            }))
        );
        assert!(event_agent_output_text(&events[0])
            .unwrap()
            .contains("extension_ui.confirm"));
    }

    #[test]
    fn extension_ui_fire_and_forget_requests_do_not_get_responses() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());
        let (response, events) = handle_extension_ui_request(
            &json!({
                "type": "extension_ui_request",
                "id": "notify-1",
                "method": "notify",
                "message": "hello"
            }),
            &mut state,
        )
        .expect("expected extension ui handling");

        assert_eq!(response, None);
        assert!(event_agent_output_text(&events[0])
            .unwrap()
            .contains("extension_ui.notify"));
    }

    #[test]
    fn pi_agent_text_and_tool_events_are_kept_separate() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let tool_action = handle_pi_event(
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
            &mut state,
        )
        .unwrap();
        let text_action = handle_pi_event(
            &json!({
                "type": "message_update",
                "assistantMessageEvent": {
                    "type": "text_delta",
                    "delta": "hello"
                }
            }),
            &mut state,
        )
        .unwrap();

        assert!(state.has_assistant_output());
        let PiEventAction::Continue(tool_events) = tool_action else {
            panic!("expected tool continue action");
        };
        let PiEventAction::Continue(text_events) = text_action else {
            panic!("expected text continue action");
        };
        assert!(event_agent_output_text(&tool_events[0])
            .unwrap()
            .contains("failed grep"));
        assert_eq!(event_agent_output_text(&text_events[0]), Some("hello"));
    }

    #[test]
    fn append_agent_output_field_mask_appends_text() {
        let existing = agent_output_message("message", "hel");
        let append = append_agent_output_event(
            "task".to_string(),
            "request".to_string(),
            "message".to_string(),
            "lo".to_string(),
        );
        let api::response_event::Type::ClientActions(actions) = append.r#type.unwrap() else {
            panic!("expected client actions");
        };
        let Some(api::client_action::Action::AppendToMessageContent(append)) = actions
            .actions
            .into_iter()
            .next()
            .and_then(|action| action.action)
        else {
            panic!("expected append action");
        };

        let merged = FieldMaskOperation::append(
            &api::MESSAGE_DESCRIPTOR,
            &existing,
            &append.message.unwrap(),
            append.mask.unwrap(),
        )
        .apply()
        .unwrap();

        assert_eq!(
            event_agent_output_text(&api::ResponseEvent {
                r#type: Some(api::response_event::Type::ClientActions(
                    api::response_event::ClientActions {
                        actions: vec![api::ClientAction {
                            action: Some(api::client_action::Action::AddMessagesToTask(
                                api::client_action::AddMessagesToTask {
                                    task_id: "task".to_string(),
                                    messages: vec![merged],
                                },
                            )),
                        }],
                    },
                )),
            }),
            Some("hello")
        );
    }
}
