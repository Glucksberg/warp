use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context as _, anyhow};
use async_process::Command;
use futures::FutureExt as _;
use futures_lite::StreamExt as _;
use futures_lite::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
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
const PI_DISABLE_ACTION_PROXY_ENV: &str = "WARP_PI_DISABLE_ACTION_PROXY";
const PI_SESSION_DIR_ENV: &str = "WARP_PI_SESSION_DIR";
const LOCAL_CONVERSATION_PREFIX: &str = "pi-local-";
const DEFAULT_PI_PROVIDER: &str = "openai-codex";
const DEFAULT_PI_MODEL: &str = "gpt-5.5";
const DEFAULT_PI_THINKING: &str = "high";
const READONLY_PI_TOOLS: &str = "read,grep,find,ls";
const WARP_NATIVE_PI_TOOLS: &str = "bash,edit,write,warp_mcp_call,warp_mcp_read_resource,warp_lrc_write,warp_lrc_read,warp_lrc_transfer";
const DEFAULT_PI_TOOLS: &str = "read,grep,find,ls,bash,edit,write,warp_mcp_call,warp_mcp_read_resource,warp_lrc_write,warp_lrc_read,warp_lrc_transfer";
const ALL_PI_TOOLS: &str = DEFAULT_PI_TOOLS;
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
        let mut stream_state = PiStreamState::new_with_cwd(
            task_id.clone(),
            request_id.clone(),
            params
                .session_context
                .current_working_directory()
                .as_ref()
                .map(PathBuf::from),
        );
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

        if !stream_state.has_stream_output() {
            yield Err(Arc::new(AIApiError::Other(anyhow!(
                "Pi runtime completed without assistant output or Warp action output. Check Pi authentication, selected model, and local Pi logs."
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
            command.arg("--tools").arg(DEFAULT_PI_TOOLS);
            PiToolsConfig::Enabled
        }
        Some(value) if value.is_empty() || value.eq_ignore_ascii_case("none") => {
            command.arg("--no-tools");
            PiToolsConfig::Disabled
        }
        Some(value) => {
            let normalized = if value.eq_ignore_ascii_case("readonly") {
                READONLY_PI_TOOLS
            } else if value.eq_ignore_ascii_case("warp-native") {
                WARP_NATIVE_PI_TOOLS
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
    let mut prompt = params.input.iter().rev().find_map(prompt_from_input)?;
    let mcp_context = format_mcp_context(params.mcp_context.as_ref());
    if let Some(mcp_context) = mcp_context {
        prompt = format!("{mcp_context}\n\n{prompt}");
    }
    Some(prompt)
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

fn format_mcp_context(mcp_context: Option<&crate::ai::agent::MCPContext>) -> Option<String> {
    let mcp_context = mcp_context?;
    let mut lines = Vec::new();

    #[allow(deprecated)]
    for resource in &mcp_context.resources {
        lines.push(format!(
            "- resource uri={} name={}",
            resource.raw.uri, resource.raw.name
        ));
    }

    #[allow(deprecated)]
    for tool in &mcp_context.tools {
        let description = tool
            .description
            .as_ref()
            .map(|description| description.to_string())
            .unwrap_or_default();
        lines.push(format!("- tool name={} {}", tool.name, description));
    }

    for server in &mcp_context.servers {
        lines.push(format!(
            "- server id={} name={} {}",
            server.id, server.name, server.description
        ));
        for resource in &server.resources {
            lines.push(format!(
                "  - resource uri={} name={}",
                resource.raw.uri, resource.raw.name
            ));
        }
        for tool in &server.tools {
            let description = tool
                .description
                .as_ref()
                .map(|description| description.to_string())
                .unwrap_or_default();
            lines.push(format!("  - tool name={} {}", tool.name, description));
        }
    }

    (!lines.is_empty()).then(|| {
        format!(
            "<warp_mcp_context>\nUse warp_mcp_call for MCP tools and warp_mcp_read_resource for MCP resources. Include serverId when a server id is listed.\n{}\n</warp_mcp_context>",
            lines.join("\n")
        )
    })
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
    current_working_directory: Option<PathBuf>,
    assistant_message_id: String,
    tool_message_id: String,
    assistant_message_started: bool,
    tool_message_started: bool,
    assistant_output_seen: bool,
    warp_action_output_seen: bool,
}

impl PiStreamState {
    #[cfg(test)]
    fn new(task_id: String, request_id: String) -> Self {
        Self::new_with_cwd(task_id, request_id, None)
    }

    fn new_with_cwd(
        task_id: String,
        request_id: String,
        current_working_directory: Option<PathBuf>,
    ) -> Self {
        Self {
            task_id,
            request_id,
            current_working_directory,
            assistant_message_id: format!("pi-message-{}", Uuid::new_v4()),
            tool_message_id: format!("pi-tool-message-{}", Uuid::new_v4()),
            assistant_message_started: false,
            tool_message_started: false,
            assistant_output_seen: false,
            warp_action_output_seen: false,
        }
    }

    fn has_assistant_output(&self) -> bool {
        self.assistant_output_seen
    }

    fn has_stream_output(&self) -> bool {
        self.assistant_output_seen || self.warp_action_output_seen
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

    fn push_warp_bash_action(
        &mut self,
        tool_call_id: String,
        command: String,
    ) -> Vec<api::ResponseEvent> {
        self.push_warp_tool_action(
            tool_call_id,
            api::message::tool_call::Tool::RunShellCommand(
                api::message::tool_call::RunShellCommand {
                    command,
                    is_read_only: false,
                    uses_pager: false,
                    citations: vec![],
                    is_risky: true,
                    risk_category: api::RiskCategory::Risky as i32,
                    wait_until_complete_value: Some(
                        api::message::tool_call::run_shell_command::WaitUntilCompleteValue::WaitUntilComplete(true),
                    ),
                },
            ),
        )
    }

    fn push_warp_tool_action(
        &mut self,
        tool_call_id: String,
        tool: api::message::tool_call::Tool,
    ) -> Vec<api::ResponseEvent> {
        self.warp_action_output_seen = true;
        vec![add_tool_call_event(
            self.task_id.clone(),
            self.request_id.clone(),
            format!("pi-warp-action-message-{}", Uuid::new_v4()),
            tool_call_id,
            tool,
        )]
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
            if let Some(events) = proxy_tool_call_to_warp(event, state) {
                return Ok(PiEventAction::Finish(events));
            }
            if should_proxy_tool_call_to_warp(event) {
                let tool_name = event
                    .get("toolName")
                    .and_then(Value::as_str)
                    .unwrap_or("tool");
                return Ok(PiEventAction::Error(format!(
                    "Warp could not proxy Pi tool \"{tool_name}\" because its arguments were missing or invalid."
                )));
            }

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

fn should_proxy_tool_call_to_warp(event: &Value) -> bool {
    if env_flag_is_enabled(PI_DISABLE_ACTION_PROXY_ENV)
        || env_flag_is_enabled("WARP_PI_ALLOW_UNBRIDGED_MUTATING_TOOLS")
    {
        return false;
    }

    matches!(
        event.get("toolName").and_then(Value::as_str),
        Some(
            "bash"
                | "edit"
                | "write"
                | "warp_mcp_call"
                | "warp_mcp_read_resource"
                | "warp_lrc_write"
                | "warp_lrc_read"
                | "warp_lrc_transfer"
        )
    )
}

fn proxy_tool_call_to_warp(
    event: &Value,
    state: &mut PiStreamState,
) -> Option<Vec<api::ResponseEvent>> {
    if !should_proxy_tool_call_to_warp(event) {
        return None;
    }

    let tool_call_id = event_string(event, "toolCallId")
        .unwrap_or_else(|| format!("pi-warp-action-{}", Uuid::new_v4()));
    let args = event.get("args");

    match event.get("toolName").and_then(Value::as_str)? {
        "bash" => {
            let command = args
                .and_then(|args| args.get("command"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .filter(|command| !command.trim().is_empty())?;
            Some(state.push_warp_bash_action(tool_call_id, command))
        }
        "edit" => {
            build_edit_tool_call(args).map(|tool| state.push_warp_tool_action(tool_call_id, tool))
        }
        "write" => build_write_tool_call(args, state.current_working_directory.as_deref())
            .map(|tool| state.push_warp_tool_action(tool_call_id, tool)),
        "warp_mcp_call" => build_mcp_call_tool_call(args)
            .map(|tool| state.push_warp_tool_action(tool_call_id, tool)),
        "warp_mcp_read_resource" => build_mcp_read_resource_tool_call(args)
            .map(|tool| state.push_warp_tool_action(tool_call_id, tool)),
        "warp_lrc_write" => build_lrc_write_tool_call(args)
            .map(|tool| state.push_warp_tool_action(tool_call_id, tool)),
        "warp_lrc_read" => build_lrc_read_tool_call(args)
            .map(|tool| state.push_warp_tool_action(tool_call_id, tool)),
        "warp_lrc_transfer" => build_lrc_transfer_tool_call(args)
            .map(|tool| state.push_warp_tool_action(tool_call_id, tool)),
        _ => None,
    }
}

fn build_edit_tool_call(args: Option<&Value>) -> Option<api::message::tool_call::Tool> {
    let args = args?;
    let file_path = path_arg(args)?;
    let mut diffs = Vec::new();

    if let Some(edits) = args.get("edits").and_then(Value::as_array) {
        for edit in edits {
            let search = string_arg(edit, &["oldText", "old_text", "search"])?;
            let replace = optional_string_arg(edit, &["newText", "new_text", "replace"])?;
            diffs.push(api::message::tool_call::apply_file_diffs::FileDiff {
                file_path: file_path.clone(),
                search,
                replace,
            });
        }
    } else {
        diffs.push(api::message::tool_call::apply_file_diffs::FileDiff {
            file_path: file_path.clone(),
            search: string_arg(args, &["oldText", "old_text", "search"])?,
            replace: optional_string_arg(args, &["newText", "new_text", "replace"])?,
        });
    }

    (!diffs.is_empty()).then(|| {
        api::message::tool_call::Tool::ApplyFileDiffs(api::message::tool_call::ApplyFileDiffs {
            summary: format!("Edit {file_path}"),
            diffs,
            new_files: vec![],
            deleted_files: vec![],
            v4a_updates: vec![],
        })
    })
}

fn build_write_tool_call(
    args: Option<&Value>,
    current_working_directory: Option<&Path>,
) -> Option<api::message::tool_call::Tool> {
    let args = args?;
    let file_path = path_arg(args)?;
    let content = optional_string_arg(args, &["content"])?;
    let existing_content = resolve_existing_text_file(args, &file_path, current_working_directory);

    let (diffs, new_files) = if let Some(existing_content) = existing_content {
        (
            vec![api::message::tool_call::apply_file_diffs::FileDiff {
                file_path: file_path.clone(),
                search: existing_content,
                replace: content,
            }],
            vec![],
        )
    } else {
        (
            vec![],
            vec![api::message::tool_call::apply_file_diffs::NewFile {
                file_path: file_path.clone(),
                content,
            }],
        )
    };

    Some(api::message::tool_call::Tool::ApplyFileDiffs(
        api::message::tool_call::ApplyFileDiffs {
            summary: format!("Write {file_path}"),
            diffs,
            new_files,
            deleted_files: vec![],
            v4a_updates: vec![],
        },
    ))
}

fn build_mcp_call_tool_call(args: Option<&Value>) -> Option<api::message::tool_call::Tool> {
    let args = args?;
    let tool_args = args
        .get("args")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let prost_args = serde_json_to_prost_struct(tool_args).ok()?;

    Some(api::message::tool_call::Tool::CallMcpTool(
        api::message::tool_call::CallMcpTool {
            name: string_arg(args, &["name"])?,
            args: Some(prost_args),
            server_id: optional_string_arg(args, &["serverId", "server_id"]).unwrap_or_default(),
        },
    ))
}

fn build_mcp_read_resource_tool_call(
    args: Option<&Value>,
) -> Option<api::message::tool_call::Tool> {
    let args = args?;
    Some(api::message::tool_call::Tool::ReadMcpResource(
        api::message::tool_call::ReadMcpResource {
            uri: string_arg(args, &["uri"])?,
            server_id: optional_string_arg(args, &["serverId", "server_id"]).unwrap_or_default(),
        },
    ))
}

fn build_lrc_write_tool_call(args: Option<&Value>) -> Option<api::message::tool_call::Tool> {
    let args = args?;
    let mode = match optional_string_arg(args, &["mode"]).as_deref() {
        Some("line") => {
            api::message::tool_call::write_to_long_running_shell_command::mode::Mode::Line(())
        }
        Some("block") => {
            api::message::tool_call::write_to_long_running_shell_command::mode::Mode::Block(())
        }
        _ => api::message::tool_call::write_to_long_running_shell_command::mode::Mode::Raw(()),
    };

    Some(
        api::message::tool_call::Tool::WriteToLongRunningShellCommand(
            api::message::tool_call::WriteToLongRunningShellCommand {
                command_id: string_arg(args, &["commandId", "command_id"])?,
                input: optional_string_arg(args, &["input"])?.into_bytes(),
                mode: Some(
                    api::message::tool_call::write_to_long_running_shell_command::Mode {
                        mode: Some(mode),
                    },
                ),
            },
        ),
    )
}

fn build_lrc_read_tool_call(args: Option<&Value>) -> Option<api::message::tool_call::Tool> {
    let args = args?;
    let delay = if args
        .get("waitUntilComplete")
        .or_else(|| args.get("wait_until_complete"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Some(api::message::tool_call::read_shell_command_output::Delay::OnCompletion(()))
    } else {
        args.get("delaySeconds")
            .or_else(|| args.get("delay_seconds"))
            .and_then(Value::as_i64)
            .map(|seconds| {
                api::message::tool_call::read_shell_command_output::Delay::Duration(
                    prost_types::Duration { seconds, nanos: 0 },
                )
            })
    };

    Some(api::message::tool_call::Tool::ReadShellCommandOutput(
        api::message::tool_call::ReadShellCommandOutput {
            command_id: string_arg(args, &["commandId", "command_id"])?,
            delay,
        },
    ))
}

fn build_lrc_transfer_tool_call(args: Option<&Value>) -> Option<api::message::tool_call::Tool> {
    let args = args?;
    Some(
        api::message::tool_call::Tool::TransferShellCommandControlToUser(
            api::message::tool_call::TransferShellCommandControlToUser {
                reason: string_arg(args, &["reason"])?,
            },
        ),
    )
}

fn path_arg(args: &Value) -> Option<String> {
    string_arg(args, &["path", "file_path"])
}

fn string_arg(args: &Value, names: &[&str]) -> Option<String> {
    optional_string_arg(args, names).filter(|value| !value.trim().is_empty())
}

fn optional_string_arg(args: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| args.get(*name).and_then(Value::as_str))
        .map(str::to_owned)
}

fn resolve_existing_text_file(
    args: &Value,
    file_path: &str,
    current_working_directory: Option<&Path>,
) -> Option<String> {
    let path = PathBuf::from(file_path);
    let path = if path.is_absolute() {
        path
    } else {
        current_working_directory
            .map(Path::to_path_buf)
            .or_else(|| std::env::current_dir().ok())?
            .join(path)
    };

    read_existing_text_file(&path).or_else(|| {
        optional_string_arg(
            args,
            &[
                "previousContent",
                "previous_content",
                "oldContent",
                "old_content",
            ],
        )
    })
}

fn read_existing_text_file(path: &Path) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(content) => Some(content),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => None,
    }
}

fn serde_json_to_prost_struct(value: Value) -> Result<prost_types::Struct, String> {
    match serde_json_to_prost(value)? {
        prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(value)),
        } => Ok(value),
        _ => Err("MCP tool args must be a JSON object".to_string()),
    }
}

fn serde_json_to_prost(value: Value) -> Result<prost_types::Value, String> {
    use prost_types::value::Kind::*;
    use serde_json::Value::*;

    Ok(prost_types::Value {
        kind: Some(match value {
            Null => NullValue(0),
            Bool(v) => BoolValue(v),
            Number(n) => NumberValue(
                n.as_f64()
                    .ok_or_else(|| format!("float {n} is not valid JSON number"))?,
            ),
            String(s) => StringValue(s),
            Array(a) => ListValue(prost_types::ListValue {
                values: a
                    .into_iter()
                    .map(serde_json_to_prost)
                    .collect::<Result<Vec<_>, std::string::String>>()?,
            }),
            Object(v) => StructValue(prost_types::Struct {
                fields: v
                    .into_iter()
                    .map(|(k, v)| serde_json_to_prost(v).map(|v| (k, v)))
                    .collect::<Result<BTreeMap<_, _>, std::string::String>>()?,
            }),
        }),
    })
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

fn add_tool_call_event(
    task_id: String,
    request_id: String,
    message_id: String,
    tool_call_id: String,
    tool: api::message::tool_call::Tool,
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
                                message: Some(api::message::Message::ToolCall(
                                    api::message::ToolCall {
                                        tool_call_id,
                                        tool: Some(tool),
                                    },
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

#[cfg(test)]
fn add_bash_tool_call_event(
    task_id: String,
    request_id: String,
    message_id: String,
    tool_call_id: String,
    command: String,
) -> api::ResponseEvent {
    add_tool_call_event(
        task_id,
        request_id,
        message_id,
        tool_call_id,
        api::message::tool_call::Tool::RunShellCommand(
            api::message::tool_call::RunShellCommand {
                command,
                is_read_only: false,
                uses_pager: false,
                citations: vec![],
                is_risky: true,
                risk_category: api::RiskCategory::Risky as i32,
                wait_until_complete_value: Some(
                    api::message::tool_call::run_shell_command::WaitUntilCompleteValue::WaitUntilComplete(true),
                ),
            },
        ),
    )
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
        PiEventAction, PiStreamState, add_bash_tool_call_event, append_agent_output_event,
        handle_extension_ui_request, handle_pi_event, is_pi_local_conversation_token,
        plain_prompt_from_input, prompt_from_input, sanitize_session_file_stem,
        validate_prompt_input,
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

    fn event_tool_call(event: &api::ResponseEvent) -> Option<&api::message::ToolCall> {
        let api::response_event::Type::ClientActions(actions) = event.r#type.as_ref()? else {
            return None;
        };
        let action = actions.actions.first()?.action.as_ref()?;
        let api::client_action::Action::AddMessagesToTask(add) = action else {
            return None;
        };
        let message = add.messages.first()?;
        let api::message::Message::ToolCall(tool_call) = message.message.as_ref()? else {
            return None;
        };
        Some(tool_call)
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
    fn pi_bash_tool_start_is_proxied_to_native_warp_action() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_bash",
                "toolName": "bash",
                "args": { "command": "cargo test" }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Finish(events) = action else {
            panic!("expected stream finish for proxied bash");
        };

        assert!(state.has_stream_output());
        let tool_call = event_tool_call(&events[0]).unwrap();
        assert_eq!(tool_call.tool_call_id, "call_bash");
        let Some(api::message::tool_call::Tool::RunShellCommand(command)) = tool_call.tool.as_ref()
        else {
            panic!("expected RunShellCommand tool call");
        };
        assert_eq!(command.command, "cargo test");
        assert!(command.is_risky);
        assert_eq!(command.risk_category, api::RiskCategory::Risky as i32);
    }

    #[test]
    fn pi_edit_tool_start_is_proxied_to_native_file_diff_action() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_edit",
                "toolName": "edit",
                "args": {
                    "path": "src/main.rs",
                    "edits": [
                        { "oldText": "fn main() {}", "newText": "fn main() { println!(\"hi\"); }" }
                    ]
                }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Finish(events) = action else {
            panic!("expected stream finish for proxied edit");
        };

        let tool_call = event_tool_call(&events[0]).unwrap();
        assert_eq!(tool_call.tool_call_id, "call_edit");
        let Some(api::message::tool_call::Tool::ApplyFileDiffs(diff)) = tool_call.tool.as_ref()
        else {
            panic!("expected ApplyFileDiffs tool call");
        };
        assert_eq!(diff.summary, "Edit src/main.rs");
        assert_eq!(diff.diffs.len(), 1);
        assert_eq!(diff.diffs[0].file_path, "src/main.rs");
        assert_eq!(diff.diffs[0].search, "fn main() {}");
        assert_eq!(diff.diffs[0].replace, "fn main() { println!(\"hi\"); }");
    }

    #[test]
    fn pi_edit_tool_allows_empty_replacement_for_deletions() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_edit_delete",
                "toolName": "edit",
                "args": {
                    "path": "src/main.rs",
                    "oldText": "println!(\"remove me\");",
                    "newText": ""
                }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Finish(events) = action else {
            panic!("expected stream finish for proxied edit deletion");
        };

        let tool_call = event_tool_call(&events[0]).unwrap();
        let Some(api::message::tool_call::Tool::ApplyFileDiffs(diff)) = tool_call.tool.as_ref()
        else {
            panic!("expected ApplyFileDiffs tool call");
        };
        assert_eq!(diff.diffs[0].search, "println!(\"remove me\");");
        assert_eq!(diff.diffs[0].replace, "");
    }

    #[test]
    fn pi_write_tool_start_is_proxied_to_native_file_creation_action() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_write",
                    "toolName": "write",
                    "args": {
                    "path": "__warp_pi_local_test_new_file__.txt",
                    "content": "new contents"
                }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Finish(events) = action else {
            panic!("expected stream finish for proxied write");
        };

        let tool_call = event_tool_call(&events[0]).unwrap();
        let Some(api::message::tool_call::Tool::ApplyFileDiffs(diff)) = tool_call.tool.as_ref()
        else {
            panic!("expected ApplyFileDiffs tool call");
        };
        assert_eq!(diff.summary, "Write __warp_pi_local_test_new_file__.txt");
        assert_eq!(diff.new_files.len(), 1);
        assert_eq!(
            diff.new_files[0].file_path,
            "__warp_pi_local_test_new_file__.txt"
        );
        assert_eq!(diff.new_files[0].content, "new contents");
    }

    #[test]
    fn pi_write_tool_allows_empty_file_content() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_write_empty",
                "toolName": "write",
                "args": {
                    "path": "__warp_pi_local_test_empty_file__.txt",
                    "content": ""
                }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Finish(events) = action else {
            panic!("expected stream finish for proxied empty write");
        };

        let tool_call = event_tool_call(&events[0]).unwrap();
        let Some(api::message::tool_call::Tool::ApplyFileDiffs(diff)) = tool_call.tool.as_ref()
        else {
            panic!("expected ApplyFileDiffs tool call");
        };
        assert_eq!(diff.new_files.len(), 1);
        assert_eq!(
            diff.new_files[0].file_path,
            "__warp_pi_local_test_empty_file__.txt"
        );
        assert_eq!(diff.new_files[0].content, "");
    }

    #[test]
    fn invalid_proxied_write_is_blocked_instead_of_falling_through() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_write_invalid",
                "toolName": "write",
                "args": {
                    "path": "__warp_pi_local_test_missing_content__.txt"
                }
            }),
            &mut state,
        )
        .unwrap();

        let PiEventAction::Error(message) = action else {
            panic!("expected invalid proxied write to be blocked");
        };
        assert!(message.contains("could not proxy Pi tool \"write\""));
    }

    #[test]
    fn pi_mcp_tools_are_proxied_to_native_warp_actions() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_mcp",
                "toolName": "warp_mcp_call",
                "args": {
                    "name": "github_search",
                    "serverId": "server-1",
                    "args": { "query": "warp" }
                }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Finish(events) = action else {
            panic!("expected stream finish for proxied mcp call");
        };

        let tool_call = event_tool_call(&events[0]).unwrap();
        let Some(api::message::tool_call::Tool::CallMcpTool(mcp)) = tool_call.tool.as_ref() else {
            panic!("expected CallMcpTool tool call");
        };
        assert_eq!(mcp.name, "github_search");
        assert_eq!(mcp.server_id, "server-1");
        let query = mcp
            .args
            .as_ref()
            .and_then(|args| args.fields.get("query"))
            .and_then(|value| value.kind.as_ref());
        assert!(matches!(
            query,
            Some(prost_types::value::Kind::StringValue(value)) if value == "warp"
        ));

        let mut state = PiStreamState::new("task".to_string(), "request".to_string());
        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_resource",
                "toolName": "warp_mcp_read_resource",
                "args": {
                    "uri": "mcp://resource",
                    "serverId": "server-1"
                }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Finish(events) = action else {
            panic!("expected stream finish for proxied mcp resource");
        };
        let tool_call = event_tool_call(&events[0]).unwrap();
        let Some(api::message::tool_call::Tool::ReadMcpResource(resource)) =
            tool_call.tool.as_ref()
        else {
            panic!("expected ReadMcpResource tool call");
        };
        assert_eq!(resource.uri, "mcp://resource");
        assert_eq!(resource.server_id, "server-1");
    }

    #[test]
    fn pi_long_running_command_tools_are_proxied_to_native_warp_actions() {
        let mut state = PiStreamState::new("task".to_string(), "request".to_string());

        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_lrc_write",
                "toolName": "warp_lrc_write",
                "args": {
                    "commandId": "block-1",
                    "input": "status",
                    "mode": "line"
                }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Finish(events) = action else {
            panic!("expected stream finish for proxied lrc write");
        };
        let tool_call = event_tool_call(&events[0]).unwrap();
        let Some(api::message::tool_call::Tool::WriteToLongRunningShellCommand(write)) =
            tool_call.tool.as_ref()
        else {
            panic!("expected WriteToLongRunningShellCommand tool call");
        };
        assert_eq!(write.command_id, "block-1");
        assert_eq!(write.input, b"status");
        assert!(matches!(
            write.mode.as_ref().and_then(|mode| mode.mode.as_ref()),
            Some(
                api::message::tool_call::write_to_long_running_shell_command::mode::Mode::Line(())
            )
        ));

        let mut state = PiStreamState::new("task".to_string(), "request".to_string());
        let action = handle_pi_event(
            &json!({
                "type": "tool_execution_start",
                "toolCallId": "call_lrc_read",
                "toolName": "warp_lrc_read",
                "args": {
                    "commandId": "block-1",
                    "waitUntilComplete": true
                }
            }),
            &mut state,
        )
        .unwrap();
        let PiEventAction::Finish(events) = action else {
            panic!("expected stream finish for proxied lrc read");
        };
        let tool_call = event_tool_call(&events[0]).unwrap();
        let Some(api::message::tool_call::Tool::ReadShellCommandOutput(read)) =
            tool_call.tool.as_ref()
        else {
            panic!("expected ReadShellCommandOutput tool call");
        };
        assert_eq!(read.command_id, "block-1");
        assert!(matches!(
            read.delay.as_ref(),
            Some(api::message::tool_call::read_shell_command_output::Delay::OnCompletion(()))
        ));
    }

    #[test]
    fn add_bash_tool_call_event_uses_warp_tool_call_message() {
        let event = add_bash_tool_call_event(
            "task".to_string(),
            "request".to_string(),
            "message".to_string(),
            "call".to_string(),
            "echo hi".to_string(),
        );

        let tool_call = event_tool_call(&event).unwrap();
        let Some(api::message::tool_call::Tool::RunShellCommand(command)) = tool_call.tool.as_ref()
        else {
            panic!("expected RunShellCommand tool call");
        };
        assert_eq!(tool_call.tool_call_id, "call");
        assert_eq!(command.command, "echo hi");
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
        assert!(
            event_agent_output_text(&events[0])
                .unwrap()
                .contains("extension_ui.confirm")
        );
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
        assert!(
            event_agent_output_text(&events[0])
                .unwrap()
                .contains("extension_ui.notify")
        );
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
        assert!(
            event_agent_output_text(&tool_events[0])
                .unwrap()
                .contains("failed grep")
        );
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
