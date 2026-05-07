import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { Type } from "typebox";

const READ_ONLY_TOOLS = new Set(["read", "grep", "find", "ls"]);
const WARP_ACTION_PROXY_TOOLS = new Set([
	"bash",
	"edit",
	"write",
	"warp_mcp_call",
	"warp_mcp_read_resource",
	"warp_lrc_write",
	"warp_lrc_read",
	"warp_lrc_transfer",
]);
const MUTATING_TOOLS = new Set(["bash", "edit", "write", "warp_mcp_call", "warp_lrc_write"]);

function isEnabled(value: string | undefined): boolean {
	return value === "1" || value === "true" || value === "TRUE" || value === "yes" || value === "YES";
}

export default function warpToolGate(pi: ExtensionAPI) {
	pi.registerTool({
		name: "warp_mcp_call",
		label: "Warp MCP tool",
		description: "Call an MCP tool through Warp's native MCP action bridge. Use the exact tool name from Warp context.",
		promptSnippet: "Call a Warp MCP tool by name with JSON object arguments",
		parameters: Type.Object({
			name: Type.String({ description: "MCP tool name" }),
			serverId: Type.Optional(Type.String({ description: "Optional Warp MCP server id" })),
			args: Type.Optional(Type.Any({ description: "JSON object arguments for the MCP tool" })),
		}),
		async execute() {
			return {
				content: [{ type: "text", text: "Warp should intercept warp_mcp_call before Pi executes it." }],
				isError: true,
			};
		},
	});

	pi.registerTool({
		name: "warp_mcp_read_resource",
		label: "Warp MCP resource",
		description: "Read an MCP resource through Warp's native MCP resource bridge. Use the exact URI from Warp context.",
		promptSnippet: "Read a Warp MCP resource by URI",
		parameters: Type.Object({
			uri: Type.String({ description: "MCP resource URI" }),
			serverId: Type.Optional(Type.String({ description: "Optional Warp MCP server id" })),
		}),
		async execute() {
			return {
				content: [{ type: "text", text: "Warp should intercept warp_mcp_read_resource before Pi executes it." }],
				isError: true,
			};
		},
	});

	pi.registerTool({
		name: "warp_lrc_write",
		label: "Write to command",
		description: "Write input to a long-running shell command through Warp's native terminal bridge.",
		promptSnippet: "Write input to a running Warp shell command",
		parameters: Type.Object({
			commandId: Type.String({ description: "Warp command id from a previous long-running command snapshot" }),
			input: Type.String({ description: "Input text to write to the running command" }),
			mode: Type.Optional(Type.Union([
				Type.Literal("raw"),
				Type.Literal("line"),
				Type.Literal("block"),
			], { description: "How Warp should write the input" })),
		}),
		async execute() {
			return {
				content: [{ type: "text", text: "Warp should intercept warp_lrc_write before Pi executes it." }],
				isError: true,
			};
		},
	});

	pi.registerTool({
		name: "warp_lrc_read",
		label: "Read command output",
		description: "Read output from a long-running shell command through Warp's native terminal bridge.",
		promptSnippet: "Read output from a running Warp shell command",
		parameters: Type.Object({
			commandId: Type.String({ description: "Warp command id from a previous long-running command snapshot" }),
			delaySeconds: Type.Optional(Type.Number({ description: "Optional delay before reading output" })),
			waitUntilComplete: Type.Optional(Type.Boolean({ description: "Wait until the command completes before returning output" })),
		}),
		async execute() {
			return {
				content: [{ type: "text", text: "Warp should intercept warp_lrc_read before Pi executes it." }],
				isError: true,
			};
		},
	});

	pi.registerTool({
		name: "warp_lrc_transfer",
		label: "Transfer command",
		description: "Transfer control of a running shell command back to the user through Warp.",
		promptSnippet: "Transfer a running Warp shell command back to the user",
		parameters: Type.Object({
			reason: Type.String({ description: "Why the user should take control" }),
		}),
		async execute() {
			return {
				content: [{ type: "text", text: "Warp should intercept warp_lrc_transfer before Pi executes it." }],
				isError: true,
			};
		},
	});

	pi.on("tool_call", async (event) => {
		if (READ_ONLY_TOOLS.has(event.toolName)) {
			return;
		}

		if (WARP_ACTION_PROXY_TOOLS.has(event.toolName) && !isEnabled(process.env.WARP_PI_DISABLE_ACTION_PROXY)) {
			return;
		}

		if (MUTATING_TOOLS.has(event.toolName) && isEnabled(process.env.WARP_PI_ALLOW_UNBRIDGED_MUTATING_TOOLS)) {
			return;
		}

		return {
			block: true,
			reason:
				`Warp OSS blocked Pi tool "${event.toolName}". ` +
				"Read-only tools are enabled locally; mutating tools require Warp's action approval bridge.",
		};
	});
}
