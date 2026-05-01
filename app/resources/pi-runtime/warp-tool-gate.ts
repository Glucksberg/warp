import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";

const READ_ONLY_TOOLS = new Set(["read", "grep", "find", "ls"]);
const MUTATING_TOOLS = new Set(["bash", "edit", "write"]);

function isEnabled(value: string | undefined): boolean {
	return value === "1" || value === "true" || value === "TRUE" || value === "yes" || value === "YES";
}

export default function warpToolGate(pi: ExtensionAPI) {
	pi.on("tool_call", async (event) => {
		if (READ_ONLY_TOOLS.has(event.toolName)) {
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
