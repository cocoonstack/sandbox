"""Hardware e2e for sandbox-mcp: speaks real MCP (JSON-RPC 2.0 over stdio)
to the built binary against a live node — the exact protocol an MCP client
uses. Prints MCP-E2E PASS.

    python3 e2e.py --bin ./sandbox-mcp --addr 127.0.0.1:7777 --token e2e --template rt2:24.04
"""

import argparse
import json
import subprocess
import sys


class McpClient:
    def __init__(self, argv):
        self.proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        self.seq = 0

    def call(self, method, params=None):
        self.seq += 1
        req = {"jsonrpc": "2.0", "id": self.seq, "method": method}
        if params is not None:
            req["params"] = params
        self.proc.stdin.write((json.dumps(req) + "\n").encode())
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        resp = json.loads(line)
        assert resp["id"] == self.seq, resp
        assert "error" not in resp, resp
        return resp["result"]

    def tool(self, tool_name, **arguments):
        result = self.call("tools/call", {"name": tool_name, "arguments": arguments})
        text = result["content"][0]["text"]
        if result.get("isError"):
            raise RuntimeError(f"{tool_name}: {text}")
        try:
            return json.loads(text)
        except ValueError:
            return text

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", required=True)
    parser.add_argument("--addr", default="127.0.0.1:7777")
    parser.add_argument("--token", default="")
    parser.add_argument("--template", default="rt:24.04")
    args = parser.parse_args()

    mcp = McpClient([args.bin, "-addr", args.addr, "-token", args.token, "-template", args.template])
    try:
        init = mcp.call("initialize", {"protocolVersion": "2024-11-05", "capabilities": {}})
        assert init["serverInfo"]["name"] == "sandbox-mcp", init
        tools = {t["name"] for t in mcp.call("tools/list")["tools"]}
        assert {"create_sandbox", "exec", "checkpoint", "branch_checkpoint"} <= tools, tools
        print(f"  initialize + tools/list ok ({len(tools)} tools)")

        sandbox_id = mcp.tool("create_sandbox")["sandbox_id"]
        out = mcp.tool("exec", sandbox_id=sandbox_id, command="echo mcp-hello")
        assert out["exit_code"] == 0 and out["stdout"] == "mcp-hello\n", out
        print("  create + exec ok")

        mcp.tool("write_file", sandbox_id=sandbox_id, path="/root/m.txt", content="v1")
        assert mcp.tool("read_file", sandbox_id=sandbox_id, path="/root/m.txt") == "v1"
        names = {e["name"] for e in mcp.tool("list_dir", sandbox_id=sandbox_id, path="/root")}
        assert "m.txt" in names, names
        print("  files ok")

        ckpt = mcp.tool("checkpoint", sandbox_id=sandbox_id, name="mcp-step")["checkpoint_id"]
        mcp.tool("write_file", sandbox_id=sandbox_id, path="/root/m.txt", content="v2")
        branch = mcp.tool("branch_checkpoint", checkpoint_id=ckpt)["sandbox_id"]
        assert mcp.tool("read_file", sandbox_id=branch, path="/root/m.txt") == "v1"
        assert mcp.tool("read_file", sandbox_id=sandbox_id, path="/root/m.txt") == "v2"
        listed = {c["checkpoint_id"] for c in mcp.tool("list_checkpoints")}
        assert ckpt in listed, listed
        print("  checkpoint tree ok")

        mcp.tool("hibernate", sandbox_id=sandbox_id)
        out = mcp.tool("exec", sandbox_id=sandbox_id, command="cat /root/m.txt")
        assert out["stdout"] == "v2", out  # transparent wake
        print("  hibernate + transparent wake ok")

        forks = mcp.tool("fork", sandbox_id=sandbox_id, count=2)["sandbox_ids"]
        assert len(forks) == 2, forks
        for child in forks:
            mcp.tool("release", sandbox_id=child)
        print("  fork ok")

        mcp.tool("delete_checkpoint", checkpoint_id=ckpt)
        mcp.tool("release", sandbox_id=branch)
        mcp.tool("release", sandbox_id=sandbox_id)
        info = mcp.tool("node_info")
        assert "pools" in info, info
        print("  cleanup + node_info ok")
    finally:
        mcp.close()
    print("MCP-E2E PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
