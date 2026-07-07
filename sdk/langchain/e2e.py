"""Hardware e2e for the LangChain toolkit against a real node:
python3 e2e.py --addr 10.0.0.5:7777 --token ... --template rt:24.04
Prints LANGCHAIN-ADAPTER-E2E PASS on success."""

import argparse
import sys

from cocoonsandbox_langchain import CocoonToolkit


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", default="127.0.0.1:7777")
    ap.add_argument("--token", default="")
    ap.add_argument("--template", default="rt:24.04")
    args = ap.parse_args()

    with CocoonToolkit(args.addr, api_token=args.token, template=args.template) as kit:
        exec_tool, write, read, list_dir = kit.get_tools()
        print(exec_tool.invoke({"command": "echo toolkit-mark"}))
        write.invoke({"path": "/work/lc.txt", "content": "via-langchain"})
        assert read.invoke({"path": "/work/lc.txt"}) == "via-langchain"
        assert "lc.txt" in list_dir.invoke({"path": "/work"})
        out = exec_tool.invoke({"command": "cat /work/lc.txt"})
        assert "via-langchain" in out, out
    print("LANGCHAIN-ADAPTER-E2E PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
