"""Hardware e2e for the Python SDK: drives the full surface against a real
sandboxd node. Mirrors the Go smoke's step discipline; prints PY-E2E PASS.

    python3 e2e.py --addr 127.0.0.1:7777 --token e2e --template rt2:24.04
"""

from __future__ import annotations

import argparse
import io
import sys
import tarfile
import time

from cocoonsandbox import Client, ExitError, SilkdError


def tar_bytes(name: str, content: bytes) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tw:
        info = tarfile.TarInfo(name)
        info.size = len(content)
        tw.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:7777")
    parser.add_argument("--token", default="")
    parser.add_argument("--template", default="rt:24.04")
    args = parser.parse_args()

    client = Client(args.addr, api_token=args.token)
    sb = client.new(args.template, net="none")
    steps = []

    def step(name):
        def wrap(fn):
            steps.append((name, fn))
            return fn
        return wrap

    @step("exec")
    def _exec():
        assert sb.exec("echo", "hello") == "hello\n"
        try:
            sb.exec("sh", "-c", "echo boom >&2; exit 3")
            raise AssertionError("nonzero exit did not raise")
        except ExitError as exc:
            assert exc.code == 3 and "boom" in exc.stderr

    @step("files")
    def _files():
        sb.write_file("/root/py.txt", b"from-python")
        assert sb.read_file("/root/py.txt") == b"from-python"
        sb.mkdir("/root/pydir/sub", parents=True)
        names = {e["name"] for e in sb.list_dir("/root/pydir")}
        assert "sub" in names, names
        st = sb.stat("/root/py.txt")
        assert st["size"] == len(b"from-python"), st
        sb.rename("/root/py.txt", "/root/py2.txt")
        sb.remove("/root/pydir", recursive=True)

    @step("session")
    def _session():
        sess = sb.session(cwd="/root")
        try:
            sess.exec("sh", "-c", "MARK=py; export MARK")
            assert sess.exec("sh", "-c", "echo $MARK") == "py\n"
        finally:
            sess.close()

    @step("find-replace")
    def _find():
        sb.write_file("/root/fr.txt", b"alpha beta alpha")
        matches = sb.find("/root", "alpha", glob="fr.txt")
        assert matches, "no matches"
        replaced = sb.replace(["/root/fr.txt"], "alpha", "gamma")
        assert replaced and b"gamma" in sb.read_file("/root/fr.txt")

    @step("tree")
    def _tree():
        sb.push("/root/tree", tar_bytes("t.txt", b"tar-marker"))
        assert sb.read_file("/root/tree/t.txt") == b"tar-marker"
        pulled = sb.pull("/root/tree")
        with tarfile.open(fileobj=io.BytesIO(pulled)) as tr:
            member = tr.extractfile("tree/t.txt")
            assert member and member.read() == b"tar-marker"

    @step("git-typed-error")
    def _git():
        try:
            sb.git_clone("https://example.com/x.git", "/root/x")
            raise AssertionError("clone on the none lane did not raise")
        except SilkdError as exc:
            assert exc.kind == "unimplemented", exc

    @step("hibernate-wake")
    def _hibernate():
        sess = sb.session()
        sess.exec("sh", "-c", "HIB=alive; export HIB")
        sb.hibernate()
        assert sess.exec("sh", "-c", "echo $HIB") == "alive\n"  # transparent wake
        sess.close()

    @step("fork")
    def _fork():
        sb.write_file("/root/fk.txt", b"parent")
        children = sb.fork(2)
        try:
            assert len(children) == 2
            for child in children:
                assert child.read_file("/root/fk.txt") == b"parent"
            children[0].write_file("/root/fk.txt", b"child0")
            assert sb.read_file("/root/fk.txt") == b"parent"
        finally:
            for child in children:
                child.close()

    @step("checkpoint-tree")
    def _checkpoint():
        sb.write_file("/root/ck.txt", b"v1")
        ckpt = sb.checkpoint("py-step")
        sb.write_file("/root/ck.txt", b"v2")
        branch = ckpt.new()
        try:
            assert branch.read_file("/root/ck.txt") == b"v1"  # captured moment
            assert sb.read_file("/root/ck.txt") == b"v2"      # source unaffected
        finally:
            branch.close()
        listed = [c.id for c in client.checkpoints()]
        assert ckpt.id in listed, listed
        ckpt.delete()

    @step("promote-template")
    def _promote():
        sb.write_file("/root/tpl.txt", b"tpl-marker")
        tpl = sb.promote("py-tpl")
        clone = tpl.new()
        try:
            assert clone.read_file("/root/tpl.txt") == b"tpl-marker"
        finally:
            clone.close()
        tpl.delete()

    @step("port")
    def _port():
        deadline = time.time() + 5
        while True:
            try:
                conn = sb.dial_port(22)
                banner = conn.recv()
                conn.close()
                assert banner.startswith(b"SSH-"), banner
                return
            except SilkdError:
                if time.time() > deadline:
                    raise
                time.sleep(0.3)

    try:
        for name, fn in steps:
            start = time.time()
            fn()
            print(f"  {name:<18} ok ({int((time.time() - start) * 1000)}ms)")
    finally:
        sb.close()
    print("PY-E2E PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
