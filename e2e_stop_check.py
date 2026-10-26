#!/usr/bin/env python3
"""End-to-end stop drill against the real (untagged) sidecar binary.

Scenario: connection/connect to a silent TCP acceptor (proxy that accepts but
never answers the dump handshake) -> job/start hangs in connecting ->
job/stop must produce a final my2sql/job/done with status "stopped".
"""
import json, os, queue, socket, subprocess, sys, tempfile, threading, time

ROOT = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.join(ROOT, "backend")
BIN = os.path.join(tempfile.gettempdir(), "my2sql-sidecar-e2e")

def build():
    r = subprocess.run(["go", "build", "-tags", "my2sql", "-o", BIN, "."], cwd=BACKEND)
    if r.returncode != 0:
        sys.exit("build failed")

def silent_acceptor():
    srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0)); srv.listen(8)
    port = srv.getsockname()[1]
    def loop():
        while True:
            try:
                c, _ = srv.accept()  # accept and never reply — hung-proxy mock
            except OSError:
                return
    threading.Thread(target=loop, daemon=True).start()
    return port

def main():
    build()
    port = silent_acceptor()
    env = dict(os.environ, GOFLAGS="", DBX_PLUGIN_DATA_DIR=tempfile.mkdtemp(prefix="dbx-e2e-"))
    p = subprocess.Popen([BIN], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, env=env, text=True, bufsize=1)
    out = queue.Queue()
    def reader():
        for line in p.stdout:
            out.put(line.strip())
    threading.Thread(target=reader, daemon=True).start()
    seq = [0]
    def recv(timeout):
        try:
            return json.loads(out.get(timeout=timeout))
        except queue.Empty:
            return None
    pending = []  # events that arrived while waiting for an rpc ack
    def call(method, params, timeout=10):
        seq[0] += 1; mid = str(seq[0])
        p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": mid, "method": method, "params": params}) + "\n")
        p.stdin.flush()
        for ev in pending:
            if ev.get("id") == mid:
                pending.remove(ev); return ev
        t0 = time.time()
        while time.time() - t0 < timeout:
            ev = recv(timeout - (time.time() - t0))
            if ev is None:
                break
            if ev.get("id") == mid:
                return ev
            pending.append(ev)
        raise AssertionError(f"no reply to {method} within {timeout}s")
    def next_event(method, timeout):
        for ev in pending:
            if ev.get("method") == method:
                pending.remove(ev); return ev
        t0 = time.time()
        while time.time() - t0 < timeout:
            ev = recv(timeout - (time.time() - t0))
            if ev is None:
                return None
            if ev.get("method") == method:
                return ev
            if ev.get("id") is not None:
                pending.append(ev)
        return None

    ack = call("plugin/initialize", {"host": {"protocolVersions": [1]}}, 5)
    assert "error" not in ack, ack
    print("[1] initialize OK")

    ack = call("connection/connect", {"connection": {
        "id": "e2e-src", "name": "e2e", "host": "127.0.0.1", "port": port,
        "username": "repl", "password": "x",
        "external_config": {"server_id": "9999", "timezone": "+08:00"}}}, 5)
    assert "error" not in ack, ack
    print("[2] connect session registered OK")

    ack = call("job/start", {"sourceConnectionId": "e2e-src", "mode": "repl", "workType": "rollback"})
    assert "error" not in ack, ack
    job_id = ack["result"]["jobId"]
    print(f"[3] job/start OK jobId={job_id}")

    # The manager must report the job as soon as it starts (no silent stall),
    # and the engine must reach the connection phase once its probes finish.
    ev = next_event("my2sql/job/progress", 30)
    stage = (ev or {}).get("params", {}).get("stage")
    assert stage in ("starting", "initiating", "connecting", "parsing"), f"first progress stage={stage!r}"
    if stage != "connecting":
        ev = next_event("my2sql/job/progress", 60)
        stage = (ev or {}).get("params", {}).get("stage") if ev else stage
        assert stage == "connecting", f"expected connecting stage, got {stage!r}"
    print("[4] engine reached 'connecting' (dump handshake in flight)")

    ack = call("job/stop", {"jobId": job_id})
    assert "error" not in ack, ack
    print("[5] job/stop RPC OK")

    done = next_event("my2sql/job/done", 10)
    assert done, "no my2sql/job/done within 10s — stop did NOT terminate the job"
    st = done["params"].get("status")
    print(f"[6] terminal job/done received: status={st!r} error={done['params'].get('error')!r}")
    assert st == "stopped", f"status={st} want stopped"

    ack = call("job/stop", {"jobId": job_id})
    assert "error" not in ack, ack
    print("[7] repeated stop tolerated (idempotent)")

    p.stdin.close(); p.terminate()
    print("\nSTOP_E2E_PASS")

if __name__ == "__main__":
    main()
