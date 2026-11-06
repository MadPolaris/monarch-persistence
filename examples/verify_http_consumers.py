#!/usr/bin/env python3
"""Local R2 acceptance: staged public-JAR consumers plus an independent ledger JVM.

Build/stage consumers separately. This script owns only new temporary directories
and process groups it starts. It never stops an existing service or deletes data.
No browser automation is claimed: HTML/JavaScript and real HTTP contracts are tested.
"""
import argparse
import base64
import hashlib
import http.cookiejar
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def free_port():
    with socket.socket() as connection:
        connection.bind(("127.0.0.1", 0))
        return connection.getsockname()[1]


def exact(connection, count):
    parts = []
    while count:
        part = connection.recv(count)
        require(part, "truncated external fixture frame")
        parts.append(part)
        count -= len(part)
    return b"".join(parts)


def ledger(port, command, *fields):
    values = [value.encode("utf-8") for value in (command,) + fields]
    body = struct.pack(">I", len(values)) + b"".join(struct.pack(">I", len(value)) + value for value in values)
    with socket.create_connection(("127.0.0.1", port), 3) as connection:
        connection.settimeout(3)
        connection.sendall(struct.pack(">I", len(body)) + body)
        length, = struct.unpack(">I", exact(connection, 4))
        require(4 <= length <= 128 * 1024, "invalid fixture frame length")
        data = exact(connection, length)
    count, = struct.unpack_from(">I", data)
    require(0 < count <= 16, "invalid fixture field count")
    position = 4
    result = []
    for _ in range(count):
        size, = struct.unpack_from(">I", data, position)
        position += 4
        require(size <= 65536 and position + size <= len(data), "invalid fixture string length")
        result.append(data[position:position + size].decode("utf-8"))
        position += size
    require(position == len(data), "trailing fixture bytes")
    return result


class OwnedProcess:
    def __init__(self, command, directory, label, env=None):
        self.directory = directory
        self.label = label
        self.log = directory / (label + ".log")
        self.stream = self.log.open("xb")
        self.process = subprocess.Popen(command, stdout=self.stream, stderr=subprocess.STDOUT,
                                        env=env, cwd=directory, start_new_session=True)
        self.closed = False

    def stop(self):
        if self.closed:
            return
        forced = self.process.poll() is None
        if forced:
            os.killpg(self.process.pid, signal.SIGKILL)
        code = self.process.wait(timeout=15)
        self.stream.close()
        with (self.directory / "processes.jsonl").open("a") as record:
            record.write(json.dumps(dict(label=self.label, pid=self.process.pid,
                                         forced=forced, exit=code, waited=True)) + "\n")
        self.closed = True

    def alive(self):
        require(self.process.poll() is None, f"{self.label} exited: {self.log.read_text(errors='replace')[-12000:]}")


class Http:
    def __init__(self, port):
        self.base = f"http://127.0.0.1:{port}"
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookies))
        self.csrf = ""

    def request(self, path, body=None, csrf=True):
        headers = {"Accept": "application/json", "Cache-Control": "no-cache"}
        if body is not None:
            headers["Content-Type"] = "application/json"
            if csrf:
                headers["Csrf-Token"] = self.csrf
        request = urllib.request.Request(self.base + path, data=None if body is None else json.dumps(body).encode(), headers=headers)
        try:
            response = self.opener.open(request, timeout=60)
        except urllib.error.HTTPError as response:
            return response.code, response.read().decode("utf-8")
        with response:
            return response.status, response.read().decode("utf-8")

    def json(self, path, body=None, expected=(200,), csrf=True):
        status, text = self.request(path, body, csrf)
        require(status in expected, f"HTTP {status} at {path}: {text[:4000]}")
        return json.loads(text)


def wait_until(label, check, child, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        child.alive()
        try:
            value = check()
            if value:
                return value
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as error:
            last = str(error)
        time.sleep(.25)
    raise AssertionError(f"timeout waiting for {label}: {last}; log={child.log}")


def run(args, kind):
    evidence = Path(args.evidence_root).resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="r2-http-" + kind + "-", dir=evidence))
    (directory / ".owned-r2-http-validation").write_text("Only processes created by verify_http_consumers.py\n")
    external_port, http_port = free_port(), free_port()
    while external_port == http_port:
        http_port = free_port()
    fixture = None
    application = None
    report = {"kind": kind, "evidence": str(directory), "scenarios": []}
    secret = secrets.token_hex(32)
    env = os.environ.copy()
    env["JAVA_HOME"] = str(Path(args.java).resolve().parent.parent)
    env["PATH"] = str(Path(args.java).resolve().parent) + os.pathsep + env.get("PATH", "")
    if kind == "bank":
        stage = Path(args.bank_stage).resolve()
        env.update(BANK_DURABLE_PLAY_SECRET=secret, MONARCH_DURABLE_BANK_DIR=str(directory / "application"),
                   MONARCH_DURABLE_BANK_EXTERNAL_PORT=str(external_port), MONARCH_DURABLE_BANK_EXTERNAL_CONTROLS="true")
        profile, page = "application-durable-bank.conf", "/bank-batch/durable"
        executable = stage / "bin/bank-batch-transactor-web"
        count = 3
    else:
        stage = Path(args.fab_stage).resolve()
        env.update(FAB_DURABLE_PLAY_SECRET=secret, FAB_DURABLE_STORAGE_ROOT=str(directory / "application"))
        profile, page = "application-durable-fab.conf", "/fab/durable"
        executable = stage / "bin/fab-simulator-web"
        count = 2
    require(executable.is_file(), f"missing staged consumer {executable}")
    application_args = [str(executable), "-J-Xms64m", "-J-Xmx384m", "-Dconfig.resource=" + profile,
                        "-Dhttp.address=127.0.0.1", "-Dhttp.port=" + str(http_port),
                        "-Dplay.server.http.address=127.0.0.1", "-Dplay.server.http.port=" + str(http_port),
                        "-Dpidfile.path=/dev/null", "-Dmonarch.operation.max-checks=1",
                        "-Dmonarch.operation.manual-check-budget=30", "-Dmonarch.delivery.max-polls=6",
                        "-Dmonarch.delivery.poll-interval=1s"]
    if kind == "fab":
        application_args += ["-Dfab.durable.fixture-port=" + str(external_port), "-Dfab.durable.fixture-controls=true"]

    def start_application(label):
        child = OwnedProcess(application_args, directory, label, env)
        client = Http(http_port)
        try:
            html = wait_until("HTTP page", lambda: html_response(client), child)
            match = re.search(r'const\s+csrfToken\s*=\s*("(?:[^"\\]|\\.)*")', html)
            require(match is not None, "HTML must expose the issued CSRF token to its own POST buttons")
            client.csrf = json.loads(match.group(1))
            require(client.csrf, "missing CSRF token")
            (directory / (label + ".html")).write_text(html)
            for index, script in enumerate(re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)):
                path = directory / f"{label}-{index}.js"
                path.write_text(script)
                subprocess.run([args.node, "--check", str(path)], check=True, capture_output=True)
            return child, client
        except BaseException:
            child.stop()
            raise

    def html_response(client):
        status, text = client.request(page)
        if status == 200:
            return text
        if status >= 500:
            raise AssertionError(f"page HTTP {status}: {text[:6000]}")
        return None

    def base(job):
        return ("/api/bank-durable/" if kind == "bank" else "/fab/durable/jobs/") + job

    def current(client, job):
        return client.json(base(job) + ("/state" if kind == "bank" else ""))

    def submit(client, job, number, expected=(200, 202)):
        path = base(job) + "/start" + ("?count=" + str(number) if kind == "fab" else "")
        return client.json(path, {"count": number} if kind == "bank" else {}, expected=expected)

    def tasks(client, job):
        path = "/api/bank-durable/external/tasks" if kind == "bank" else base(job) + "/tasks"
        return client.json(path)["operationKeys"]

    def complete(client, job, key):
        if kind == "bank":
            client.json("/api/bank-durable/external/complete", {"jobId": job, "operationKey": key})
        else:
            client.json(base(job) + "/complete?operationKey=" + urllib.parse.quote(key, safe=""), {})

    def reconcile(client, job, batch_id):
        # Explicit test operator action, never part of the consumer's automatic polling.
        path = base(job) + "/reconcile?batchId=" + urllib.parse.quote(batch_id, safe="")
        status, text = client.request(path, {})
        require(status in (200, 409), f"reconciliation HTTP {status}: {text}")

    def verify_origin(view):
        origin = view["runtimeOrigin"]
        require(origin.endswith(".jar") and "monarch-persistence" in origin, f"non-artifact runtime: {origin}")
        path = Path(urllib.request.url2pathname(urllib.parse.urlparse(origin).path))
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        require(actual == args.expected_persistence_sha, f"wrong persistence JAR {path}: {actual}")
        report["runtimeOrigin"] = origin
        report["persistenceSha256"] = actual

    def drive(client, job, child):
        deadline = time.monotonic() + 150
        decisions = {}
        last = None
        while time.monotonic() < deadline:
            child.alive()
            last = current(client, job)
            verify_origin(last)
            if last["phase"] == "Drained":
                require(last["accepted"] == count and last["succeeded"] == count and last["failed"] == 0 and
                        last["held"] == 0 and last["active"] == 0 and last["inputClosed"] and not last["batches"], last)
                return last
            owned = {b["batchId"] + "/execute": b for b in last["batches"]}
            for key in tasks(client, job):
                if key in owned:
                    complete(client, job, key)
                    reconcile(client, job, owned[key]["batchId"])
            for batch in last["batches"]:
                if batch["phase"] == "Blocked":
                    key = batch["batchId"]
                    attempts, previous = decisions.get(key, (0, 0))
                    if time.monotonic() - previous > 2:
                        require(attempts < 3, f"batch stayed blocked after bounded manual actions: {last}")
                        reconcile(client, job, key)
                        decisions[key] = attempts + 1, time.monotonic()
            time.sleep(.25)
        raise AssertionError(f"HTTP workflow did not drain: {last}")

    def verify_items(port, jobs):
        rows = ledger(port, "ITEMS")
        require(rows[0] == "ITEMS", rows)
        entries = rows[1 if kind == "bank" else 2].split(",")
        require(len(entries) == count * jobs, rows)
        for encoded in entries:
            fields = encoded.split("~")
            item = base64.urlsafe_b64decode(fields[0] + "=" * (-len(fields[0]) % 4)).decode()
            require(item.startswith("http-" + kind + "-"), f"unexpected ledger item {item}")
            if kind == "bank":
                require(fields[1:] == ["0", "100"], f"wrong customer balance/holding: {item} {fields}")
            else:
                require(fields[1] == "stocker", f"FOUP not unloaded: {item} {fields}")

    try:
        fixture = OwnedProcess([args.java, "-Xms32m", "-Xmx192m", "-cp", args.fixture_classpath,
                                "net.imadz.monarch.persistence.ExternalTaskServiceFixture",
                                str(directory / "external"), str(external_port)], directory, "external")
        wait_until("external ledger", lambda: ledger(external_port, "EPOCH"), fixture, 30)
        application, client = start_application("first")
        job = "http-" + kind + "-normal"
        # POSTs must not bypass the browser's CSRF protection.
        denied, _ = client.request(base(job) + "/start", {}, csrf=False)
        require(denied == 403, f"missing CSRF token must be denied, got {denied}")
        submit(client, job, count)
        normal = drive(client, job, application)
        before = ledger(external_port, "COUNT")
        duplicate = submit(client, job, count)
        require(ledger(external_port, "COUNT") == before, "duplicate completed input caused external calls")
        submit(client, job, count + 1, expected=(409,))
        report["scenarios"].append(dict(name="normal-and-duplicate", view=normal, duplicate=duplicate,
                                         count=before, business=ledger(external_port, "BUSINESS")))
        verify_items(external_port, 1)

        recovered_job = "http-" + kind + "-recovery"
        submit(client, recovered_job, count)
        wait_until("durable blocked budget", lambda: current(client, recovered_job)["phase"] == "ClosingBlocked",
                   application, 25)
        running = tasks(client, recovered_job)
        require(running, "independent operation must still be running before killing the application")
        # Kill the entire application JVM; the external task and its durable history survive.
        application.stop()
        application = None
        for key in running:
            require(ledger(external_port, "COMPLETE", key)[0] == "SUCCEEDED", "external completion failed")
        application, client = start_application("recovered")
        restored = current(client, recovered_job)
        require(restored["phase"] == "ClosingBlocked", restored)
        for batch in restored["batches"]:
            reconcile(client, recovered_job, batch["batchId"])
        recovered = drive(client, recovered_job, application)
        counters = ledger(external_port, "COUNT")
        require(counters[1] == counters[2] == counters[3], f"orphan task or repeated submit: {counters}")
        business = ledger(external_port, "BUSINESS")
        if kind == "bank":
            require(business[1:5] == ["6", "6", "0", "600"], business)
        else:
            require(business[5:7] == ["4", "4"], business)
        require(business[7] == "0", business)
        verify_items(external_port, 2)
        report["scenarios"].append(dict(name="blocked-whole-jvm-recovery", restored=restored, view=recovered,
                                         count=counters, business=business, items=ledger(external_port, "ITEMS")))
        application.stop()
        application = None
        application, client = start_application("terminal-recovered")
        terminal = current(client, recovered_job)
        require(terminal == recovered, "committed terminal view changed after a fresh JVM")
        require(ledger(external_port, "COUNT") == counters, "terminal activation called the external service")
        report["scenarios"].append(dict(name="terminal-whole-jvm-recovery", view=terminal))
        report["passed"] = True
        (directory / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(dict(kind=kind, passed=True, evidence=str(directory)), ensure_ascii=False), flush=True)
    except BaseException as error:
        report.update(passed=False, error=str(error))
        (directory / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        raise
    finally:
        if application is not None:
            application.stop()
        if fixture is not None:
            fixture.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=("bank", "fab", "both"), default="both")
    parser.add_argument("--java", required=True)
    parser.add_argument("--node", default="node")
    parser.add_argument("--fixture-classpath", required=True)
    parser.add_argument("--bank-stage")
    parser.add_argument("--fab-stage")
    parser.add_argument("--expected-persistence-sha", required=True)
    parser.add_argument("--evidence-root", required=True)
    args = parser.parse_args()
    kinds = ("bank", "fab") if args.kind == "both" else (args.kind,)
    for kind in kinds:
        require(getattr(args, kind + "_stage"), "stage path required for " + kind)
        run(args, kind)


if __name__ == "__main__":
    main()
