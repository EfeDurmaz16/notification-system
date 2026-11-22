"""Run the SMS partition and feedback matrix after starting the isolated fixture."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from urllib.request import urlopen

LAB = Path(__file__).resolve().parent
ROOT = LAB.parent.parent
RUNS = LAB / "runs"
RUNS.mkdir(exist_ok=True)
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("traffic_wave", ROOT / "scripts/traffic-wave.py")
wave = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wave)
if not os.environ.get("COMPOSE_PROJECT_NAME", "").startswith("notify-partition-replay-"):
    sys.exit("Start the isolated project with run.sh.")
if os.environ.get("PROVIDER_URL") != "http://provider:8081/send":
    sys.exit("The scaling replay requires the local provider.")
C = [
    "docker",
    "compose",
    "-f",
    str(ROOT / "docker-compose.yml"),
    "-p",
    os.environ["COMPOSE_PROJECT_NAME"],
]
API = "http://127.0.0.1:" + os.environ["API_PORT"]
PROVIDER = "http://127.0.0.1:" + os.environ["PROVIDER_PORT"]
LOAD = LAB / "loadtest"
TS = ("high", "normal", "low")


def save(path, value):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(value, indent=2) + "\n")
    tmp.replace(path)


def log(*args):
    print(datetime.now(timezone.utc).isoformat(), *args, flush=True)


def cmd(args, timeout=90):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, timeout=timeout)


def compose(*args):
    return cmd([*C, *args])


class Exp:
    api = API

    def compose(self, *args, capture=False, check=True):
        return subprocess.run(
            [*C, *args], text=True, capture_output=capture, check=check, timeout=90
        )

    def stats(self):
        with urlopen(PROVIDER + "/stats", timeout=8) as r:
            return json.load(r)


KAFKA = [
    "exec",
    "-T",
    "kafka",
    "/opt/kafka/bin/kafka-consumer-groups.sh",
    "--bootstrap-server",
    "kafka:9092",
    "--describe",
]
for tier in TS:
    KAFKA += ["--group", f"delivery.{tier}.batch.sms"]


def offsets():
    raw = compose(*KAFKA)
    rows = {}
    for line in raw.splitlines():
        f = line.split()
        if len(f) >= 8 and f[0] in {f"delivery.{t}.batch.sms" for t in TS}:
            rows[f"{f[0].split('.')[1]}:{f[2]}"] = {
                "committed": int(f[3]) if f[3].isdigit() else None,
                "end": int(f[4]) if f[4].isdigit() else None,
                "owner": f[7].strip("/"),
            }
    return {"records": rows, "raw": raw}


def drained(partitions, timeout=65):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        s = offsets()
        if set(s["records"]) == {f"{t}:{i}" for t in TS for i in range(partitions)} and all(
            row["owner"] != "-"
            and row["end"] is not None
            and (row["committed"] == row["end"] or row["committed"] is None and row["end"] == 0)
            for row in s["records"].values()
        ):
            return s
        time.sleep(1)
    raise ValueError("Partition offsets/assignments did not settle: " + str(s))


def load_case(d, count, strict):
    p = subprocess.run(
        [
            str(LOAD),
            "-api",
            API,
            "-provider",
            PROVIDER,
            "-scenario",
            "burst",
            "-total",
            str(count),
            "-rate",
            "0",
            "-batch",
            str(min(1000, count)),
            "-deadline",
            "12m",
            "-audit-receipts",
            "-out",
            str(d),
        ],
        capture_output=True,
        text=True,
        timeout=750,
    )
    d.with_suffix(".log").write_text(p.stdout + p.stderr)
    data = json.loads(d.read_text())
    expected = {**data["checks"]}
    expected.pop("providerRollingSecondWithinQuota", None)
    ok = (
        all(expected.values())
        and data["acceptedNotifications"] == count
        and data["finalObservedStates"] == {"accepted": count}
        and data["receiptAudit"]
        == {"checked": count, "zeroPosts": 0, "multiplePosts": 0, "readErrors": 0}
    )
    if strict and not ok:
        log("WORKLOAD CHECK FAILED", d, p.returncode, data["checks"], data["receiptAudit"])
    return data, ok


def case(parts, policy, count, cap):
    name = f"p{parts}-{policy}-{count}-q{cap}"
    d = RUNS / name
    if (d / "result.json").exists():
        return
    d.mkdir(exist_ok=True)
    if (d / "load.json").exists():
        raise ValueError("Partially completed evidence, inspect: " + str(d))
    initial = 3 if policy == "fixed3" else 1
    log("preparing", name)
    compose(
        "up",
        "-d",
        "--no-deps",
        "--force-recreate",
        "--scale",
        f"worker={initial}",
        "--wait",
        "worker",
    )
    quota_ack = compose("exec", "-T", "redis", "redis-cli", "SET", "experiment:quota", str(cap))
    if quota_ack.strip() != "OK":
        raise RuntimeError(f"Could not set the benchmark quota to {cap}: {quota_ack}")
    time.sleep(1.1)
    _, warm_ok = load_case(d / "warmup.json", 300, False)
    if not warm_ok:
        raise RuntimeError(f"Warm-up checks failed for {name}")
    before = drained(parts)
    save(d / "before.json", before)
    time.sleep(1.1)
    start = time.monotonic()
    done = threading.Event()
    done_kafka = threading.Event()
    timeline = []
    events = []
    snapshots = []

    def watch():
        while not done_kafka.is_set():
            sample = offsets()
            sample["s"] = round(time.monotonic() - start, 2)
            snapshots.append(sample)
            done_kafka.wait(4)

    exp = Exp()
    log("timing", name)
    with ThreadPoolExecutor(max_workers=2) as pool:
        mon = pool.submit(wave.monitor, exp, policy, start, 30, done, timeline, events)
        owner_watch = pool.submit(watch)
        try:
            result, ok = load_case(d / "load.json", count, True)
            post_drained = drained(parts)
            done_kafka.set()
            if policy == "feedback":
                until = time.monotonic() + 100
                while time.monotonic() < until and not mon.done():
                    if (
                        timeline
                        and timeline[-1]["replicas"] == 1
                        and timeline[-1]["pending"] == 0
                        and events
                        and events[-1]["to"] == 1
                    ):
                        break
                    time.sleep(0.4)
        finally:
            done.set()
            done_kafka.set()
            mon.result()
            owner_watch.result()
            save(
                d / "samples.json",
                {"events": events, "timeline": timeline, "partitionSamples": snapshots},
            )
    post = drained(parts)
    delta = {k: r["end"] - before["records"][k]["end"] for k, r in post["records"].items()}
    save(d / "after.json", post)
    save(d / "post-drain-owners.json", post_drained)
    r = {
        k: result[k]
        for k in [
            "runId",
            "elapsedSeconds",
            "observedTerminalPerElapsedSecond",
            "createStartToPersistedTerminal",
            "createStartToPersistedTerminalByPriority",
            "receiptAudit",
            "finalObservedStates",
            "provider",
            "checks",
            "pollErrors",
            "ingressSeconds",
        ]
    }
    scaledown = events and events[-1]["to"] == 1 and timeline[-1]["replicas"] == 1
    r.update(
        name=name,
        partitions=parts,
        policy=policy,
        quota=cap,
        offered=count,
        offsetDeltas=delta,
        events=events,
        actualScale=[
            s["replicas"]
            for i, s in enumerate(timeline)
            if not i or timeline[i - 1]["replicas"] != s["replicas"]
        ],
        replicaSeconds=wave.replica_seconds(timeline, result["elapsedSeconds"], initial),
        withinQuota=result["provider"]["channels"]["sms"]["peakPerSecond"] <= cap,
        functionalOK=ok,
        deltaCheck=sum(delta.values()) == count and all(v > 0 for v in delta.values()),
        scaledDown=bool(scaledown) if policy == "feedback" else None,
    )
    save(d / "result.json", r)
    log(
        "result",
        name,
        round(r["observedTerminalPerElapsedSecond"], 1),
        "actual",
        r["actualScale"],
        "IDs",
        r["functionalOK"],
        "offsets",
        r["deltaCheck"],
    )
    if result["notObservedTerminal"] or result["observedTerminal"] != count:
        raise RuntimeError("Undrained workload, preserving receipt evidence")


def expand():
    kafka = [
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "kafka:9092",
    ]
    for t in TS:
        compose(*kafka, "--alter", "--topic", f"notifications.{t}.batch.sms", "--partitions", "3")
    compose("restart", "connect")
    started = time.monotonic()
    while time.monotonic() - started < 90:
        try:
            with urlopen(
                "http://127.0.0.1:"
                + os.environ["CONNECT_PORT"]
                + "/connectors/notification-outbox/status",
                timeout=4,
            ) as response:
                c = json.load(response)
                if (
                    c["connector"]["state"] == "RUNNING"
                    and len(c["tasks"])
                    and all(t["state"] == "RUNNING" for t in c["tasks"])
                ):
                    break
        except OSError:
            pass
        time.sleep(2)
    else:
        raise RuntimeError("Connector did not recover after topology change")
    save(
        LAB / "expanded.json",
        {t: compose(*kafka, "--describe", "--topic", f"notifications.{t}.batch.sms") for t in TS},
    )


def main():
    (LAB / "baseline-image.txt").write_text(
        cmd(["docker", "image", "inspect", os.environ["APP_IMAGE"], "--format", "{{.Id}}"])
    )
    for policy in ("fixed1", "feedback", "fixed3"):
        case(1, policy, 10000, 10000)
    if not (LAB / "expanded.json").exists():
        expand()
    for policy in ("fixed3", "feedback", "fixed1"):
        case(3, policy, 10000, 10000)
    for policy in ("fixed1", "feedback"):
        case(3, policy, 3000, 100)
    for policy in ("fixed3", "feedback", "fixed1"):
        case(3, policy, 100000, 10000)


if __name__ == "__main__":
    main()
