#!/usr/bin/env python3
"""Check whether the running Compose workers can read the producers' batch topics."""

import subprocess
import sys


def output(*args):
    return subprocess.check_output(args, text=True, timeout=15).strip()


def running_modes(service):
    ids = output("docker", "compose", "ps", "--status", "running", "-q", service)
    if not ids:
        raise ValueError(f"No running {service} container in this Compose project")
    modes = []
    for container in ids.splitlines():
        # Read exactly one variable; the container environment also holds provider credentials.
        mode = output("docker", "exec", container, "sh", "-c", 'printf "%s" "${BATCH_ROUTING:-shared}"')
        if mode not in {"channel", "dual-read", "shared"}:
            raise ValueError(f"Unrecognized routing mode on {service} {container[:12]}")
        modes.append(mode)
    return modes


def check():
    writers = running_modes("api")
    workers = running_modes("worker")
    print(f"Running API routing: {', '.join(writers)}. Worker routing: {', '.join(workers)}.")
    if any(mode in {"channel", "dual-read"} for mode in workers):
        return
    if "channel" in writers:
        raise ValueError("The API writes channel batch topics but all workers read shared topics only")
    # An earlier producer may have committed channel work before this rollback.
    # Match the current generation so a retried notification does not give a false alarm.
    query = """
        SELECT EXISTS (
            SELECT 1 FROM notifications n JOIN outbox o ON o.aggregateid = n.id
            WHERE n.status = 'pending' AND o.aggregatetype LIKE 'notifications.%.batch.%'
              AND o.payload->>'attempt' = n.attempt::text
        )
    """
    pending = output("docker", "compose", "exec", "-T", "postgres",
                     "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "notify", "-d", "notify",
                     "-Atc", query)
    if pending != "f":
        raise ValueError("Cannot confirm all channel-routed pending work has drained")


if __name__ == "__main__":
    try:
        check()
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        sys.exit(f"Batch routing check failed: {error}. Inspect the rollout in docs/channel-batch-routing.md.")
