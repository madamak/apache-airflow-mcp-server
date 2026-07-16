#!/usr/bin/env python3
"""Seed the E2E Airflow instance with DAG runs in known terminal states.

Talks straight to the Airflow REST API (NOT through the MCP server — the seed
must be independent of the code under test). Idempotent: re-running against an
already-seeded instance is a no-op.

Usage:
    python tests/e2e/seed.py --base-url http://localhost:18080 --api-version v1
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

USERNAME = "admin"
PASSWORD = "admin"

EXPECTED_DAGS = [
    "e2e_success",
    "e2e_failure",
    "e2e_noisy_logs",
    "e2e_sensor_reschedule",
    "e2e_dataset_producer",
    "e2e_dataset_consumer",
]

EXPECTED_TASKS = {
    "e2e_success": {"say_ok"},
    "e2e_failure": {"transform"},
    "e2e_noisy_logs": {"spew"},
    "e2e_sensor_reschedule": {"wait_for_time"},
    "e2e_dataset_producer": {"produce"},
    "e2e_dataset_consumer": {"consume"},
}

# dag_id -> expected terminal run state; consumer is dataset-triggered, not seeded directly
TRIGGERED = {
    "e2e_success": "success",
    "e2e_failure": "failed",
    "e2e_noisy_logs": "success",
    "e2e_sensor_reschedule": "success",
    "e2e_dataset_producer": "success",
}

SEED_RUN_PREFIX = "e2e_seed"
SEED_SCHEMA_VERSION = 1
SEED_VERSION_CONF_KEY = "airflow_mcp_e2e_fixture_version"
TERMINAL = {"success", "failed"}


class AirflowApi:
    """Thin version-aware REST client (v1 basic auth, v2 JWT)."""

    def __init__(self, base_url: str, api_version: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self._client = httpx.Client(timeout=30)
        if api_version == "v2":
            resp = self._client.post(
                f"{self.base_url}/auth/token",
                json={"username": USERNAME, "password": PASSWORD},
            )
            resp.raise_for_status()
            token = resp.json()["access_token"]
            self._client.headers["Authorization"] = f"Bearer {token}"
        else:
            self._client.auth = (USERNAME, PASSWORD)

    def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        url = f"{self.base_url}/api/{self.api_version}{path}"
        return self._client.request(method, url, **kwargs)

    def get(self, path: str, **kwargs) -> httpx.Response:
        return self.request("GET", path, **kwargs)


def wait_for(description: str, check, timeout: int, interval: float = 3.0):
    """Poll `check` until it returns a truthy value; die loudly on timeout."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            last_error = exc
        time.sleep(interval)
    raise SystemExit(
        f"TIMEOUT after {timeout}s waiting for: {description} (last error: {last_error})"
    )


def wait_healthy(api: AirflowApi, timeout: int) -> None:
    def check() -> bool:
        health_path = "/health" if api.api_version == "v1" else "/api/v2/monitor/health"
        resp = httpx.get(f"{api.base_url}{health_path}", timeout=10)
        if resp.status_code != 200:
            return False
        health = resp.json()
        meta = health.get("metadatabase", {}).get("status")
        sched = health.get("scheduler", {}).get("status")
        return meta == "healthy" and sched == "healthy"

    wait_for("metadatabase + scheduler healthy", check, timeout)
    print("health: OK")


def wait_dags_parsed(api: AirflowApi, timeout: int) -> None:
    def check() -> bool:
        resp = api.get("/dags", params={"limit": 100})
        resp.raise_for_status()
        present = {d["dag_id"] for d in resp.json()["dags"]}
        missing = set(EXPECTED_DAGS) - present
        if missing:
            print(f"waiting for DAGs to be parsed, missing: {sorted(missing)}")
            return False

        mismatched = {}
        for dag_id, expected in EXPECTED_TASKS.items():
            tasks_resp = api.get(f"/dags/{dag_id}/tasks")
            tasks_resp.raise_for_status()
            actual = {task["task_id"] for task in tasks_resp.json()["tasks"]}
            if actual != expected:
                mismatched[dag_id] = {"expected": sorted(expected), "actual": sorted(actual)}
        if mismatched:
            print(f"waiting for current fixture DAG definitions: {mismatched}")
            return False
        return True

    wait_for("all fixture DAGs parsed", check, timeout)
    # Fixture DAGs set is_paused_upon_creation=False, but unpause defensively
    # (e.g. instance predates that attribute or was toggled by a test run).
    for dag_id in EXPECTED_DAGS:
        resp = api.request("PATCH", f"/dags/{dag_id}", json={"is_paused": False})
        resp.raise_for_status()
    print(f"dags: all {len(EXPECTED_DAGS)} current fixture DAGs parsed and unpaused")


def fixture_version(dags_dir: Path) -> str:
    """Return a stable version derived from every fixture DAG's path and content."""
    dag_files = sorted(dags_dir.glob("*.py"))
    if not dag_files:
        raise SystemExit(f"No fixture DAG files found in {dags_dir}")

    digest = hashlib.sha256()
    digest.update(f"seed-schema:{SEED_SCHEMA_VERSION}\0".encode())
    for dag_file in dag_files:
        digest.update(dag_file.name.encode())
        digest.update(b"\0")
        digest.update(dag_file.read_bytes())
        digest.update(b"\0")
    return f"v{SEED_SCHEMA_VERSION}:{digest.hexdigest()}"


def seed_run_id(dag_id: str) -> str:
    return f"{SEED_RUN_PREFIX}_{dag_id}"


def existing_seed_run(api: AirflowApi, dag_id: str) -> dict | None:
    resp = api.get(f"/dags/{dag_id}/dagRuns/{seed_run_id(dag_id)}")
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def validate_existing_seed_run(dag_id: str, run: dict, version: str) -> None:
    """Reject terminal runs produced by stale fixtures or with the wrong result."""
    run_version = (run.get("conf") or {}).get(SEED_VERSION_CONF_KEY)
    if run_version != version:
        raise SystemExit(
            f"STALE SEED: {dag_id}/{seed_run_id(dag_id)} has fixture version "
            f"{run_version!r}, expected {version!r}; recreate the E2E environment"
        )

    state = run.get("state")
    expected = TRIGGERED[dag_id]
    if state in TERMINAL and state != expected:
        raise SystemExit(
            f"SEED MISMATCH: existing {dag_id} run ended {state!r}, expected {expected!r}"
        )


def trigger_seed_runs(api: AirflowApi, version: str) -> list[str]:
    """Trigger seed runs that don't exist yet; return the dag_ids to wait on."""
    to_wait = []
    for dag_id in TRIGGERED:
        run = existing_seed_run(api, dag_id)
        if run is not None:
            validate_existing_seed_run(dag_id, run, version)
        state = run.get("state") if run else None
        if state in TERMINAL:
            print(f"trigger: {dag_id} already seeded (state={state}), skipping")
            continue
        if state is None:
            body: dict = {
                "dag_run_id": seed_run_id(dag_id),
                "conf": {SEED_VERSION_CONF_KEY: version},
            }
            if api.api_version == "v2":
                body["logical_date"] = None  # v2 requires the key; null = "now"
            resp = api.request("POST", f"/dags/{dag_id}/dagRuns", json=body)
            if resp.status_code == 409:
                raced_run = existing_seed_run(api, dag_id)
                if raced_run is None:
                    raise SystemExit(
                        f"Seed run creation for {dag_id} returned 409 but the run cannot be read"
                    )
                validate_existing_seed_run(dag_id, raced_run, version)
                print(f"trigger: {dag_id} run already exists (409, fixture version validated)")
            else:
                resp.raise_for_status()
                print(f"trigger: {dag_id} -> {seed_run_id(dag_id)}")
        else:
            print(f"trigger: {dag_id} run exists in state={state}, waiting")
        to_wait.append(dag_id)
    return to_wait


def wait_terminal(api: AirflowApi, dag_ids: list[str], timeout: int) -> None:
    for dag_id in dag_ids:
        expected = TRIGGERED[dag_id]

        def check(dag_id: str = dag_id) -> str | None:
            run = existing_seed_run(api, dag_id)
            state = run.get("state") if run else None
            return state if state in TERMINAL else None

        state = wait_for(f"{dag_id} seed run terminal", check, timeout)
        if state != expected:
            raise SystemExit(f"SEED MISMATCH: {dag_id} ended '{state}', expected '{expected}'")
        print(f"run: {dag_id} -> {state} (expected)")


def _parse_airflow_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def wait_consumer_run(api: AirflowApi, timeout: int, *, producer_was_active: bool) -> None:
    """The consumer DAG is dataset/asset-scheduled off the producer."""
    producer = existing_seed_run(api, "e2e_dataset_producer")
    producer_started = _parse_airflow_datetime(producer.get("start_date") if producer else None)
    if producer_started is None:
        raise SystemExit("Dataset producer seed run has no start_date")

    def check() -> bool:
        resp = api.get("/dags/e2e_dataset_consumer/dagRuns", params={"limit": 100})
        resp.raise_for_status()
        runs = resp.json()["dag_runs"]
        return any(
            run["state"] == "success"
            and (started := _parse_airflow_datetime(run.get("start_date"))) is not None
            and started >= producer_started
            for run in runs
        )

    if not producer_was_active and not check():
        raise SystemExit(
            "STALE SEED: producer is already terminal but no matching consumer run exists; "
            "recreate the E2E environment"
        )
    wait_for("dataset-triggered consumer run success", check, timeout)
    print("run: e2e_dataset_consumer -> success (dataset-triggered)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--api-version", choices=["v1", "v2"], default="v1")
    parser.add_argument("--timeout", type=int, default=300, help="per-phase timeout (seconds)")
    parser.add_argument(
        "--dags-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "dags",
        help="fixture DAG directory used to validate reused seed runs",
    )
    args = parser.parse_args()

    api = AirflowApi(args.base_url, args.api_version)
    version = fixture_version(args.dags_dir)
    print(f"fixture version: {version}")
    wait_healthy(api, args.timeout)
    wait_dags_parsed(api, args.timeout)
    to_wait = trigger_seed_runs(api, version)
    wait_terminal(api, to_wait, args.timeout)
    wait_consumer_run(
        api,
        args.timeout,
        producer_was_active="e2e_dataset_producer" in to_wait,
    )
    print("SEED COMPLETE")


if __name__ == "__main__":
    sys.exit(main())
