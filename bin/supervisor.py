#!/usr/bin/env python3
"""Supervise only newly launched children; never adopt PIDs from state or /proc."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from threading import Event, Thread
import time
import tomllib

import common

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVICE_NAMES = {"binance_smy_ws", "ingest_worker", "webhook_server", "gmgn_adapter"}


@dataclass(frozen=True)
class Service:
    name: str
    enabled: bool
    command: tuple[str, ...]


def load_services(path=None, *, root=None, project_root=PROJECT_ROOT, python=None):
    root = Path(root) if root is not None else common.ROOT
    path = Path(path) if path is not None else root / "config/services.toml"
    with path.open("rb") as stream:
        config = tomllib.load(stream)
    sections = config.get("services")
    if set(config) != {"services"} or not isinstance(sections, dict) or not sections:
        raise ValueError("expected nonempty [services.<name>] tables")
    services = []
    for name, values in sections.items():
        if name not in SERVICE_NAMES or not isinstance(values, dict):
            raise ValueError(f"unknown service: {name}")
        if set(values) - {"enabled", "args"} or type(values.get("enabled")) is not bool:
            raise ValueError(f"{name}: enabled must be a boolean; only enabled/args are supported")
        if name == "gmgn_adapter" and values["enabled"]:
            raise ValueError("gmgn_adapter is a one-shot adapter and cannot be supervisor-managed")
        args = values.get("args", [])
        if not isinstance(args, list) or any(not isinstance(arg, str) or "\0" in arg for arg in args):
            raise ValueError(f"{name}: args must be an array of strings")
        command = (str(python or sys.executable), "-u",
                   str(Path(project_root).resolve() / "bin" / f"{name}.py"), *args)
        services.append(Service(name, values["enabled"], command))
    return services


@dataclass
class Child:
    service: Service
    process: object = None
    restarts: int = 0
    last_exit_code: int | None = None
    attempts: int = 0
    failures: int = 0
    retry_at: float = 0
    backoff_s: int = 0
    started_at: float = 0
    error: str | None = None


class Supervisor:
    def __init__(self, services, *, root=None, project_root=PROJECT_ROOT,
                 clock=time.monotonic, popen=subprocess.Popen, stop=None,
                 shutdown_timeout=30):
        self.root = Path(root) if root is not None else common.ROOT
        self.project_root = Path(project_root).resolve()
        self.clock, self.popen = clock, popen
        self.stop = stop if stop is not None else Event()
        self.shutdown_timeout = shutdown_timeout
        self.children = {service.name: Child(service) for service in services}
        self.readers = []

    def log(self, event, **fields):
        common.log_event(event, log_name="supervisor", log_root=self.root, **fields)

    def request_shutdown(self, *_):
        # Signal handlers only set a flag. No logging, blocking or PID lookup.
        self.stop.set()

    def write_state(self):
        services = {}
        for name, child in self.children.items():
            services[name] = {
                "enabled": child.service.enabled,
                "pid": child.process.pid if child.process is not None else None,
                "restarts": child.restarts,
                "last_exit_code": child.last_exit_code,
                "backoff_s": child.backoff_s,
                "error": child.error,
            }
        common.atomic_write(self.root / "state/supervisor.json", json.dumps({
            "pid": os.getpid(), "stopping": self.stop.is_set(), "services": services,
        }, indent=2) + "\n")

    def capture_output(self, name, stream):
        try:
            # Bound a single record even if a child emits no newline.
            with stream:
                while True:
                    line = stream.readline(65536)
                    if not line:
                        break
                    self.log("service_output", service=name, message=line.rstrip("\r\n"))
        except (OSError, ValueError):
            self.log("output_error", service=name)

    def schedule_restart(self, child):
        child.backoff_s = min(60, 2 ** min(child.failures, 6))
        child.failures += 1
        child.retry_at = self.clock() + child.backoff_s

    def step(self):
        if self.stop.is_set():
            return
        changed = False
        for name, child in self.children.items():
            if self.stop.is_set():
                break
            if not child.service.enabled:
                continue
            if child.process is not None:
                code = child.process.poll()
                if code is None:
                    continue
                child.last_exit_code = code
                child.process = None
                if self.clock() - child.started_at >= 60:
                    child.failures = 0
                self.schedule_restart(child)
                self.log("service_exited", service=name, exit_code=code,
                         backoff_s=child.backoff_s)
                changed = True
            if child.process is None and self.clock() >= child.retry_at and not self.stop.is_set():
                env = dict(os.environ)
                env["SIGNALS_ROOT"] = str(self.root.resolve())
                env["PYTHONPATH"] = os.pathsep.join(filter(None, (
                    str(self.project_root / "bin"), str(self.project_root), env.get("PYTHONPATH"))))
                try:
                    process = self.popen(child.service.command, cwd=self.project_root, env=env,
                                         stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                                         errors="replace", start_new_session=True)
                except OSError as exc:
                    child.error = type(exc).__name__
                    self.schedule_restart(child)
                    self.log("service_start_failed", service=name, error=child.error,
                             backoff_s=child.backoff_s)
                else:
                    child.process = process
                    child.restarts += int(child.attempts > 0)
                    child.started_at = self.clock()
                    child.backoff_s = 0
                    child.error = None
                    reader = Thread(target=self.capture_output, args=(name, process.stdout), daemon=True)
                    self.readers.append(reader)
                    reader.start()
                    self.log("service_started", service=name, pid=process.pid, restarts=child.restarts)
                child.attempts += 1
                changed = True
        self.readers = [reader for reader in self.readers if reader.is_alive()]
        if changed:
            self.write_state()

    def shutdown(self):
        self.stop.set()
        # Signal all owned children before waiting for any one of them.
        for child in self.children.values():
            child.backoff_s = 0
            if child.process is not None and child.process.poll() is None:
                try:
                    child.process.terminate()
                except ProcessLookupError:
                    pass
        deadline = self.clock() + self.shutdown_timeout
        for name, child in self.children.items():
            if child.process is None:
                continue
            try:
                code = child.process.wait(timeout=max(0, deadline - self.clock()))
            except subprocess.TimeoutExpired:
                self.log("service_stop_timeout", service=name)
                child.process.kill()
                code = child.process.wait()
            child.last_exit_code = code
            child.process = None
            child.backoff_s = 0
            self.log("service_stopped", service=name, exit_code=code)
        for reader in self.readers:
            reader.join(timeout=1)
        self.write_state()

    def run(self):
        lock_path = self.root / "state/supervisor.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a") as lock:
            # A second supervisor fails without reading state or touching children.
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            previous = {sig: signal.signal(sig, self.request_shutdown)
                        for sig in (signal.SIGTERM, signal.SIGINT)}
            try:
                self.write_state()
                while not self.stop.is_set():
                    self.step()
                    self.stop.wait(0.1)
            finally:
                try:
                    self.shutdown()
                finally:
                    for sig, handler in previous.items():
                        signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="default: SIGNALS_ROOT/config/services.toml")
    args = parser.parse_args()
    try:
        Supervisor(load_services(args.config)).run()
    except (OSError, ValueError) as exc:
        common.log_event("supervisor_error", log_name="supervisor", error=type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
