import io
import json
import signal
import subprocess
import sys
from threading import Event

import pytest

import common
import supervisor
from conftest import REPO


class FakeProcess:
    def __init__(self, pid):
        self.pid = pid
        self.returncode = None
        self.stdout = io.StringIO("child diagnostic\n")
        self.terminated = False
        self.killed = False
        self.waited = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = 0

    def wait(self, timeout=None):
        self.waited = True
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9


@pytest.fixture
def rig(tmp_path):
    now = [100.0]
    launches = []

    def launch(command, **kwargs):
        process = FakeProcess(10000 + len(launches))
        launches.append((process, command, kwargs))
        return process

    services = [supervisor.Service("ingest_worker", True, (sys.executable, "fake.py")),
                supervisor.Service("webhook_server", False, (sys.executable, "disabled.py"))]
    instance = supervisor.Supervisor(services, root=tmp_path, clock=lambda: now[0], popen=launch)
    yield instance, now, launches
    instance.shutdown()


def state(instance):
    return json.loads((instance.root / "state/supervisor.json").read_text())


def test_config_enabled_flags_commands_and_defaults(tmp_path, monkeypatch):
    bundled = supervisor.load_services(REPO / "config/services.toml")
    assert {s.name for s in bundled} == supervisor.SERVICE_NAMES
    assert all(s.enabled is False for s in bundled)
    path = tmp_path / "config/services.toml"
    path.parent.mkdir()
    path.write_text('[services.ingest_worker]\nenabled=true\nargs=["--example", "a b"]\n'
                    '[services.webhook_server]\nenabled=false\n')
    monkeypatch.setattr(common, "ROOT", tmp_path)
    services = supervisor.load_services(python="/some/python", project_root=tmp_path)
    assert services[0].enabled is True and services[1].enabled is False
    assert services[0].command == ("/some/python", "-u", str(tmp_path / "bin/ingest_worker.py"),
                                    "--example", "a b")
    with pytest.raises(FileNotFoundError):
        supervisor.load_services(root=tmp_path / "missing")


@pytest.mark.parametrize("config", [
    '', '[services]', '[services.unknown]\nenabled=true',
    '[services.ingest_worker]\nenabled="false"',
    '[services.ingest_worker]\nenabled=1',
    '[services.ingest_worker]\nargs=[]',
    '[services.ingest_worker]\nenabled=true\nargs="bad"',
    '[services.ingest_worker]\nenabled=true\nargs=[1]',
    '[services.ingest_worker]\nenabled=true\ncommand="bad"',
])
def test_invalid_config_fails_closed(tmp_path, config):
    path = tmp_path / "services.toml"
    path.write_text(config)
    with pytest.raises(ValueError):
        supervisor.load_services(path)


def test_gmgn_one_shot_cannot_be_enabled_under_supervisor(tmp_path):
    path = tmp_path / "services.toml"
    path.write_text("[services.gmgn_adapter]\nenabled=true\n")
    with pytest.raises(ValueError, match="one-shot"):
        supervisor.load_services(path)


def test_state_backoff_restart_cap_and_launch_environment(rig):
    instance, now, launches = rig
    instance.step()
    process, command, kwargs = launches[0]
    assert len(launches) == 1
    assert command == instance.children["ingest_worker"].service.command
    assert kwargs["cwd"] == REPO
    assert kwargs["env"]["SIGNALS_ROOT"] == str(instance.root)
    assert kwargs["env"]["PYTHONPATH"].split(__import__('os').pathsep)[:2] == [str(REPO / "bin"), str(REPO)]
    assert kwargs["start_new_session"] is True
    assert kwargs["stderr"] == subprocess.STDOUT and kwargs["stdout"] == subprocess.PIPE
    row = state(instance)["services"]["ingest_worker"]
    assert row == dict(enabled=True, pid=process.pid, restarts=0, last_exit_code=None,
                       backoff_s=0, error=None)
    assert state(instance)["services"]["webhook_server"]["pid"] is None
    for restarts, delay in enumerate([1, 2, 4, 8, 16, 32, 60, 60], 1):
        launches[-1][0].returncode = -9 if restarts == 1 else 0
        instance.step()
        row = state(instance)["services"]["ingest_worker"]
        assert row["pid"] is None and row["backoff_s"] == delay
        assert row["last_exit_code"] == (-9 if restarts == 1 else 0)
        assert row["restarts"] == restarts - 1
        now[0] += delay - .01
        instance.step()
        assert len(launches) == restarts
        now[0] += .02
        instance.step()
        assert len(launches) == restarts + 1
        row = state(instance)["services"]["ingest_worker"]
        assert row["restarts"] == restarts and row["pid"] == launches[-1][0].pid
    # A minute of stable uptime resets the consecutive-failure backoff.
    now[0] += 60
    launches[-1][0].returncode = 7
    instance.step()
    assert state(instance)["services"]["ingest_worker"]["backoff_s"] == 1


def test_spawn_failure_backs_off_without_a_pid(rig):
    instance, now, launches = rig
    launch = instance.popen
    instance.popen = lambda *a, **k: (_ for _ in ()).throw(OSError("unavailable"))
    instance.step()
    row = state(instance)["services"]["ingest_worker"]
    assert row["error"] == "OSError" and row["pid"] is None
    assert row["last_exit_code"] is None and row["backoff_s"] == 1
    instance.step()
    assert instance.children["ingest_worker"].attempts == 1
    now[0] += 1
    instance.popen = launch
    instance.step()
    assert len(launches) == 1
    assert state(instance)["services"]["ingest_worker"]["restarts"] == 1


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_signal_handler_graceful_shutdown_without_restarts(rig, monkeypatch, sig):
    instance, now, launches = rig
    handlers = {}
    registrations = []

    def install(signum, handler):
        registrations.append((signum, handler))
        old = handlers.get(signum, signal.SIG_DFL)
        handlers[signum] = handler
        return old

    class SignalStop(Event):
        def wait(self, timeout=None):
            handlers[sig](sig, None)
            return True

    monkeypatch.setattr(supervisor.signal, "signal", install)
    instance.stop = SignalStop()
    # Run the loop entirely with fake children and a simulated delivered signal.
    instance.run()
    assert len(launches) == 1
    child = launches[0][0]
    assert child.terminated and child.waited and not child.killed
    assert state(instance)["stopping"] is True
    row = state(instance)["services"]["ingest_worker"]
    assert row["pid"] is None and row["last_exit_code"] == 0 and row["restarts"] == 0
    now[0] += 1000
    instance.step()
    assert len(launches) == 1
    assert len(registrations) == 4
    assert all(handler == signal.SIG_DFL for handler in handlers.values())


def test_shutdown_during_backoff_does_not_restart(rig):
    instance, now, launches = rig
    instance.step()
    launches[0][0].returncode = 3
    instance.step()
    instance.request_shutdown()
    now[0] += 1000
    instance.step()
    instance.shutdown()
    assert len(launches) == 1 and not launches[0][0].terminated
    assert state(instance)["services"]["ingest_worker"]["last_exit_code"] == 3


def test_shutdown_terminates_all_before_wait_and_escalates_only_owned(rig):
    instance, _, launches = rig
    instance.children["webhook_server"] = supervisor.Child(
        supervisor.Service("webhook_server", True, ("fake",)))
    instance.step()
    first, second = [entry[0] for entry in launches]

    def wait(timeout=None):
        assert first.terminated and second.terminated
        if not first.killed:
            raise subprocess.TimeoutExpired("fake", timeout)
        return -9

    first.wait = wait
    instance.shutdown()
    assert first.killed and not second.killed
    assert state(instance)["services"]["ingest_worker"]["last_exit_code"] == -9


def test_stale_state_never_adopted_and_lock_fails_closed(tmp_path):
    services = [supervisor.Service("ingest_worker", False, ("unused",))]
    instance = supervisor.Supervisor(services, root=tmp_path,
                                     popen=lambda *a, **k: pytest.fail("must not launch"))
    stale = {"services": {"ingest_worker": {"pid": 805754}, "binance_smy_ws": {"pid": 806790}}}
    common.atomic_write(tmp_path / "state/supervisor.json", json.dumps(stale))
    with common.file_lock(tmp_path / "state/supervisor.lock"):
        with pytest.raises(BlockingIOError):
            instance.run()
    assert state(instance) == stale
    instance.shutdown()
    assert state(instance)["services"]["ingest_worker"]["pid"] is None


def test_output_and_rotation_leave_event_journal_unchanged(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(common, "ROOT", tmp_path)
    assert common.LOG_MAX_BYTES == 10 * 1024 * 1024
    assert common.LOG_BACKUP_COUNT == 5
    common.log_event("ws_message", sample=1)
    journal = (tmp_path / "state/events.jsonl").read_bytes()
    monkeypatch.setattr(common, "LOG_MAX_BYTES", 240)
    for n in range(20):
        common.log_event("service_message", log_name="binance_smy_ws", message="x" * 80, n=n)
    paths = sorted((tmp_path / "state").glob("binance_smy_ws.log*"))
    logs = [path for path in paths if path.suffix != ".lock"]
    assert len(logs) == 6
    assert {p.name for p in logs} == {"binance_smy_ws.log", *[f"binance_smy_ws.log.{i}" for i in range(1, 6)]}
    for path in logs:
        assert all(json.loads(line)["event"] == "service_message" for line in path.read_text().splitlines())
    assert json.loads((tmp_path / "state/binance_smy_ws.log").read_text())["n"] == 19
    instance = supervisor.Supervisor([], root=tmp_path)
    instance.capture_output("ingest_worker", io.StringIO("diagnostic\n"))
    output = json.loads((tmp_path / "state/supervisor.log").read_text())
    assert output["event"] == "service_output" and output["service"] == "ingest_worker"
    assert output["message"] == "diagnostic"
    assert (tmp_path / "state/events.jsonl").read_bytes() == journal
    assert not list(tmp_path.rglob("*.out"))
    assert capsys.readouterr().out == ""


def test_ws_log_no_longer_prints_or_double_writes(tmp_path, monkeypatch, capsys):
    import binance_smy_ws
    monkeypatch.setattr(common, "ROOT", tmp_path)
    binance_smy_ws.log("subscription ack")
    assert capsys.readouterr().out == ""
    row = json.loads((tmp_path / "state/binance_smy_ws.log").read_text())
    assert row["message"] == "subscription ack"
    assert not (tmp_path / "state/events.jsonl").exists()
