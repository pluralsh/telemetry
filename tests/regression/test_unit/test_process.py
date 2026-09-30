from pathlib import Path

import harness.compose as compose_module
import pytest
from harness.compose import ComposeProject
from harness.process import CommandFailed, CommandResult, run


def test_run_captures_output_and_reports_failure() -> None:
    result = run(("python", "-c", "print('ok')"))
    assert result.stdout == "ok\n"
    with pytest.raises(CommandFailed, match="exited with 7"):
        run(("python", "-c", "raise SystemExit(7)"))


def test_compose_context_starts_waits_and_always_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[tuple[str, ...]] = []
    readiness: list[str] = []

    def fake_run(args: tuple[str, ...], **_: object) -> CommandResult:
        commands.append(tuple(args))
        return CommandResult(tuple(args), "", "")

    monkeypatch.setattr(compose_module, "run", fake_run)
    monkeypatch.setattr(compose_module, "wait_http", lambda url: readiness.append(url))
    project = ComposeProject(
        Path("/tmp/product/docker-compose.yml"),
        "unit",
        readiness_urls=("http://service/ready",),
        services=("service",),
    )
    with pytest.raises(RuntimeError, match="boom"), project:
        raise RuntimeError("boom")

    flattened = [" ".join(command) for command in commands]
    assert any(
        "up --detach --remove-orphans --build service" in item for item in flattened
    )
    assert any("logs --no-color" in item for item in flattened)
    assert any("down --volumes --remove-orphans" in item for item in flattened)
    assert readiness == ["http://service/ready"]


def test_compose_skips_build_when_images_are_prebuilt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[str] = []

    def fake_run(args: tuple[str, ...], **_: object) -> CommandResult:
        commands.append(" ".join(args))
        return CommandResult(tuple(args), "", "")

    monkeypatch.setattr(compose_module, "run", fake_run)
    monkeypatch.setenv("REGRESSION_BUILD", "0")
    project = ComposeProject(Path("/tmp/product/docker-compose.yml"), "unit")
    project.up("reader")
    assert commands[-1].endswith("up --detach --remove-orphans reader")
