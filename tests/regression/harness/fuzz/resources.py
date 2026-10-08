"""CPU and memory sampling of a compose project's containers during a fuzz run."""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import threading
import time
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

from .recorder import percentile

DEFAULT_INTERVAL_S = 2.0
_CONTEXT_HOST = "{{.Endpoints.docker.Host}}"
_SERVICE_LABEL = "com.docker.compose.service"


class _UnixConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self._path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._path)


def docker_socket() -> str | None:
    host = os.environ.get("DOCKER_HOST")
    if not host:
        try:
            host = subprocess.run(
                ("docker", "context", "inspect", "--format", _CONTEXT_HOST),
                capture_output=True,
                text=True,
                timeout=10,
                check=True,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            host = "unix:///var/run/docker.sock"
    parsed = urlparse(host)
    return parsed.path if parsed.scheme == "unix" else None


class DockerApi:
    """The two Engine API calls the sampler needs, over the local socket."""

    def __init__(self, path: str, timeout: float = 10.0) -> None:
        self.path = path
        self.timeout = timeout

    def _get(self, url: str) -> Any:
        connection = _UnixConnection(self.path, self.timeout)
        try:
            connection.request("GET", url)
            response = connection.getresponse()
            body = response.read()
            if response.status != 200:
                raise RuntimeError(f"docker API {url}: HTTP {response.status}")
            return json.loads(body)
        finally:
            connection.close()

    def _network(self, action: str, network: str, container: str) -> bytes:
        connection = _UnixConnection(self.path, self.timeout)
        try:
            connection.request(
                "POST",
                f"/networks/{quote(network)}/{action}",
                body=json.dumps({"Container": container, "Force": True}),
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            body = response.read()
            if response.status == 200:
                return b""
            return f"HTTP {response.status} ".encode() + body[:200]
        finally:
            connection.close()

    def connect(self, network: str, container: str) -> None:
        """Attach a container to a network; already attached is fine."""
        error = self._network("connect", network, container)
        if error and b"already exists" not in error:
            raise RuntimeError(f"docker API connect {network}: {error.decode()}")

    def info(self) -> dict[str, Any]:
        return self._get("/info")

    def update_cpus(self, container: str, cpuset: str, cores: int) -> None:
        """Pin a running container to `cpuset` and cap it at `cores`."""
        connection = _UnixConnection(self.path, self.timeout)
        try:
            connection.request(
                "POST",
                f"/containers/{quote(container)}/update",
                body=json.dumps({"CpusetCpus": cpuset, "NanoCpus": cores * 10**9}),
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            body = response.read()
            if response.status != 200:
                raise RuntimeError(
                    f"docker API update {container[:12]}: HTTP {response.status} "
                    f"{body[:200].decode(errors='replace')}"
                )
        finally:
            connection.close()

    def disconnect(self, network: str, container: str) -> None:
        """Detach a container from a network; not attached is fine."""
        error = self._network("disconnect", network, container)
        if error and not error.startswith((b"HTTP 404", b"HTTP 403")):
            raise RuntimeError(f"docker API disconnect {network}: {error.decode()}")

    def containers(self, project: str) -> dict[str, str]:
        """Running container IDs of a compose project, keyed to their service."""
        filters = json.dumps({"label": [f"com.docker.compose.project={project}"]})
        listed = self._get(f"/containers/json?filters={quote(filters)}")
        return {
            item["Id"]: item["Labels"].get(_SERVICE_LABEL, item["Id"][:12])
            for item in listed
        }

    def images(self, project: str) -> dict[str, dict[str, str]]:
        """Image reference and short image ID per service of a compose project."""
        filters = json.dumps({"label": [f"com.docker.compose.project={project}"]})
        listed = self._get(f"/containers/json?filters={quote(filters)}")
        images = {}
        for item in listed:
            service = item["Labels"].get(_SERVICE_LABEL, item["Id"][:12])
            image_id = str(item.get("ImageID", ""))
            images[service] = {
                "image": str(item.get("Image", "")),
                "id": image_id.removeprefix("sha256:")[:12],
            }
        return dict(sorted(images.items()))

    def usage(self, container: str) -> tuple[int, int]:
        """Cumulative CPU nanoseconds and the working-set memory in bytes."""
        stats = self._get(f"/containers/{container}/stats?stream=false&one-shot=true")
        cpu = int(stats["cpu_stats"]["cpu_usage"]["total_usage"])
        memory = stats.get("memory_stats", {})
        detail = memory.get("stats", {})
        # Matches `docker stats`: page cache the kernel can drop is not counted.
        cache = detail.get("inactive_file", detail.get("total_inactive_file", 0))
        return cpu, max(0, int(memory.get("usage", 0)) - int(cache))


class ResourceSampler:
    """Samples every container of a compose project on a background thread.

    Services map to a role through `roles`; anything unlisted (object storage)
    is reported as `shared`. CPU comes from cumulative cgroup counters, so the
    totals are exact regardless of the sampling interval.
    """

    def __init__(
        self,
        api: DockerApi,
        project: str,
        roles: Mapping[str, str],
        path: Path,
        interval_s: float = DEFAULT_INTERVAL_S,
    ) -> None:
        self.api = api
        self.project = project
        self.roles = dict(roles)
        self.path = path
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name="fuzz-resources", daemon=True
        )
        self._last: dict[str, tuple[float, int]] = {}
        self._services: dict[str, str] = {}
        # Per role: (interval seconds, cores) and memory summed per sample.
        self._cores: dict[str, list[tuple[float, float]]] = defaultdict(list)
        self._memory: dict[str, list[int]] = defaultdict(list)
        self._service_peak: dict[str, int] = defaultdict(int)
        self._service_cpu: dict[str, float] = defaultdict(float)
        self._errors = 0
        self._started = 0.0

    def role(self, service: str) -> str:
        return self.roles.get(service, "shared")

    def start(self) -> None:
        self._started = time.monotonic()
        self._output = self.path.open("w", encoding="utf-8")
        self._sample()
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=30)
        self._sample()
        self._output.close()
        return self.summary()

    def _loop(self) -> None:
        while not self._stop.wait(self.interval_s):
            self._sample()

    def _sample(self) -> None:
        try:
            containers = self.api.containers(self.project)
        except Exception:
            self._errors += 1
            return
        now = time.monotonic()
        cores: dict[str, float] = defaultdict(float)
        memory: dict[str, int] = defaultdict(int)
        dt_by_role: dict[str, float] = {}
        services = []
        for container, service in containers.items():
            try:
                cpu_ns, memory_bytes = self.api.usage(container)
            except Exception:
                self._errors += 1
                continue
            role = self.role(service)
            self._services[container] = service
            previous = self._last.get(container)
            self._last[container] = (now, cpu_ns)
            memory[role] += memory_bytes
            self._service_peak[service] = max(self._service_peak[service], memory_bytes)
            sample: dict[str, Any] = {
                "t": round(now - self._started, 3),
                "service": service,
                "role": role,
                "cpu_ns": cpu_ns,
                "memory_bytes": memory_bytes,
            }
            if previous is not None and now > previous[0]:
                used_s = max(0, cpu_ns - previous[1]) / 1e9
                self._service_cpu[service] += used_s
                cores[role] += used_s / (now - previous[0])
                dt_by_role[role] = now - previous[0]
            services.append(sample)
        for sample in services:
            self._output.write(json.dumps(sample) + "\n")
        self._output.flush()
        for role, total in memory.items():
            self._memory[role].append(total)
        for role, value in cores.items():
            self._cores[role].append((dt_by_role[role], value))

    def summary(self) -> dict[str, Any]:
        elapsed = time.monotonic() - self._started
        roles: dict[str, Any] = {}
        for role in sorted(set(self._memory) | set(self._cores)):
            cpu_seconds = sum(
                self._service_cpu[service]
                for service in set(self._services.values())
                if self.role(service) == role
            )
            cores = [value for _, value in self._cores[role]]
            memory = self._memory[role]
            roles[role] = {
                "services": sorted(
                    {s for s in self._services.values() if self.role(s) == role}
                ),
                "cpu_seconds": round(cpu_seconds, 1),
                "cpu_cores": {
                    "mean": round(cpu_seconds / elapsed, 3) if elapsed else None,
                    "p95": _round(percentile(cores, 0.95)),
                    "max": _round(max(cores) if cores else None),
                },
                "memory_mib": {
                    "mean": _mib(sum(memory) / len(memory)) if memory else None,
                    "p95": _mib(percentile([float(m) for m in memory], 0.95)),
                    "max": _mib(max(memory) if memory else None),
                    "end": _mib(memory[-1] if memory else None),
                },
            }
        return {
            "interval_s": self.interval_s,
            "elapsed_s": round(elapsed, 1),
            "samples_failed": self._errors,
            "roles": roles,
            "service_peak_memory_mib": {
                service: _mib(value)
                for service, value in sorted(self._service_peak.items())
            },
            "service_cpu_seconds": {
                service: round(value, 1)
                for service, value in sorted(self._service_cpu.items())
            },
        }


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 3)


def _mib(value: float | None) -> float | None:
    return None if value is None else round(value / (1024 * 1024), 1)
