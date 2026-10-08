"""Where the harness reaches compose services from.

On the host, services are reached through their published ports. When the
harness itself runs in a container (`FUZZ_IN_NETWORK=1`, see
`tests/regression/products/runner`), it joins each product's compose network
and talks to services by name, so neither side pays the host port-forwarding
hop and latency is measured from inside the same network.
"""

from __future__ import annotations

import os
import socket

IN_NETWORK_ENV = "FUZZ_IN_NETWORK"


def in_network() -> bool:
    return os.environ.get(IN_NETWORK_ENV, "").strip().lower() not in (
        "",
        "0",
        "false",
        "no",
        "off",
    )


def service_url(service: str, port: int, host_port: int) -> str:
    """`http://<service>:<port>` inside the network, else the published port."""
    if in_network():
        return f"http://{service}:{port}"
    return f"http://127.0.0.1:{host_port}"


def minio_floor_url(host_port: int | None = None) -> dict[str, str]:
    """MinIO's liveness URL, when it is reachable from where the harness runs."""
    if in_network():
        return {"minio": "http://minio:9000/minio/health/live"}
    if host_port is not None:
        return {"minio": f"http://127.0.0.1:{host_port}/minio/health/live"}
    return {}


def _docker():
    from .resources import DockerApi, docker_socket

    path = docker_socket()
    if path is None:
        raise RuntimeError(f"{IN_NETWORK_ENV} requires the Docker socket")
    return DockerApi(path)


def join_project_network(project: str) -> None:
    """Attach this container to `<project>_default` once compose created it."""
    if in_network():
        _docker().connect(f"{project}_default", socket.gethostname())


def leave_project_network(project: str) -> None:
    if in_network():
        _docker().disconnect(f"{project}_default", socket.gethostname())


def network_mode() -> str:
    """Recorded in `run.json` so in-network and host runs are not compared."""
    return "compose-network" if in_network() else "host-ports"
