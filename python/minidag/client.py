"""Python client for the minidag HTTP server."""

from dataclasses import dataclass, field

import requests


@dataclass
class NodeMetric:
    """Per-node execution metric returned by the server."""

    name: str
    duration_us: int
    skipped: bool = False
    timed_out: bool = False


@dataclass
class DagRunResult:
    """Result of a DAG execution."""

    dag: str
    result: dict | None
    total_us: int
    metrics: list[NodeMetric] = field(default_factory=list)


class MinidagClient:
    """Client for the minidag REST API.

    Args:
        base_url: Server base URL (default: http://localhost:8080).
        timeout: Request timeout in seconds (default: 30).
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        timeout: float = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def health(self) -> dict:
        """Check server health.

        Returns:
            dict with ``{"status": "ok"}`` on success.
        """
        resp = requests.get(
            f"{self.base_url}/api/v1/health", timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()

    def list_dags(self) -> list[str]:
        """List available DAG names.

        Returns:
            List of DAG name strings.
        """
        resp = requests.get(
            f"{self.base_url}/api/v1/dags", timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()["dags"]

    def run_dag(
        self,
        name: str,
        *,
        uid: int,
        query: str,
        timeout_ms: int = 2000,
    ) -> DagRunResult:
        """Execute a DAG and return the result.

        Args:
            name: DAG name (e.g. "search", "recommend").
            uid: User ID for the request.
            query: Query string for the request.
            timeout_ms: Server-side execution timeout in milliseconds.

        Returns:
            DagRunResult with the execution output and metrics.

        Raises:
            requests.HTTPError: On 4xx/5xx responses.
        """
        payload = {
            "request": {"uid": uid, "query": query},
            "timeout_ms": timeout_ms,
        }
        resp = requests.post(
            f"{self.base_url}/api/v1/dags/{name}/run",
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()

        metrics = [
            NodeMetric(
                name=m["name"],
                duration_us=m["duration_us"],
                skipped=m.get("skipped", False),
                timed_out=m.get("timed_out", False),
            )
            for m in data.get("metrics", [])
        ]

        return DagRunResult(
            dag=data["dag"],
            result=data.get("result"),
            total_us=data["total_us"],
            metrics=metrics,
        )
