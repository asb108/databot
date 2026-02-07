"""Spark connector — manage Spark jobs via Livy REST, YARN, or Kubernetes.

Supports three deployment modes:
  * ``livy``  — Apache Livy REST API (interactive sessions + batch jobs)
  * ``yarn``  — YARN ResourceManager REST API (application status, logs, kill)
  * ``k8s``   — Kubernetes Spark Operator API (SparkApplication CRDs)
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from databot.connectors.base import ConnectorResult, ConnectorStatus, ConnectorType
from databot.connectors.rest_connector import RESTConnector


class SparkConnector(RESTConnector):
    """Connector for Apache Spark job management."""

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        super().__init__(name, config)
        self._mode = self._config.get("mode", "livy")  # livy | yarn | k8s
        self._default_conf: dict[str, str] = self._config.get("default_conf", {})

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.PROCESSING

    def capabilities(self) -> list[str]:
        base = ["submit_batch", "batch_status", "batch_logs", "kill_batch", "list_batches"]
        if self._mode == "livy":
            base += ["create_session", "run_statement", "session_status", "list_sessions"]
        return base

    async def health_check(self) -> ConnectorStatus:
        if not self._base_url:
            return ConnectorStatus.NOT_CONFIGURED
        try:
            if self._mode == "livy":
                result = await self.request("GET", "/batches", params={"from": 0, "size": 1})
            elif self._mode == "yarn":
                result = await self.request("GET", "/ws/v1/cluster/info")
            else:
                result = await self.request("GET", "/apis/sparkoperator.k8s.io/v1beta2/sparkapplications")
            return ConnectorStatus.HEALTHY if result.success else ConnectorStatus.DEGRADED
        except Exception:
            return ConnectorStatus.UNREACHABLE

    # ------------------------------------------------------------------
    # Operations — Livy Batch API
    # ------------------------------------------------------------------

    async def _op_submit_batch(
        self,
        file: str,
        class_name: str = "",
        args: list[str] | None = None,
        conf: dict[str, str] | None = None,
        name: str = "",
        **kwargs: Any,
    ) -> ConnectorResult:
        """Submit a Spark batch job."""
        if self._mode == "livy":
            return await self._livy_submit_batch(file, class_name, args, conf, name)
        elif self._mode == "k8s":
            return await self._k8s_submit(file, class_name, args, conf, name)
        return ConnectorResult(success=False, error=f"submit_batch not supported in {self._mode} mode")

    async def _op_batch_status(self, batch_id: str = "", app_id: str = "", **kwargs: Any) -> ConnectorResult:
        """Get batch job status."""
        if self._mode == "livy":
            return await self.request("GET", f"/batches/{batch_id}")
        elif self._mode == "yarn":
            return await self.request("GET", f"/ws/v1/cluster/apps/{app_id}")
        elif self._mode == "k8s":
            ns = self._config.get("namespace", "default")
            return await self.request("GET", f"/apis/sparkoperator.k8s.io/v1beta2/namespaces/{ns}/sparkapplications/{app_id}")
        return ConnectorResult(success=False, error="Unknown mode")

    async def _op_batch_logs(self, batch_id: str = "", app_id: str = "", **kwargs: Any) -> ConnectorResult:
        """Get batch job logs."""
        if self._mode == "livy":
            result = await self.request("GET", f"/batches/{batch_id}/log", params={"from": 0, "size": 200})
            if result.success and isinstance(result.data, dict):
                log_lines = result.data.get("log", [])
                return ConnectorResult(data="\n".join(log_lines))
            return result
        elif self._mode == "yarn":
            return await self.request("GET", f"/ws/v1/cluster/apps/{app_id}/appattempts")
        return ConnectorResult(success=False, error="Logs not available in this mode")

    async def _op_kill_batch(self, batch_id: str = "", app_id: str = "", **kwargs: Any) -> ConnectorResult:
        """Kill a running batch job."""
        if self._mode == "livy":
            return await self.request("DELETE", f"/batches/{batch_id}")
        elif self._mode == "yarn":
            return await self.request("PUT", f"/ws/v1/cluster/apps/{app_id}/state", json={"state": "KILLED"})
        elif self._mode == "k8s":
            ns = self._config.get("namespace", "default")
            return await self.request("DELETE", f"/apis/sparkoperator.k8s.io/v1beta2/namespaces/{ns}/sparkapplications/{app_id}")
        return ConnectorResult(success=False, error="Unknown mode")

    async def _op_list_batches(self, limit: int = 20, **kwargs: Any) -> ConnectorResult:
        """List batch jobs."""
        if self._mode == "livy":
            result = await self.request("GET", "/batches", params={"from": 0, "size": limit})
            if result.success and isinstance(result.data, dict):
                sessions = result.data.get("sessions", [])
                columns = ["id", "appId", "state", "appInfo"]
                rows = [
                    [s.get("id"), s.get("appId", ""), s.get("state", ""), str(s.get("appInfo", {}))]
                    for s in sessions
                ]
                return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))
            return result
        elif self._mode == "yarn":
            result = await self.request("GET", "/ws/v1/cluster/apps", params={"limit": str(limit)})
            if result.success and isinstance(result.data, dict):
                apps = result.data.get("apps", {}).get("app", [])
                columns = ["id", "name", "state", "finalStatus", "applicationType"]
                rows = [
                    [a.get("id"), a.get("name"), a.get("state"), a.get("finalStatus"), a.get("applicationType")]
                    for a in apps
                ]
                return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))
            return result
        return ConnectorResult(success=False, error="list_batches not supported in this mode")

    # ------------------------------------------------------------------
    # Operations — Livy Interactive Sessions
    # ------------------------------------------------------------------

    async def _op_create_session(
        self,
        kind: str = "pyspark",
        conf: dict[str, str] | None = None,
        name: str = "",
        **kwargs: Any,
    ) -> ConnectorResult:
        """Create an interactive Spark session (Livy only)."""
        if self._mode != "livy":
            return ConnectorResult(success=False, error="Interactive sessions only available in livy mode")

        body: dict[str, Any] = {"kind": kind}
        merged_conf = dict(self._default_conf)
        if conf:
            merged_conf.update(conf)
        if merged_conf:
            body["conf"] = merged_conf
        if name:
            body["name"] = name

        return await self.request("POST", "/sessions", json=body)

    async def _op_run_statement(
        self,
        session_id: str,
        code: str,
        kind: str = "",
        **kwargs: Any,
    ) -> ConnectorResult:
        """Execute code in an interactive Spark session (Livy only)."""
        if self._mode != "livy":
            return ConnectorResult(success=False, error="Statements only available in livy mode")

        body: dict[str, Any] = {"code": code}
        if kind:
            body["kind"] = kind

        # Submit the statement
        submit_result = await self.request("POST", f"/sessions/{session_id}/statements", json=body)
        if not submit_result.success:
            return submit_result

        # Poll for completion
        stmt_id = submit_result.data.get("id") if isinstance(submit_result.data, dict) else None
        if stmt_id is None:
            return submit_result

        import asyncio
        for _ in range(60):  # Max 60 polls (~2 min)
            await asyncio.sleep(2)
            status_result = await self.request("GET", f"/sessions/{session_id}/statements/{stmt_id}")
            if not status_result.success:
                return status_result
            state = status_result.data.get("state", "") if isinstance(status_result.data, dict) else ""
            if state in ("available", "error", "cancelled"):
                output = status_result.data.get("output", {}) if isinstance(status_result.data, dict) else {}
                if output.get("status") == "error":
                    return ConnectorResult(
                        success=False,
                        error=f"{output.get('ename', 'Error')}: {output.get('evalue', '')}",
                    )
                return ConnectorResult(data=output.get("data", {}).get("text/plain", str(output)))

        return ConnectorResult(success=False, error="Statement timed out after 2 minutes")

    async def _op_session_status(self, session_id: str, **kwargs: Any) -> ConnectorResult:
        """Get interactive session status."""
        return await self.request("GET", f"/sessions/{session_id}")

    async def _op_list_sessions(self, limit: int = 20, **kwargs: Any) -> ConnectorResult:
        """List active interactive sessions."""
        result = await self.request("GET", "/sessions", params={"from": 0, "size": limit})
        if result.success and isinstance(result.data, dict):
            sessions = result.data.get("sessions", [])
            columns = ["id", "kind", "state", "appId"]
            rows = [
                [s.get("id"), s.get("kind"), s.get("state"), s.get("appId", "")]
                for s in sessions
            ]
            return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))
        return result

    # ------------------------------------------------------------------
    # Livy batch submission
    # ------------------------------------------------------------------

    async def _livy_submit_batch(
        self,
        file: str,
        class_name: str,
        args: list[str] | None,
        conf: dict[str, str] | None,
        name: str,
    ) -> ConnectorResult:
        body: dict[str, Any] = {"file": file}
        if class_name:
            body["className"] = class_name
        if args:
            body["args"] = args
        if name:
            body["name"] = name

        merged_conf = dict(self._default_conf)
        if conf:
            merged_conf.update(conf)
        if merged_conf:
            body["conf"] = merged_conf

        return await self.request("POST", "/batches", json=body)

    # ------------------------------------------------------------------
    # K8s Spark Operator submission
    # ------------------------------------------------------------------

    async def _k8s_submit(
        self,
        file: str,
        class_name: str,
        args: list[str] | None,
        conf: dict[str, str] | None,
        name: str,
    ) -> ConnectorResult:
        ns = self._config.get("namespace", "default")
        image = self._config.get("image", "spark:latest")
        spark_version = self._config.get("spark_version", "3.5.0")

        manifest: dict[str, Any] = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": name or "databot-spark-job", "namespace": ns},
            "spec": {
                "type": "Python" if file.endswith(".py") else "Scala",
                "mode": "cluster",
                "image": image,
                "sparkVersion": spark_version,
                "mainApplicationFile": file,
                "mainClass": class_name or None,
                "arguments": args or [],
                "sparkConf": conf or {},
                "driver": self._config.get("driver_spec", {"cores": 1, "memory": "1g"}),
                "executor": self._config.get("executor_spec", {"cores": 1, "instances": 2, "memory": "1g"}),
            },
        }

        return await self.request(
            "POST",
            f"/apis/sparkoperator.k8s.io/v1beta2/namespaces/{ns}/sparkapplications",
            json=manifest,
        )
