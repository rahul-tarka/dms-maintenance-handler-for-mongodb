"""
AWS DMS maintenance orchestrator for MongoDB source maintenance windows.

Triggered by EventBridge with event: {"action": "stop"} or {"action": "resume"}.
Uses resume-processing only on resume; validates CDC-capable migration types.
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Iterable

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Migration types that support CDC checkpoint resume (not full-load-only)
CDC_CAPABLE_TYPES = frozenset({"cdc", "full-load-and-cdc"})

@dataclass(frozen=True)
class Config:
    action: str
    batch_size: int
    inter_batch_delay_sec: float
    max_attempts: int
    base_backoff_sec: float
    max_backoff_sec: float
    jitter_ratio: float
    replication_instance_arns: tuple[str, ...]
    task_identifier_prefix: str | None
    task_arns_override: tuple[str, ...]
    ssm_parameter_arn_list: str | None
    require_cdc: bool
    dry_run: bool


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _load_config(event: dict[str, Any]) -> Config:
    action = (event.get("action") or os.environ.get("ACTION") or "").strip().lower()
    if action not in ("stop", "resume"):
        raise ValueError('event.action or env ACTION must be "stop" or "resume"')

    ri = os.environ.get("DMS_REPLICATION_INSTANCE_ARNS", "").strip()
    replication_instance_arns = tuple(a.strip() for a in ri.split(",") if a.strip())

    override = os.environ.get("DMS_TASK_ARNS", "").strip()
    task_arns_override = tuple(a.strip() for a in override.split(",") if a.strip())

    prefix = os.environ.get("DMS_TASK_IDENTIFIER_PREFIX", "").strip() or None
    ssm_param = os.environ.get("DMS_TASK_ARNS_SSM_PARAMETER", "").strip() or None

    return Config(
        action=action,
        batch_size=max(1, int(os.environ.get("BATCH_SIZE", "12"))),
        inter_batch_delay_sec=float(os.environ.get("INTER_BATCH_DELAY_SEC", "90")),
        max_attempts=max(1, int(os.environ.get("MAX_ATTEMPTS", "5"))),
        base_backoff_sec=float(os.environ.get("BASE_BACKOFF_SEC", "2")),
        max_backoff_sec=float(os.environ.get("MAX_BACKOFF_SEC", "60")),
        jitter_ratio=float(os.environ.get("JITTER_RATIO", "0.2")),
        replication_instance_arns=replication_instance_arns,
        task_identifier_prefix=prefix,
        task_arns_override=task_arns_override,
        ssm_parameter_arn_list=ssm_param,
        require_cdc=_env_bool("REQUIRE_CDC", True),
        dry_run=_env_bool("DRY_RUN", False),
    )


def _jitter_sleep(base: float, jitter_ratio: float) -> None:
    if base <= 0:
        return
    jitter = base * jitter_ratio
    delay = max(0.0, base + random.uniform(-jitter, jitter))
    time.sleep(delay)


def _retry_call(
    fn: Any,
    *,
    max_attempts: int,
    base_backoff: float,
    max_backoff: float,
    jitter_ratio: float,
    is_retryable: Any,
) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except ClientError as e:
            last_exc = e
            code = e.response.get("Error", {}).get("Code", "")
            if not is_retryable(code, e) or attempt == max_attempts:
                raise
            backoff = min(max_backoff, base_backoff * (2 ** (attempt - 1)))
            logger.warning(
                "Retryable API error (attempt %s/%s): %s — sleeping %.2fs",
                attempt,
                max_attempts,
                code,
                backoff,
            )
            _jitter_sleep(backoff, jitter_ratio)
    raise last_exc  # pragma: no cover


def _is_retryable_dms(code: str, _exc: ClientError) -> bool:
    return code in (
        "ThrottlingException",
        "TooManyRequestsException",
        "LimitExceededException",
        "ReplicationSubnetGroupFault",
        "ServiceUnavailable",
    )


def _get_ssm_arn_list(parameter_name: str) -> list[str]:
    ssm = boto3.client("ssm")
    resp = ssm.get_parameter(Name=parameter_name)
    raw = resp["Parameter"]["Value"].strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except json.JSONDecodeError:
        pass
    return [a.strip() for a in raw.split(",") if a.strip()]


def _list_task_arns_for_instance(dms: Any, replication_instance_arn: str) -> list[str]:
    arns: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {
            "MaxRecords": 100,
            "Filters": [
                {"Name": "replication-instance-arn", "Values": [replication_instance_arn]}
            ],
        }
        if token:
            kwargs["Marker"] = token

        def _page() -> Any:
            return dms.list_replication_tasks(**kwargs)

        resp = _retry_call(
            _page,
            max_attempts=5,
            base_backoff=1.0,
            max_backoff=30.0,
            jitter_ratio=0.2,
            is_retryable=_is_retryable_dms,
        )
        for t in resp.get("ReplicationTasks", []):
            arn = t.get("ReplicationTaskArn")
            if arn:
                arns.append(arn)
        token = resp.get("Marker")
        if not token:
            break
    return arns


def _discover_task_arns(cfg: Config) -> list[str]:
    if cfg.task_arns_override:
        return list(cfg.task_arns_override)
    if cfg.ssm_parameter_arn_list:
        return _get_ssm_arn_list(cfg.ssm_parameter_arn_list)
    if not cfg.replication_instance_arns:
        raise ValueError(
            "Set DMS_REPLICATION_INSTANCE_ARNS, DMS_TASK_ARNS, or DMS_TASK_ARNS_SSM_PARAMETER"
        )
    dms = boto3.client("dms")
    seen: set[str] = set()
    ordered: list[str] = []
    for ri in cfg.replication_instance_arns:
        for arn in _list_task_arns_for_instance(dms, ri):
            if arn not in seen:
                seen.add(arn)
                ordered.append(arn)
    return ordered


def _describe_tasks(
    dms: Any, arns: list[str], *, max_attempts: int
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    # DescribeReplicationTasks accepts MaxRecords 20–100; batch ARNs via filter
    for i in range(0, len(arns), 20):
        chunk = arns[i : i + 20]

        def _desc() -> Any:
            return dms.describe_replication_tasks(
                Filters=[{"Name": "replication-task-arn", "Values": chunk}],
                MaxRecords=100,
            )

        resp = _retry_call(
            _desc,
            max_attempts=max_attempts,
            base_backoff=1.0,
            max_backoff=30.0,
            jitter_ratio=0.2,
            is_retryable=_is_retryable_dms,
        )
        for t in resp.get("ReplicationTasks", []):
            a = t.get("ReplicationTaskArn")
            if a:
                out[a] = t
    return out


def _filter_by_prefix(
    arns: list[str], details: dict[str, dict[str, Any]], prefix: str | None
) -> list[str]:
    if not prefix:
        return arns
    kept: list[str] = []
    for arn in arns:
        t = details.get(arn) or {}
        ident = (t.get("ReplicationTaskIdentifier") or "").strip()
        if ident.startswith(prefix):
            kept.append(arn)
        else:
            logger.info("Skipping task (prefix mismatch): %s %s", ident, arn)
    return kept


def _stop_task(
    dms: Any, arn: str, cfg: Config, task: dict[str, Any]
) -> dict[str, Any]:
    meta = {"arn": arn, "op": "stop", "ok": False, "note": ""}
    status = (task.get("Status") or "").lower()

    if status in ("stopped", "ready"):
        meta["ok"] = True
        meta["note"] = f"already_{status}"
        logger.info(
            "Stop skipped (already idle): %s status=%s",
            task.get("ReplicationTaskIdentifier", arn),
            status,
        )
        return meta

    def _call() -> Any:
        return dms.stop_replication_task(ReplicationTaskArn=arn)

    try:
        if cfg.dry_run:
            meta["ok"] = True
            meta["note"] = "dry_run"
            return meta
        _retry_call(
            _call,
            max_attempts=cfg.max_attempts,
            base_backoff=cfg.base_backoff_sec,
            max_backoff=cfg.max_backoff_sec,
            jitter_ratio=cfg.jitter_ratio,
            is_retryable=_is_retryable_dms,
        )
        meta["ok"] = True
        meta["note"] = "stopped"
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        # Already stopped / not running — treat as success for idempotency
        if code in ("InvalidResourceStateFault", "ResourceNotFoundFault"):
            logger.info("Stop noop for %s: %s", arn, code)
            meta["ok"] = True
            meta["note"] = code
        else:
            meta["note"] = str(e)
            logger.exception("Stop failed for %s", arn)
    return meta


def _resume_task(
    dms: Any, arn: str, cfg: Config, task: dict[str, Any]
) -> dict[str, Any]:
    meta = {"arn": arn, "op": "resume", "ok": False, "note": ""}
    status = (task.get("Status") or "").lower()

    if status == "running":
        meta["ok"] = True
        meta["note"] = "already_running"
        logger.info(
            "Resume skipped (already running): %s",
            task.get("ReplicationTaskIdentifier", arn),
        )
        return meta

    def _call() -> Any:
        return dms.start_replication_task(
            ReplicationTaskArn=arn,
            StartReplicationTaskType="resume-processing",
        )

    try:
        if cfg.dry_run:
            meta["ok"] = True
            meta["note"] = "dry_run"
            return meta
        _retry_call(
            _call,
            max_attempts=cfg.max_attempts,
            base_backoff=cfg.base_backoff_sec,
            max_backoff=cfg.max_backoff_sec,
            jitter_ratio=cfg.jitter_ratio,
            is_retryable=_is_retryable_dms,
        )
        meta["ok"] = True
        meta["note"] = "resume-processing"
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "InvalidResourceStateFault":
            logger.info("Resume noop for %s: %s", arn, e)
            meta["ok"] = True
            meta["note"] = "invalid_state"
        else:
            meta["note"] = str(e)
            logger.exception("Resume failed for %s", arn)
    return meta


def _chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _emit_metric(namespace: str, name: str, value: float, dimensions: dict[str, str]) -> None:
    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    "MetricName": name,
                    "Value": value,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": k, "Value": v} for k, v in dimensions.items()
                    ],
                }
            ],
        )
    except ClientError:
        logger.exception("Failed to emit metric %s", name)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    cfg = _load_config(event or {})
    namespace = os.environ.get("METRICS_NAMESPACE", "DMS/MaintenanceOrchestrator")

    logger.info(
        "Starting DMS maintenance action=%s batch_size=%s dry_run=%s",
        cfg.action,
        cfg.batch_size,
        cfg.dry_run,
    )

    arns = _discover_task_arns(cfg)
    dms = boto3.client("dms")
    details = _describe_tasks(dms, arns, max_attempts=cfg.max_attempts)
    arns = [a for a in arns if a in details]
    arns = _filter_by_prefix(arns, details, cfg.task_identifier_prefix)

    if cfg.action == "resume" and cfg.require_cdc:
        non_cdc_skipped: list[str] = []
        kept: list[str] = []
        for arn in arns:
            t = details.get(arn, {})
            mtype = (t.get("MigrationType") or "").lower().strip()
            if mtype not in CDC_CAPABLE_TYPES:
                ident = t.get("ReplicationTaskIdentifier", arn)
                logger.error(
                    "Skipping resume for non-CDC task %s: MigrationType=%s",
                    ident,
                    t.get("MigrationType"),
                )
                non_cdc_skipped.append(arn)
            else:
                kept.append(arn)
        arns = kept
        if non_cdc_skipped:
            _emit_metric(
                namespace,
                "TasksSkippedNonCdc",
                float(len(non_cdc_skipped)),
                {"Action": cfg.action},
            )

    results: list[dict[str, Any]] = []
    batch_index = 0
    total_batches = math.ceil(len(arns) / cfg.batch_size) if arns else 0
    for batch in _chunks(arns, cfg.batch_size):
        batch_index += 1
        logger.info("Processing batch %s size=%s", batch_index, len(batch))
        for arn in batch:
            task = details.get(arn, {})
            if cfg.action == "stop":
                results.append(_stop_task(dms, arn, cfg, task))
            else:
                results.append(_resume_task(dms, arn, cfg, task))

        if batch_index < total_batches:
            logger.info(
                "Inter-batch delay %.2fs (replication instance breathing room)",
                cfg.inter_batch_delay_sec,
            )
            time.sleep(cfg.inter_batch_delay_sec)

    ok = sum(1 for r in results if r.get("ok"))
    failed = [r for r in results if not r.get("ok")]

    _emit_metric(namespace, "TasksProcessed", float(len(results)), {"Action": cfg.action})
    _emit_metric(namespace, "TasksSucceeded", float(ok), {"Action": cfg.action})
    _emit_metric(namespace, "TasksFailed", float(len(failed)), {"Action": cfg.action})

    summary = {
        "action": cfg.action,
        "total": len(results),
        "succeeded": ok,
        "failed": len(failed),
        "failures": failed,
    }
    logger.info("Summary: %s", json.dumps(summary))
    if failed:
        # Let EventBridge/Lambda mark failure for alarms; partial failure is still actionable
        raise RuntimeError(
            f"DMS maintenance {cfg.action} completed with {len(failed)} failure(s)"
        )
    return summary
