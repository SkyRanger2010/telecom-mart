"""SQLGuard: проверка SQL и пайплайн NL2SQL после ответа LLM."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from nl2sql_rules import GuardAssistantIn, enforce_limit_sql, guard_assistant_response, validate_readonly_clickhouse_sql

SERVICE = "sqlguard"

app = FastAPI(
    title="telecom-mart sqlguard-api",
    version="1.1.0",
    description=(
        "Защита SQL: для dialect=clickhouse — правила NL2SQL (только SELECT, KPI-витрины). "
        "POST /api/v1/nl2sql/guard-assistant: сырой ответ модели; опционально materialize_table "
        "— SQL пересоздания dashboard_query_stub для дашборда по запросу."
    ),
)


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "service": SERVICE}


@app.get("/api/v1/info")
def info() -> dict[str, Any]:
    return {"service": SERVICE, "time": datetime.now(tz=UTC).isoformat()}


class SqlValidateIn(BaseModel):
    sql: str = ""
    dialect: str | None = None


@app.post("/api/sql/validate")
def sql_validate(body: SqlValidateIn) -> dict[str, Any]:
    if (body.dialect or "").strip().lower() != "clickhouse":
        return {
            "valid": True,
            "issues": [],
            "normalized_sql": body.sql.strip()[:500] or None,
            "clickhouse_nl2sql_rules": False,
        }
    ok, msg = validate_readonly_clickhouse_sql(body.sql)
    issues: list[str] = [] if ok else [msg or "ошибка"]
    return {"valid": ok, "issues": issues, "normalized_sql": body.sql.strip()[:2000] or None, "clickhouse_nl2sql_rules": True}


@app.post("/api/sql/guard")
def sql_guard(body: SqlValidateIn) -> dict[str, Any]:
    if (body.dialect or "").strip().lower() != "clickhouse":
        return {"allowed": True, "reason": None, "clickhouse_nl2sql_rules": False}
    ok, msg = validate_readonly_clickhouse_sql(body.sql)
    return {
        "allowed": ok,
        "reason": None if ok else msg,
        "issues": [] if ok else [msg or "ошибка"],
        "clickhouse_nl2sql_rules": True,
    }


@app.post("/api/v1/nl2sql/guard-assistant")
def nl2sql_guard_assistant(body: GuardAssistantIn) -> dict[str, Any]:
    out = guard_assistant_response(body)
    return out.model_dump() | {"clickhouse_nl2sql_rules": True}


class SqlOnlyGuardIn(BaseModel):
    sql: str = ""
    max_rows: int = Field(default=500, ge=1, le=50000)


@app.post("/api/v1/nl2sql/guard-sql")
def nl2sql_guard_sql(body: SqlOnlyGuardIn) -> dict[str, Any]:
    ok, msg = validate_readonly_clickhouse_sql(body.sql)
    if not ok:
        return {
            "ok": False,
            "sql": None,
            "errors": [msg or "?"],
            "warnings": [],
            "clickhouse_nl2sql_rules": True,
        }
    sql2, warn = enforce_limit_sql(body.sql, body.max_rows)
    return {
        "ok": True,
        "sql": sql2,
        "warnings": [warn] if warn else [],
        "errors": [],
        "clickhouse_nl2sql_rules": True,
    }
