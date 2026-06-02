"""Правила извлечения и проверки SQL для NL2SQL (ClickHouse KPI-витрины)."""

from __future__ import annotations

import re

from pydantic import BaseModel, Field

# Список синхронизирован с mart_serving_clickhouse.MART_TABLE_NAMES
ALLOWED_KPI_TABLES = (
    "kpi_ab0_daily",
    "kpi_ab30_daily",
    "kpi_ab90_daily",
    "kpi_arpu_daily",
    "kpi_revenue_daily",
    "kpi_revenue_active_month",
    "kpi_revenue_active_week",
    "kpi_revenue_active_quarter",
    "kpi_revenue_active_year",
    "kpi_inflow_daily",
    "kpi_outflow_daily",
    "dim_tariff",
)

# DESC намеренно не в списке: ловит ORDER BY … DESC. Команда DESC table
# отсекается проверкой «только WITH/SELECT» в начале запроса; DESCRIBE — ниже.
_FORBIDDEN = re.compile(
    r"\b(INSERT|REPLACE|ALTER|DROP|DETACH|ATTACH|TRUNCATE|RENAME|"
    r"SYSTEM|KILL|OPTIMIZE|GRANT|REVOKE|CREATE|EXEC|EXECUTE|"
    r"SETTINGS|SHOW|DESCRIBE|EXPLAIN\s+PIPELINE)\b",
    re.IGNORECASE,
)
# ClickHouse: DESC db.table (не ORDER BY … DESC LIMIT)
_FORBIDDEN_DESC_TABLE = re.compile(
    r"\bDESC\s+(?!LIMIT\b|OFFSET\b|NULLS\b)(?:`[^`]+`|[\w]+(?:\.[\w]+)?)\b",
    re.IGNORECASE,
)
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_FENCED_SQL = re.compile(r"```(?:sql)?\s*([\s\S]*?)\s*```", re.IGNORECASE)


def extract_sql(llm_answer: str) -> str | None:
    text = llm_answer.strip()
    fb = _FENCED_SQL.search(text)
    if fb:
        cand = fb.group(1).strip()
        body = cand.rstrip(";").strip()
        return body or None
    one = text.rstrip(";").strip()
    u = one.upper()
    if u.startswith("WITH ") or u.startswith("SELECT") or u.startswith("(SELECT"):
        return one
    return None


def _strip_comments_for_scan(sql: str) -> str:
    s = _BLOCK_COMMENT.sub("", sql)
    lines_out: list[str] = []
    for line in s.splitlines():
        if "--" in line:
            line = line.split("--", 1)[0]
        lines_out.append(line)
    out = "\n".join(lines_out)
    out = re.sub(r"'([^']|'')*'", "''", out)
    return out


def _mentions_allowed_table(norm_sql: str) -> bool:
    low = norm_sql.lower().replace("`", "").replace('"', "")
    for tn in ALLOWED_KPI_TABLES:
        if tn.lower() in low:
            return True
    return False


def validate_readonly_clickhouse_sql(sql: str) -> tuple[bool, str | None]:
    raw = sql.strip()
    if not raw:
        return False, "пустой SQL"

    stripped = raw.rstrip().rstrip(";")
    if ";" in stripped:
        return False, "несколько выражений (точка с запятой) запрещены"

    scan_nostring = _strip_comments_for_scan(stripped)
    scan_head = scan_nostring.strip()
    uh2 = scan_head.upper().lstrip(" \t\r\n\f\v(")

    if not (uh2.startswith("WITH ") or uh2.startswith("SELECT") or uh2.startswith("(SELECT")):
        return False, "разрешены только WITH/SELECT"

    uf = uh2.upper()
    if " FORMAT " in f" {uf} ":
        return False, "FORMAT запрещён"

    if _FORBIDDEN.search(scan_nostring):
        return False, "ключевые слова запрещены"

    if _FORBIDDEN_DESC_TABLE.search(scan_nostring):
        return False, "ключевые слова запрещены"

    if not _mentions_allowed_table(scan_nostring.lower()):
        return False, "нужно ссылаться на одну из витрин KPI (имена таблиц из белого списка)"

    if " INTO OUTFILE " in uh2.upper():
        return False, "INTO OUTFILE запрещён"

    return True, None


def enforce_limit_sql(sql: str, max_rows: int) -> tuple[str, str | None]:
    uh = sql.upper()
    warning: str | None = None
    m = re.search(r"\bLIMIT\s+(\d+)", uh)
    if m:
        lim = int(m.group(1))
        if lim > max_rows:
            warning = "слишком большой LIMIT — ограничили"
            return re.sub(r"\bLIMIT\s+\d+", f"LIMIT {int(max_rows)}", sql, flags=re.IGNORECASE), warning
        return sql, warning
    tail = sql.rstrip().rstrip(";")
    return f"{tail} LIMIT {max_rows}", None


# Единственная таблица, в которую sqlguard может материализовать результат дашборда по запросу.
MATERIALIZE_TABLE_ALLOWLIST = frozenset({"dashboard_query_stub"})


def _qi(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def qualified_table_name(database: str, table: str) -> str:
    return f"{_qi(database.strip())}.{_qi(table.strip())}"


def build_materialize_statements(
    select_sql: str,
    *,
    database: str,
    table: str,
) -> tuple[list[str], list[str]]:
    """DROP + CREATE … AS (SELECT): пересоздать таблицу дашборда с динамической схемой."""
    errors: list[str] = []
    tbl = table.strip()
    if tbl not in MATERIALIZE_TABLE_ALLOWLIST:
        errors.append(f"материализация разрешена только в {sorted(MATERIALIZE_TABLE_ALLOWLIST)}")
        return [], errors

    db = database.strip()
    if not db:
        errors.append("пустое имя базы")
        return [], errors

    ok, msg = validate_readonly_clickhouse_sql(select_sql)
    if not ok:
        errors.append(msg or "некорректный SELECT для материализации")
        return [], errors

    qname = qualified_table_name(db, tbl)
    body = select_sql.strip().rstrip(";")
    return (
        [
            f"DROP TABLE IF EXISTS {qname}",
            (
                f"CREATE TABLE {qname}\n"
                "ENGINE = MergeTree()\n"
                "ORDER BY tuple()\n"
                f"AS (\n{body}\n)"
            ),
        ],
        errors,
    )


class GuardAssistantIn(BaseModel):
    assistant_text: str = Field(..., description="«Сырой» ответ модели (markdown допускается)")
    max_rows: int = Field(default=500, ge=1, le=50000)
    materialize_table: str | None = Field(
        default=None,
        description="Если задано — после проверки SELECT сформировать SQL пересоздания таблицы в CH",
    )
    database: str = Field(default="serving", description="База для materialize_table")


class GuardAssistantOut(BaseModel):
    ok: bool
    sql: str | None = None
    sql_extracted: str | None = None
    materialize_sql: list[str] = Field(default_factory=list)
    materialize_table: str | None = None
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


def guard_assistant_response(body: GuardAssistantIn) -> GuardAssistantOut:
    warns: list[str] = []

    extracted = extract_sql(body.assistant_text)
    if not extracted:
        return GuardAssistantOut(
            ok=False,
            errors=["в тексте модели не найден пригодный SQL"],
        )

    ok, msg = validate_readonly_clickhouse_sql(extracted)
    if not ok:
        return GuardAssistantOut(
            ok=False,
            sql_extracted=extracted,
            errors=[msg or "валидация SQL"],
        )

    final_sql, lim_warn = enforce_limit_sql(extracted, body.max_rows)
    if lim_warn:
        warns.append(lim_warn)

    materialize_sql: list[str] = []
    mat_table = (body.materialize_table or "").strip() or None
    if mat_table:
        materialize_sql, mat_errors = build_materialize_statements(
            final_sql,
            database=body.database,
            table=mat_table,
        )
        if mat_errors:
            return GuardAssistantOut(
                ok=False,
                sql_extracted=extracted,
                sql=final_sql,
                materialize_table=mat_table,
                errors=mat_errors,
            )

    return GuardAssistantOut(
        ok=True,
        sql=final_sql,
        sql_extracted=extracted,
        materialize_sql=materialize_sql,
        materialize_table=mat_table,
        warnings=warns,
        errors=[],
    )


def validate_clickhouse_nl2sql_rules(sql: str) -> tuple[bool, list[str]]:
    ok, msg = validate_readonly_clickhouse_sql(sql)
    if ok:
        return True, []
    return False, [msg or "ошибка"]

