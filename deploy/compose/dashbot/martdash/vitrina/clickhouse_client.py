"""Клиент для прямых запросов к ClickHouse (параметрические SELECT и DDL/DML)."""
from __future__ import annotations

from typing import Any

import clickhouse_connect
from django.conf import settings


def get_client():
    """HTTP-клиент clickhouse-connect (порт из окружения, как mart_serving_clickhouse)."""
    return clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST,
        port=int(settings.CLICKHOUSE_HTTP_PORT),
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DATABASE,
    )


def qualified_table(database: str, table: str) -> str:
    def qi(x: str) -> str:
        return "`" + x.replace("`", "``") + "`"

    return f"{qi(database)}.{qi(table)}"


def fetch_rows(sql: str, *, parameters: dict[str, Any] | None = None) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Выполнить параметрический запрос; возвращает имена столбцов и строки."""
    client = get_client()
    params = parameters or {}
    result = client.query(sql, parameters=params)
    return list(result.column_names), list(result.result_rows)


def fetch_rows_raw(sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Произвольный только-SELECT (NL2SQL) без параметров; лимиты накладывает вызывающий код."""
    client = get_client()
    result = client.query(sql)
    return list(result.column_names), list(result.result_rows)


def execute_command(sql: str) -> None:
    """DDL/DML без результата (создание витрин-заглушек и т.п.)."""
    client = get_client()
    client.command(sql)


def execute_commands(statements: list[str]) -> None:
    """Выполнить несколько DDL/DML подряд (материализация dashboard_query_stub)."""
    for stmt in statements:
        s = stmt.strip()
        if not s:
            continue
        execute_command(s if s.endswith(";") else f"{s};")
