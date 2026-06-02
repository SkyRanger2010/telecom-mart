"""LLM-сервис: OpenAI-compatible chat completions (DeepSeek и др.) + NL2SQL."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

SERVICE = "llm"

UPSTREAM_CHAT_URL = (
    os.environ.get("LLM_UPSTREAM_CHAT_COMPLETIONS_URL", "").strip().rstrip("/") or ""
)
UPSTREAM_AUTH = os.environ.get("LLM_UPSTREAM_AUTH_TOKEN", "").strip()
REQUEST_TIMEOUT_SEC = float(os.environ.get("LLM_UPSTREAM_TIMEOUT_SEC", "90"))

NL2SQL_SYSTEM_LEGACY = """Ты генератор только ClickHouse SQL. Ответь одним блоком markdown ```sql ... ``` без пояснений.
Нельзя: DDL/DML/SYSTEM/KILL/SETTINGS/SHOW/FORMAT, несколько операторов.
Разрешён один SELECT или WITH … SELECT только по таблицам из схемы в конце промпта.
Даты через Date или toDate; агрегаты sum().
"""

NL2SQL_SYSTEM_JSON_TEMPLATE = """Ты — генератор SQL для аналитики телеком-оператора. Диалект: ClickHouse.

## Формат ответа
- Только один блок markdown: ```sql ... ```
- Без пояснений до и после блока.
- Один оператор: WITH … SELECT или SELECT (без «;» в конце).
- Если пользователь не указал LIMIT — добавь LIMIT {max_rows} (не больше).

## Жёсткие ограничения
- Только чтение: запрещены INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE, SYSTEM, KILL, OPTIMIZE, SETTINGS, SHOW, DESCRIBE, FORMAT, INTO OUTFILE.
- Используй только таблицы из JSON-поля `vitrinas` (полные имена: `{qualifier}.<table>`).
- Не выдумывай колонки: только из описания витрин.
- Не используй SELECT * без необходимости; явно перечисляй поля.
- Даты: toDate('YYYY-MM-DD'); диапазоны — BETWEEN toDate('…') AND toDate('…').

## Агрегация по типу метрики (важно)
| Тип | Поля | За календарный период (месяц, квартал…) | По дням («по дням», «динамика») |
| Stock «на дату» | active_subscribers (АБ0/30/90) | **avg(**active_subscribers**)** по report_date в периоде — среднесуточный уровень; **не** sum() по дням | GROUP BY report_date, **sum(**active_subscribers**)** |
| Поток за день | new_clients, churned_clients, total_revenue (день) | **sum(**мера**)** за все дни периода | GROUP BY report_date, sum(мера) |
| ARPU | kpi_arpu_daily | не суммируй arpu_daily; sum(total_revenue)/nullIf(sum(active_clients),0) или точечно на дату | по report_date |
| Периодные витрины | kpi_revenue_active_* | фильтр по period_end, sum(total_revenue) | — |

- Слово «сумма» для АБ0/АБ30/АБ90 за месяц без «по дням» трактуй как **среднее за период** (avg), не как сумму ежедневных срезов.
- «Сумма притока/оттока/выручки за период» — sum() по дням.

## Сравнение нескольких метрик
- «Сравни», «сопоставь» — выведи метрики **в соседних колонках** (CTE + FULL OUTER JOIN или JOIN по ключу разреза).
- **Не** вычитай, **не** дели и **не** считай долю одной метрики от другой, если пользователь явно не просил «разницу», «дельту», «отношение», «%».
- АБ0 (активные на дату) и приток (new_clients) — разные сущности; только side-by-side.

## Приток и отток вместе
- «приток и отток», «сравни приток и отток» — **две** витрины: `kpi_inflow_daily` (`new_clients`) и `kpi_outflow_daily` (`churned_clients`), не одна таблица.
- «по дням», «динамика», «по датам» — в каждом CTE `GROUP BY report_date`, `sum(new_clients)` / `sum(churned_clients)`; **JOIN по `report_date`** (ось `Дата` в финальном SELECT).
- «по сегментам» / «по тарифам» — `GROUP BY` соответствующего разреза в CTE; JOIN по этому ключу, не по `report_date`.
- Финальные колонки: `Дата`, `Приток`, `Отток` (алиасы с backticks); `ORDER BY` даты по возрастанию.

## Выбор витрины
- АБ0/30/90 → kpi_ab0_daily / kpi_ab30_daily / kpi_ab90_daily (report_date, active_subscribers)
- Выручка за день → kpi_revenue_daily
- ARPU на дату → kpi_arpu_daily (month_start — начало окна)
- Приток/отток → kpi_inflow_daily / kpi_outflow_daily
- Выручка за период → kpi_revenue_active_month|week|quarter|year (period_end)
- Справочник тарифов → dim_tariff

## Разрезы
- «По сегментам» → segment_id (и при необходимости другие dimension из витрины); для NULL: ifNull(segment_id, toInt64(-1)).
- Две daily-витрины — отдельные CTE, JOIN по общим ключам разреза (segment_id, tariff_id, …), не смешивай в одном GROUP BY.

## Алиасы колонок (для дашборда)
- В **финальном SELECT** давай понятные **русские** имена: AS `Дата`, AS `Сегмент`, AS `АБ0 среднее`, AS `Приток новых клиентов`.
- В ClickHouse кириллицу и пробелы в алиасах оборачивай в **обратные кавычки** (backticks).
- Внутри CTE можно оставлять технические имена (segment_id, ab0_avg); наружу — только русские подписи для пользователя.
- report_date → `Дата`; segment_id → `Сегмент` или `ID сегмента`; tariff_title → `Тариф`; active_subscribers → `АБ0` / `АБ0 среднее`; new_clients → `Приток`; churned_clients → `Отток`.

## Период по умолчанию
- Если в question **нет** явного календарного периода (конкретные даты, месяц/год/квартал, «за …», «с … по …», «последние N дней») — обязательно фильтруй по полному диапазону **defaults.date_from** … **defaults.date_to** (ключ даты витрины: report_date или period_end).
- Если пользователь указал период — используй его; defaults не подставляй.
- defaults и data_catalog.available_date_range — весь доступный диапазон данных в витринах.

Применяй filters из JSON, если пользователь не уточнил разрезы.

В user-сообщении — полный JSON. Следуй полю question."""

STUB_NL2SQL_MARKDOWN = (
    "```sql\nSELECT sum(total_revenue) AS revenue FROM serving.kpi_revenue_daily LIMIT 50\n```"
)


def upstream_chat_endpoint() -> str:
    """URL upstream chat completions API (OpenAI-совместимый, по умолчанию DeepSeek).

    Returns:
        Полный URL или пустую строку, если upstream не настроен.
    """
    if not UPSTREAM_CHAT_URL:
        return ""
    base = UPSTREAM_CHAT_URL.strip().rstrip("/")
    return base if "/chat/completions" in base else f"{base}/chat/completions"


def _upstream_headers() -> dict[str, str]:
    """Заголовки для upstream-запросов: Content-Type + Bearer-авторизация (если задан токен)."""
    hdr: dict[str, str] = {"Content-Type": "application/json"}
    if not UPSTREAM_AUTH:
        return hdr
    auth = UPSTREAM_AUTH
    low = auth.lower()
    if not low.startswith("bearer ") and not low.startswith("basic "):
        auth = f"Bearer {auth.strip()}"
    hdr["Authorization"] = auth
    return hdr


def _post_upstream(payload: dict[str, Any]) -> dict[str, Any]:
    """POST-запрос к upstream chat completions API с обработкой ошибок.

    Args:
        payload: JSON-тело запроса (model, messages).

    Returns:
        Ответ upstream в виде словаря.

    Raises:
        HTTPException(502) при ошибках соединения или HTTP-статусе ≠ 2xx.
    """
    try:
        resp = httpx.post(
            upstream_chat_endpoint(),
            json=payload,
            headers=_upstream_headers(),
            timeout=REQUEST_TIMEOUT_SEC,
        )
        resp.raise_for_status()
    except httpx.HTTPStatusError as ex:
        detail = ex.response.text.strip() or str(ex)
        raise HTTPException(
            status_code=502,
            detail=f"upstream HTTP {ex.response.status_code}: {detail[:2000]}",
        ) from ex
    except httpx.RequestError as ex:
        raise HTTPException(status_code=502, detail=f"upstream request failed: {ex}") from ex

    data = resp.json()
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="upstream returned non-JSON body")
    return data


def _nl2sql_max_rows(request: dict[str, Any] | None) -> int:
    """Извлечь max_rows из поля constraints JSON-контекста NL2SQL."""
    if not request:
        return 500
    try:
        c = request.get("constraints") or {}
        return max(1, min(int(c.get("max_rows", 500)), 50_000))
    except (TypeError, ValueError):
        return 500


def _nl2sql_table_qualifier(request: dict[str, Any] | None) -> str:
    """Извлечь table_qualifier (имя БД) из поля database JSON-контекста NL2SQL."""
    if not request:
        return "serving"
    db = request.get("database") or {}
    if isinstance(db, dict):
        return str(db.get("table_qualifier") or db.get("name") or "serving")
    return "serving"


def _build_nl2sql_messages(body: "Nl2SqlCompletionsIn") -> list[dict[str, str]]:
    """Сформировать список сообщений для LLM: system + user.

    Если есть поле request (JSON) — использует шаблон NL2SQL_SYSTEM_JSON_TEMPLATE.
    Иначе — legacy-режим с NL2SQL_SYSTEM_LEGACY и schema_block.
    """
    if body.request:
        req = body.request
        max_rows = _nl2sql_max_rows(req)
        qualifier = _nl2sql_table_qualifier(req)
        sys_content = NL2SQL_SYSTEM_JSON_TEMPLATE.format(max_rows=max_rows, qualifier=qualifier)
        user_content = (
            "Сгенерируй ClickHouse SQL по полю `question` в JSON ниже.\n\n"
            f"```json\n{json.dumps(req, ensure_ascii=False, indent=2)}\n```"
        )
        return [
            {"role": "system", "content": sys_content},
            {"role": "user", "content": user_content},
        ]

    question = (body.question or "").strip()
    schema = body.schema_block or ""
    return [
        {"role": "system", "content": f"{NL2SQL_SYSTEM_LEGACY}\n\nСхема и таблицы:\n{schema}\n"},
        {"role": "user", "content": question},
    ]


app = FastAPI(
    title="telecom-mart llm-api",
    version="1.2.0",
    description=(
        "Chat completions и NL2SQL через upstream OpenAI-compatible API (по умолчанию DeepSeek). "
        "JSON-контекст (поле request) или legacy question+schema_block."
    ),
)


@app.get("/health")
def health() -> dict[str, Any]:
    """Health-check: статус сервиса и наличие upstream."""
    return {"status": "ok", "service": SERVICE, "upstream_configured": bool(UPSTREAM_CHAT_URL)}


@app.get("/api/v1/info")
def info() -> dict[str, Any]:
    """Метаданные сервиса: версия, режимы NL2SQL, время."""
    return {
        "service": SERVICE,
        "upstream_chat_completions": bool(UPSTREAM_CHAT_URL),
        "upstream_api": "openai_compatible",
        "upstream_hint": "DeepSeek: https://api.deepseek.com/v1/chat/completions, model deepseek-chat",
        "nl2sql_modes": ["request_json", "legacy_schema_block"],
        "time": datetime.now(tz=UTC).isoformat(),
    }


class ChatCompletionMessage(BaseModel):
    role: str = "user"
    content: str = ""


class ChatCompletionIn(BaseModel):
    model: str = "stub-model"
    messages: list[ChatCompletionMessage] = Field(default_factory=list)


@app.get("/api/v1/models")
def models_list() -> dict[str, Any]:
    """Список доступных моделей (OpenAI-compatible /models endpoint)."""
    return {
        "object": "list",
        "data": [{"id": "stub-model", "object": "model", "upstream": bool(UPSTREAM_CHAT_URL)}],
    }


@app.post("/api/v1/chat/completions")
def chat_completions(body: ChatCompletionIn) -> dict[str, Any]:
    """OpenAI-совместимый chat completions: прокси к upstream или stub-ответ.

    Если UPSTREAM_CHAT_URL настроен — проксирует запрос, иначе возвращает заглушку.
    """
    last = body.messages[-1].content if body.messages else ""

    if UPSTREAM_CHAT_URL:
        payload = {
            "model": body.model,
            "messages": [m.model_dump() for m in body.messages],
        }
        data = _post_upstream(payload)
        data["proxied_upstream"] = True
        return data

    return {
        "id": f"stubchat-{uuid.uuid4()}",
        "object": "chat.completion",
        "model": body.model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": f"[stub-llm] got: {last[:300]!r}",
                },
                "finish_reason": "stop",
            }
        ],
        "stub_local": True,
    }


class Nl2SqlCompletionsIn(BaseModel):
    """NL2SQL: предпочтительно полный JSON в `request`; иначе question + schema_block."""

    request: dict[str, Any] | None = Field(
        default=None,
        description="Полный контекст NL2SQL (version, question, vitrinas, filters, …)",
    )
    question: str = Field(default="", description="Legacy: текст вопроса")
    schema_block: str = Field(default="", description="Legacy: текстовая схема")
    model: str = "stub-model"


@app.post("/api/v1/nl2sql/completions")
def nl2sql_completions(body: Nl2SqlCompletionsIn) -> dict[str, Any]:
    """NL2SQL: естественный язык → SQL (ClickHouse).

    Принимает JSON-контекст (поле request) или legacy question + schema_block.
    Проксирует в upstream LLM или возвращает stub-ответ.
    """
    if not body.request and not (body.question or "").strip():
        return {
            "error": "нужно поле request (JSON) или question (legacy)",
            "object": "error",
        }

    messages = _build_nl2sql_messages(body)

    if not UPSTREAM_CHAT_URL:
        return {
            "id": f"nl2sql-stub-{uuid.uuid4()}",
            "object": "chat.completion",
            "model": body.model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": STUB_NL2SQL_MARKDOWN},
                    "finish_reason": "stop",
                }
            ],
            "stub_local": True,
            "upstream": False,
            "nl2sql_mode": "request_json" if body.request else "legacy",
        }

    payload = {"model": body.model, "messages": messages}
    data = _post_upstream(payload)
    data["proxied_upstream"] = True
    data["upstream"] = True
    data["nl2sql_mode"] = "request_json" if body.request else "legacy"
    return data
