# dashbot — KPI-дашборды (Django)

Сервис **`dashbot`** — веб-приложение на **Django** (`martdash`), которое читает KPI-витрины из **ClickHouse** (схема `serving`) и отдаёт интерактивные дашборды, drill-down по таблицам и чат **NL2SQL** (через `llm-api` + `sqlguard-api`).

В foundation-compose это **не** статический nginx-стаб: контейнер собирается из `deploy/compose/dashbot/Dockerfile`, внутри **gunicorn** на порту `8080`. Снаружи порт задаётся **`DASHBOT_STUB_PORT`** (по умолчанию `8090`).

## Поток данных

```
Iceberg MART (Trino)  →  mart_serving_clickhouse.py  →  ClickHouse (serving.*)
                                                              ↓
                                                         dashbot (Django)
```

Ежедневные KPI пишутся в `iceberg.mart` задачами Airflow (`mart_vitrina_runner.py`, `mart_jobs/gen_kpi_*_daily.py`). Репликация в ClickHouse — `airflow/scripts/mart_serving_clickhouse.py` (полная или инкрементальная по `report_date`).

Без таблиц в `serving` дашборды откроются, но графики и списки фильтров будут пустыми.

## Быстрый доступ

| URL (локально) | Назначение |
|----------------|------------|
| `http://localhost:8090/` | **Абонентская база** (главная) |
| `http://localhost:8090/financial/` | **Финансовые показатели** (ARPU, выручка) |
| `http://localhost:8090/query/` | Дашборд по произвольному SQL-запросу (NL2SQL) |
| `http://localhost:8090/v/<table>/` | Drill-down по одной витрине |
| `http://localhost:8090/health/` | Healthcheck (`ok`) |

## REST API (JSON)

Параметры фильтров совпадают с query string страницы:

| Endpoint | Описание |
|----------|----------|
| `GET /api/subscriber-base/` | Графики АБ0/30/90, приток/отток, списки фильтров |
| `GET /api/financial/` | Графики ARPU и приведённой дневной выручки, фильтры |
| `GET /api/query-dashboard/` | Данные для дашборда по запросу |
| `POST /api/chat/nl2sql/` | Чат NL2SQL (тело JSON, CSRF) |

Общие query-параметры:

| Параметр | Описание |
|---------|----------|
| `df`, `dt` | Начало и конец периода (`YYYY-MM-DD`) |
| `segment_id` | Направление деятельности (повторяемый) |
| `service_kind` | Вид услуги |
| `tariff_id` | Тариф |
| `client_type` | Тип клиента: подпись `client_type_title` или код `person` / `org` / `ip` |

При ошибке ClickHouse API возвращает `{"ok": false, "error": "…"}`; фильтры по возможности всё равно отдаются в теле ответа.

## Дашборды

### Абонентская база (`/`)

- График **АБ0 / АБ30 / АБ90** (`kpi_ab0_daily`, `kpi_ab30_daily`, `kpi_ab90_daily`).
- График **приток и отток** (`kpi_inflow_daily`, `kpi_outflow_daily`).
- Боковая панель фильтров + чат NL2SQL (скрывается в полноэкранном layout дашборда).

Конфиг подписей: `martdash/vitrina/subscriber_dashboard_labels.json`.

### Финансовые показатели (`/financial/`)

- **ARPU** — сводный по дням: `sum(total_revenue) / sum(active_clients) × дней в месяце` (`kpi_arpu_daily`).
- **Приведённая дневная выручка** — `sum(total_revenue)` по `kpi_revenue_daily`.

Конфиг: `martdash/vitrina/financial_dashboard_labels.json`.

## Фильтры (общие для обоих дашбордов)

### Период

- Поле **flatpickr** (диапазон дат) + скрытые поля `df` / `dt`.
- **Быстрые пресеты** (кнопки под полем даты), модуль `static/vitrina/date_range_filter.js`:

  | Пресет | Диапазон |
  |--------|----------|
  | 7 дней | Последние 7 календарных дней до `dataMaxDate` |
  | 30 дней | Последние 30 дней |
  | 90 дней | Последние 90 дней |
  | Месяц | С 1-го числа текущего месяца |
  | Пр. месяц | Полный предыдущий календарный месяц |
  | Весь период | От `dataMinDate` до `dataMaxDate` витрины |

Границы «весь период» и верхняя дата пресетов берутся из boot (`dataMinDate` / `dataMaxDate` — min/max `report_date` в `kpi_ab0_daily`).

### Тип клиента

Фильтр по **`client_type_title`** (подпись из справочника `public.client_property`), а не только по коду DDS `person` / `org` / `ip`.

- **Группы** (чекбокс группы + пункты внутри): **РФ**, **Нерезиденты**, **УКР** — по префиксу подписи.
- **Быстрый выбор**: **Физлица**, **Юрлица**, **ИП** — все строки с соответствующим `client_type`.

Логика группировки и SQL: `vitrina/client_type_filter.py`. UI: `static/vitrina/client_type_filter.js`.

Примеры подписей в данных:

| Группа | Примеры `client_type_title` |
|--------|----------------------------|
| РФ | РФ — Физическое лицо, РФ — Юридическое лицо, РФ — ИП |
| Нерезиденты | Нерезидент — Физическое лицо, … |
| УКР | УКР — Физическое лицо, … |

### Сегмент, услуга, тариф

Каскадные чеклисты: смена сегмента или типа клиента сбрасывает зависимые «услуга» и «тариф». Подписи сегментов — `vitrina/segment_labels.py` (иерархия `dim_segment`).

## Структура кода (`martdash/vitrina/`)

| Модуль | Назначение |
|--------|------------|
| `views.py` | Страницы и API |
| `subscriber_base_data.py` | Данные дашборда «Абонентская база» |
| `financial_base_data.py` | Данные дашборда «Финансовые показатели» |
| `client_type_filter.py` | Группы типа клиента, WHERE по title/коду |
| `metadata.py` | Описание витрин для каталога и drill-down |
| `clickhouse_client.py` | HTTP-клиент ClickHouse |
| `serving_flow_columns.py` | Патчи схемы CH при старте (`client_type`, метрики притока/оттока) |
| `nl2sql.py`, `nl2sql_context.py` | Чат и контекст схемы для LLM |
| `query_dashboard.py` | Дашборд по запросу |
| `templates/vitrina/` | HTML-шаблоны |
| `static/vitrina/` | `client_type_filter.js`, `date_range_filter.js`, `kpi_line_chart.js` |

## Переменные окружения

Задаются в `deploy/compose/.env` (см. `.env.example`):

| Переменная | Назначение |
|------------|------------|
| `DASHBOT_STUB_PORT` | Порт на хосте (→ `8080` в контейнере) |
| `CLICKHOUSE_HOST`, `CLICKHOUSE_DB`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | Подключение к serving |
| `CLICKHOUSE_INTERNAL_HTTP_PORT` | HTTP-порт CH внутри docker-сети (`8123`) |
| `CLICKHOUSE_ENSURE_FLOW_METRICS` | `1` — при старте добавить недостающие колонки в CH |
| `LLM_API_URL`, `SQLGUARD_API_URL` | NL2SQL и sqlguard |
| `NL2SQL_LLM_MODEL`, `NL2SQL_MAX_ROWS` | Модель и лимит строк |
| `QUERY_DASHBOARD_TABLE` | Таблица CH для ad-hoc запроса |
| `DJANGO_SECRET_KEY`, `DJANGO_DEBUG`, `DJANGO_ALLOWED_HOSTS` | Django |

## Сборка и перезапуск

```bash
cd deploy/compose
docker compose build dashbot
docker compose up -d dashbot
```

После изменения Python или статики нужна **пересборка образа** (`collectstatic` выполняется в Dockerfile). Проверка:

```bash
docker compose exec dashbot ls /app/staticfiles/vitrina/
docker compose exec dashbot python -c "from vitrina.client_type_filter import build_client_type_filter_options; print('ok')"
```


