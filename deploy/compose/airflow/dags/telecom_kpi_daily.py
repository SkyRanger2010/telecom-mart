"""Ежедневный пайплайн telecom-mart для Airflow.

Целевая схема воркфлоу (слои и порядок):

1. Ежедневная загрузка из источника: по одному `load_raw_day.py --table <schema.table>`
   на каждую строку `tables[]` в `raw_load_config.json`, затем барьер `raw_layer_done`.
2. Инкрементальный режим по `global_log`, full — полный снимок в `*_snapshot`; см. режим таблицы в JSON.
3. Контроль качества загруженных данных: группа `dq_jobs/*` (сводка манифеста RAW и журнала
   DDS → `validate.dq_daily_summary` + логи задач Airflow); per-table DQ при сборке DDS —
   `dds_validate/*.sql`, `dds_clean/*.sql`, запись в `dds.dds_table_load_log`;
   см. также `validate_raw_daily.sql`.
4. Пополнение DDS: сейчас задачи `dds_jobs/*` строят текущие таблицы `dds_*`
   из RAW; историчность по Type 2 (SCD2) — шаблон `dds_scd2_template.sql`, отдельный
   шаг пайплайна после его внедрения.
5. Витрины MART поверх актуализированного DDS: `mart_jobs` (`gen_dim_tariff` → inflow/outflow → АБ0/30/90 → прочие KPI).

Текущий DAG: resolve_kpi_daily_data_day → raw → DDS → mart__gen_dim_tariff → inflow/outflow и прочие KPI параллельно →
mart__gen_kpi_ab* после inflow/outflow → advance_kpi_daily_watermark (курсор в Iceberg).

Дополнительные BashOperator-ы с Trino SQL можно добавить в те же группы DQ.

Выбор календарного «дня данных»:
- по умолчанию — водяной знак в Iceberg (следующий день после успешной загрузки);
- переопределение — **conf** или **Params** `process_date` (`YYYY-MM-DD`). Подробности — **`doc_md`** / `_DAG_EXTRA_DOC`.
"""

from __future__ import annotations

import json
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param, TaskGroup
from airflow.task.trigger_rule import TriggerRule

from telecom_mart_job_scripts import (
    MART_AB_JOB_SCRIPTS,
    MART_FLOW_JOB_SCRIPTS,
    MART_JOB_SCRIPTS,
)

_DIM_TARIFF_SCRIPT = "gen_dim_tariff.py"
assert _DIM_TARIFF_SCRIPT in MART_JOB_SCRIPTS, "MART_JOB_SCRIPTS должен включать gen_dim_tariff.py (первый шаг MART после DDS)"

_AF_ROOT = Path(__file__).resolve().parents[1]
_SCRIPTS_DIR = _AF_ROOT / "scripts"
if _SCRIPTS_DIR.is_dir():
    _p = str(_SCRIPTS_DIR)
    if _p not in sys.path:
        sys.path.insert(0, _p)

RAW_CONFIG = "/opt/airflow/scripts/raw_load_config.json"
_RAW_TABLE_NAMES_FALLBACK: tuple[str, ...] = (
    "public.clients",
    "public.client_property",
    "public.agreements",
    "public.accounts",
    "public.accounts_history",
    "public.account_transactions",
    "public.ledgers",
    "public.orders",
    "public.segments",
    "public.order_base_types",
    "public.packages",
    "public.packages2orders",
    "public.tariffs",
    "public.tariff_sales",
    "public.tariff2option",
    "public.tariff2pack",
)


def raw_table_names_for_dag(config_path: str = RAW_CONFIG) -> tuple[str, ...]:
    """Имена `tables[].name` из JSON подряд как в конфиге; fallback если файл недоступен при парсе DAG."""
    path = Path(config_path)
    if path.is_file():
        try:
            with path.open(encoding="utf-8") as handle:
                payload = json.load(handle)
            return tuple(str(row["name"]).strip() for row in payload["tables"])
        except (OSError, KeyError, TypeError, json.JSONDecodeError, ValueError):
            pass
    return _RAW_TABLE_NAMES_FALLBACK


RAW_TABLE_NAMES = raw_table_names_for_dag()
RAW_TZ = "Europe/Moscow"
START = pendulum.datetime(2024, 1, 1, tz=RAW_TZ)

# День данных: задача resolve_kpi_daily_data_day записывает XCom `data_day` (
# см. telecom_kpi_daily_watermark.resolve_data_day ).
_KPI_DATA_DAY = "{{ ti.xcom_pull(task_ids='resolve_kpi_daily_data_day', key='data_day') }}"


def _resolve_kpi_daily_data_day(**context) -> None:
    from telecom_kpi_daily_watermark import resolve_data_day

    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    if not isinstance(conf, dict):
        conf = {}
    ov_raw = conf.get("process_date")
    params_obj = context.get("params") or {}
    if ov_raw is None or (isinstance(ov_raw, str) and ov_raw.strip() == ""):
        ov_raw = params_obj.get("process_date")
    ov: str | None
    if ov_raw is None or (isinstance(ov_raw, str) and ov_raw.strip() == ""):
        ov = None
    else:
        ov = str(ov_raw).strip()

    ti = context["ti"]
    day_str = resolve_data_day(
        config_path=RAW_CONFIG,
        tz_name=RAW_TZ,
        process_date_override=ov,
    )
    ti.xcom_push(key="data_day", value=day_str)


def _advance_kpi_daily_watermark(**context) -> None:
    from datetime import date as date_cls

    from telecom_kpi_daily_watermark import advance_daily_watermark

    ti = context["ti"]
    day_str = ti.xcom_pull(task_ids="resolve_kpi_daily_data_day", key="data_day")
    if not day_str:
        raise RuntimeError("XCom data_day отсутствует (resolve_kpi_daily_data_day)")
    advance_daily_watermark(
        config_path=RAW_CONFIG,
        tz_name=RAW_TZ,
        loaded_day=date_cls.fromisoformat(str(day_str)),
    )


_DAG_EXTRA_DOC = """
### Дата дня данных (водяной знак и переопределение)

**Авторежим.** Первая задача считает день данных как **следующий календарный день после последнего
успешно завершённого** для связки (**`load.batch_service.daily_loader_name`**, таймзона DAG)
в Iceberg **`raw.raw_load_service_state`**. Если записи ещё нет — от
**`load.batch_service.bootstrap_last_loaded_day`** (см. `telecom_kpi_daily_watermark.py`).

**После успешного** цикла RAW → DQ → DDS → DQ → MART задача **`advance_kpi_daily_watermark`**
сохраняет загруженный день как новый курсор. При сбое на любом шаге курсор **не двигается** —
следующий запуск снова предложит тот же день.

**Явная дата.** Задайте **Params → process_date** или **Configuration JSON:**
`{"process_date": "2024-01-01"}`. Пустое значение = снова авторежим. **Logical date (ds)** на выбор
дня данных **не используется** (чтобы scheduled run не воспринимал «сегодня по Airflow» как дату источника).

Массовый пересчёт за диапазон дат: DAG **`telecom_kpi_daily_catchup`** последовательно триггерит
`telecom_kpi_daily` на каждый день (conf ``start_date`` / ``end_date``). Иначе — вручную для каждого дня trigger с
`process_date`; без него каждый следующий успешный run берёт уже **«следующий»** день из водяного знака —
как после обычного ежедневного SLA.

**Только MART** (без RAW/DDS и без сдвига водяного знака): DAG **`telecom_mart_backfill`** (все витрины)
или **`telecom_mart_vitrina_backfill`** (одна витрина + период) — conf ``start_date`` / ``end_date``.

Из контейнера **scheduler** (Compose: `docker compose exec airflow-scheduler …`):

```bash
airflow dags trigger telecom_kpi_daily
airflow dags trigger telecom_kpi_daily -c '{"process_date": "2024-01-01"}'
```

Интервал inclusive (Apache Airflow 3.x):

```bash
airflow backfill create --dag-id telecom_kpi_daily --from-date 2024-01-01 --to-date 2024-01-07
```
"""

# Последовательность dds_jobs: справочники и сущности без тяжёлых FK можно переставлять при необходимости.
DDS_JOB_SCRIPTS: tuple[str, ...] = (
    "dds_public_clients.py",
    "dds_public_client_property.py",
    "dds_public_accounts.py",
    "dds_public_accounts_history.py",
    "dds_public_account_transactions.py",
    "dds_public_agreements.py",
    "dds_public_orders.py",
    "dds_public_packages.py",
    "dds_public_packages2orders.py",
    "dds_public_ledgers.py",
    "dds_public_segments.py",
    "dds_public_order_base_types.py",
    "dds_public_tariffs.py",
    "dds_public_tariff_sales.py",
    "dds_public_tariff2option.py",
    "dds_public_tariff2pack.py",
)

# Список MART_JOB_SCRIPTS — модуль ``telecom_mart_job_scripts`` (общий с ``telecom_mart_backfill``).


with DAG(
    dag_id="telecom_kpi_daily",
    description="RAW → DQ сводки → DDS → DQ DDS → MART KPI",
    doc_md=_DAG_EXTRA_DOC,
    schedule="0 5 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    tags=["telecom-mart", "raw", "dds", "mart", "kpi", "dq"],
    params={
        "process_date": Param(
            default=None,
            type=["null", "string"],
            title="Дата дня данных (YYYY-MM-DD)",
            description=(
                "Если непустое — принудительный день загрузки (после conf.process_date из trigger). "
                "Иначе — автоматический: следующий день после last_fully_loaded_day в Iceberg "
                "(см. load.batch_service.daily_loader_name + bootstrap_last_loaded_day)."
            ),
        ),
    },
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
) as dag:
    resolve_kpi_daily_data_day = PythonOperator(
        task_id="resolve_kpi_daily_data_day",
        python_callable=_resolve_kpi_daily_data_day,
    )

    raw_tasks = [
        BashOperator(
            task_id=f"raw__{name.replace('.', '__')}",
            bash_command=(
                "python3 /opt/airflow/scripts/load_raw_day.py "
                f"--config {RAW_CONFIG} "
                f"--day {_KPI_DATA_DAY} "
                f"--tz {RAW_TZ} "
                f"--table '{name}'"
            ),
        )
        for name in RAW_TABLE_NAMES
    ]

    raw_layer_done = EmptyOperator(
        task_id="raw_layer_done",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Сводки объёмов и DQ: манифест RAW; сюда же позже можно добавить BashOperator с validate_raw_daily.sql.
    with TaskGroup(group_id="dq_loaded_data") as dq_loaded_data:
        dq_summarize_raw_manifest = BashOperator(
            task_id="summarize_raw_manifest",
            bash_command=(
                "python3 /opt/airflow/scripts/dq_jobs/dq_summarize_raw_manifest.py "
                f"--config {RAW_CONFIG} "
                f"--process-date {_KPI_DATA_DAY}"
            ),
        )

    dds_tasks = [
        BashOperator(
            task_id=f"dds__{name[:-3]}",
            bash_command=(
                f"python3 /opt/airflow/scripts/dds_jobs/{name} "
                f"--config {RAW_CONFIG} "
                f"--cutoff-day {_KPI_DATA_DAY} "
                f"--dds-day-tz {RAW_TZ} "
                "--skip-pre-count"
            ),
        )
        for name in DDS_JOB_SCRIPTS
    ]

    dds_layer_done = EmptyOperator(
        task_id="dds_layer_done",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Агрегация dds_table_load_log после всех пересборок DDS.
    with TaskGroup(group_id="dq_dds_quality") as dq_dds_quality:
        dq_summarize_dds_load_log = BashOperator(
            task_id="summarize_dds_load_log",
            bash_command=(
                "python3 /opt/airflow/scripts/dq_jobs/dq_summarize_dds_load_log.py "
                f"--config {RAW_CONFIG} "
                f"--process-date {_KPI_DATA_DAY}"
            ),
        )

    def _mart_bash_operator(script: str) -> BashOperator:
        return BashOperator(
            task_id=f"mart__{script[:-3]}",
            bash_command=(
                f"python3 /opt/airflow/scripts/mart_jobs/{script} "
                f"--config {RAW_CONFIG} "
                f"--report-date {_KPI_DATA_DAY}"
            ),
        )

    # Справочник тарифов из DDS — до KPI-витрин; АБ0/30/90 — после притока и оттока.
    mart_dim_tariff = _mart_bash_operator(_DIM_TARIFF_SCRIPT)
    mart_flow_tasks = [_mart_bash_operator(name) for name in MART_FLOW_JOB_SCRIPTS]
    mart_ab_tasks = [_mart_bash_operator(name) for name in MART_AB_JOB_SCRIPTS]
    _mart_skip = {_DIM_TARIFF_SCRIPT, *MART_FLOW_JOB_SCRIPTS, *MART_AB_JOB_SCRIPTS}
    mart_other_tasks = [_mart_bash_operator(name) for name in MART_JOB_SCRIPTS if name not in _mart_skip]

    advance_kpi_daily_watermark = PythonOperator(
        task_id="advance_kpi_daily_watermark",
        python_callable=_advance_kpi_daily_watermark,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    mart_all_kpi = mart_flow_tasks + mart_ab_tasks + mart_other_tasks
    mart_dim_tariff >> (mart_flow_tasks + mart_other_tasks)
    for _flow in mart_flow_tasks:
        _flow >> mart_ab_tasks

    # Граф: resolve → RAW … → DDS → MART → водяной знак.
    resolve_kpi_daily_data_day >> raw_tasks >> raw_layer_done >> dq_loaded_data >> dds_tasks >> dds_layer_done >> dq_dds_quality >> mart_dim_tariff >> mart_all_kpi >> advance_kpi_daily_watermark
