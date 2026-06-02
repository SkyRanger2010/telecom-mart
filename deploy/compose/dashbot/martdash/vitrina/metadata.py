"""Метаописание KPI-витрин (сerving в ClickHouse; по сути как mart_serving_clickhouse.MART_TABLE_NAMES)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MeasureSum:
    column: str
    label_ru: str
    agg: str = "sum"  # sum | avg по строкам источника


@dataclass(frozen=True)
class MeasureRatio:
    numerator_col: str
    denominator_col: str
    label_ru: str
    round_decimals: int | None = None


MeasureDef = MeasureSum | MeasureRatio


@dataclass(frozen=True)
class VitrinaDef:
    table: str
    title_ru: str
    description_ru: str
    date_column: str
    dimension_cols: tuple[str, ...]
    measures: tuple[MeasureDef, ...]


_DIM_STD = ("segment_id", "tariff_id", "tariff_title", "service_kind")


ACTIVE_SUBSCRIBER_TABLES: frozenset[str] = frozenset(
    ("kpi_ab0_daily", "kpi_ab30_daily", "kpi_ab90_daily"),
)

TABLES_WITH_TARIFF_NAME_AXIS: frozenset[str] = frozenset(
    (
        *ACTIVE_SUBSCRIBER_TABLES,
        "kpi_arpu_daily",
        "kpi_revenue_daily",
        "kpi_receipts_daily",
        "kpi_revenue_active_month",
        "kpi_revenue_active_week",
        "kpi_revenue_active_quarter",
        "kpi_revenue_active_year",
    ),
)

VITRINS: tuple[VitrinaDef, ...] = (
    VitrinaDef(
        table="kpi_ab0_daily",
        title_ru="Активные (AB0), день",
        description_ru="Активные абоненты по календарному дню.",
        date_column="report_date",
        dimension_cols=_DIM_STD,
        measures=(MeasureSum("active_subscribers", "Активные абоненты (сумма)"),),
    ),
    VitrinaDef(
        table="kpi_ab30_daily",
        title_ru="Активные по AB30, день",
        description_ru="Активные за 30 дней — дневная витрина.",
        date_column="report_date",
        dimension_cols=_DIM_STD,
        measures=(MeasureSum("active_subscribers", "Активные абоненты (сумма)"),),
    ),
    VitrinaDef(
        table="kpi_ab90_daily",
        title_ru="Активные по AB90, день",
        description_ru="Активные за 90 дней — дневная витрина.",
        date_column="report_date",
        dimension_cols=_DIM_STD,
        measures=(MeasureSum("active_subscribers", "Активные абоненты (сумма)"),),
    ),
    VitrinaDef(
        table="kpi_revenue_daily",
        title_ru="Приведённая дневная выручка, день",
        description_ru=(
            "Приведённая дневная выручка по разрезам: тип клиента (client_type), "
            "направление (segment_id → dim_segment.segment_title с иерархией), "
            "услуга (service_kind), тариф (tariff_id); credits ÷ дней [date_start, date_end] проводки, "
            "если report_date в интервале и заказ ENABLED. paying_owners = активные AB0. "
            "Поступления — kpi_receipts_daily."
        ),
        date_column="report_date",
        dimension_cols=_DIM_STD + ("client_type",),
        measures=(
            MeasureSum("total_revenue", "Приведённая дневная выручка (сумма)"),
            MeasureSum("paying_owners", "Активные клиенты AB0 (сумма)"),
        ),
    ),
    VitrinaDef(
        table="kpi_receipts_daily",
        title_ru="Поступления, день",
        description_ru=(
            "Сумма фактических поступлений (credits из ledgers без нормализации) "
            "по дате проводки для заказов, активных на report_date."
        ),
        date_column="report_date",
        dimension_cols=_DIM_STD + ("client_type",),
        measures=(
            MeasureSum("total_receipts", "Поступления (сумма)"),
            MeasureSum("paying_owners", "Активные клиенты AB0 (сумма)"),
        ),
    ),
    VitrinaDef(
        table="kpi_arpu_daily",
        title_ru="ARPU, день",
        description_ru=(
            "По строке разреза: total_revenue — приведённая дневная выручка за report_date; "
            "arpu_daily = total_revenue / active_clients; arpu_monthly = arpu_daily × дней в месяце. "
            "Сводный ARPU за день по всем разрезам: sum(total_revenue) / sum(active_clients) × дней в месяце "
            "(не sum(arpu_monthly))."
        ),
        date_column="report_date",
        dimension_cols=_DIM_STD + ("client_type",),
        measures=(
            MeasureSum("total_revenue", "Приведённая дневная выручка (сумма)"),
            MeasureSum("active_clients", "Активные клиенты AB0 (сумма)"),
            MeasureRatio(
                numerator_col="total_revenue",
                denominator_col="active_clients",
                label_ru="ARPU дневной (сводный: выручка / AB0)",
            ),
            MeasureSum("arpu_daily", "ARPU по разрезу (среднее по строкам)", agg="avg"),
            MeasureSum("arpu_monthly", "ARPU×дней по разрезу (среднее)", agg="avg"),
        ),
    ),
    VitrinaDef(
        table="kpi_inflow_daily",
        title_ru="Приток, день",
        description_ru="Новые клиенты, новые договоры, новые заказы (день активации заказа; первый день — по MIN(activated) по клиенту и по договору).",
        date_column="report_date",
        dimension_cols=_DIM_STD,
        measures=(
            MeasureSum("new_clients", "Новые клиенты (сумма)"),
            MeasureSum("new_agreements", "Новые договоры (сумма)"),
            MeasureSum("new_orders", "Новые заказы (сумма)"),
        ),
    ),
    VitrinaDef(
        table="kpi_outflow_daily",
        title_ru="Отток, день",
        description_ru="Уход клиентов, завершившиеся договоры, завершившиеся заказы (календарный день expire_time; MAX по клиенту/договору).",
        date_column="report_date",
        dimension_cols=_DIM_STD,
        measures=(
            MeasureSum("churned_clients", "Уход клиентов (сумма)"),
            MeasureSum("completed_agreements", "Завершившиеся договоры (сумма)"),
            MeasureSum("completed_orders", "Завершившиеся заказы (сумма)"),
        ),
    ),
    VitrinaDef(
        table="kpi_revenue_active_month",
        title_ru="Выручка активных за месяц",
        description_ru="Ключ period_end — последний день месяца; выручка — сумма kpi_revenue_daily за месяц; paying_owners — active_clients из ARPU на min(день отчёта, конец месяца).",
        date_column="period_end",
        dimension_cols=_DIM_STD,
        measures=(
            MeasureSum("total_revenue", "Выручка (сумма)"),
            MeasureSum("paying_owners", "Активные клиенты AB0 (сумма)"),
            MeasureRatio(
                numerator_col="total_revenue",
                denominator_col="paying_owners",
                label_ru="ARPU (выручка / активные клиенты)",
            ),
        ),
    ),
    VitrinaDef(
        table="kpi_revenue_active_week",
        title_ru="Выручка активных за неделю",
        description_ru="Ключ period_end — воскресенье ISO-недели; сумма kpi_revenue_daily за неделю; paying_owners — active_clients ARPU на последний учтённый день.",
        date_column="period_end",
        dimension_cols=_DIM_STD,
        measures=(
            MeasureSum("total_revenue", "Выручка (сумма)"),
            MeasureSum("paying_owners", "Активные клиенты AB0 (сумма)"),
            MeasureRatio(
                numerator_col="total_revenue",
                denominator_col="paying_owners",
                label_ru="ARPU (выручка / активные клиенты)",
            ),
        ),
    ),
    VitrinaDef(
        table="kpi_revenue_active_quarter",
        title_ru="Выручка активных за квартал",
        description_ru="Ключ period_end — последний день квартала; сумма kpi_revenue_daily за квартал; paying_owners — active_clients ARPU на конец периода.",
        date_column="period_end",
        dimension_cols=_DIM_STD,
        measures=(
            MeasureSum("total_revenue", "Выручка (сумма)"),
            MeasureSum("paying_owners", "Активные клиенты AB0 (сумма)"),
            MeasureRatio(
                numerator_col="total_revenue",
                denominator_col="paying_owners",
                label_ru="ARPU (выручка / активные клиенты)",
            ),
        ),
    ),
    VitrinaDef(
        table="kpi_revenue_active_year",
        title_ru="Выручка активных за год",
        description_ru="Ключ period_end — 31.12; сумма kpi_revenue_daily за год; paying_owners — active_clients ARPU на конец периода.",
        date_column="period_end",
        dimension_cols=_DIM_STD,
        measures=(
            MeasureSum("total_revenue", "Выручка (сумма)"),
            MeasureSum("paying_owners", "Активные клиенты AB0 (сумма)"),
            MeasureRatio(
                numerator_col="total_revenue",
                denominator_col="paying_owners",
                label_ru="ARPU (выручка / активные клиенты)",
            ),
        ),
    ),
    VitrinaDef(
        table="dim_segment",
        title_ru="Справочник направлений деятельности",
        description_ru=(
            "Иерархия public.segments: segment_title = «родитель / … / узел» по parent_id. "
            "Подпись к segment_id в KPI-витринах и фильтрах дашбордов."
        ),
        date_column="refreshed_at",
        dimension_cols=("segment_id", "parent_id", "segment_title"),
        measures=(),
    ),
    VitrinaDef(
        table="dim_tariff",
        title_ru="Справочник тарифов",
        description_ru="Собирается в mart из DDS; в ClickHouse — полная копия витрины.",
        date_column="refreshed_at",
        dimension_cols=("tariff_id", "tariff_title"),
        measures=(),
    ),
)


def vitrina_by_table(name: str) -> VitrinaDef | None:
    for v in VITRINS:
        if v.table == name:
            return v
    return None


# Витрины для UI дашбордов (остальные KPI в NL2SQL / метаданных сохраняются).
DASHBOARD_TABLE = "kpi_ab0_daily"
FINANCIAL_DASHBOARD_TABLE = "kpi_revenue_daily"


def dashboard_vitrins() -> tuple[VitrinaDef, ...]:
    v = vitrina_by_table(DASHBOARD_TABLE)
    return (v,) if v else ()


def schema_nl2sql_block(*, db: str) -> str:
    """Текст схемы для системного промпта NL2SQL."""
    lines: list[str] = [
        f"База данных ClickHouse (только SELECT): `{db}`. Доступные таблицы:",
    ]
    for v in VITRINS:
        cols_desc: list[str] = []
        if v.table == "dim_tariff":
            cols_desc = [
                "refreshed_at DateTime64(6, 'UTC')",
                "tariff_id Int64",
                "tariff_title String",
            ]
        else:
            cols_desc = [v.date_column + " Date"]
            if v.table == "kpi_arpu_daily":
                cols_desc.append("month_start Date")
            for d in v.dimension_cols:
                if d.endswith("_id"):
                    cols_desc.append(f"{d} Nullable(Int64)")
                else:
                    cols_desc.append(f"{d} Nullable(String)")
            for m in v.measures:
                if isinstance(m, MeasureSum):
                    col = m.column
                    if col.endswith("_daily") or "revenue" in col:
                        ctype = "Float64"
                    else:
                        ctype = "Int64/Float64"
                    cols_desc.append(f"{col} ({ctype})")
                elif isinstance(m, MeasureRatio):
                    cols_desc.append(f"{m.numerator_col}, {m.denominator_col} для ARPU")
        lines.append(f"  — {db}.{v.table}: " + "; ".join(cols_desc))

    lines.append("")
    lines.append("Даты сравниваются через Date; для сумм использовать sum(); избегать SELECT * без LIMIT.")

    return "\n".join(lines)
