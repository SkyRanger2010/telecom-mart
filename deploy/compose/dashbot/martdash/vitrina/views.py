"""Страницы витрин, drilldown и NL2SQL API."""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from django.core.serializers.json import DjangoJSONEncoder
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_POST

from vitrina.metadata import DASHBOARD_TABLE, FINANCIAL_DASHBOARD_TABLE, vitrina_by_table
from vitrina.subscriber_config import get_subscriber_dashboard_config
from vitrina.financial_base_data import build_financial_api_payload
from vitrina.financial_config import get_financial_dashboard_config
from vitrina.subscriber_base_data import (
    AB_SERIES_KEYS,
    FLOW_SERIES_DEFAULT_KEYS,
    build_subscriber_api_payload,
    parse_subscriber_base_filters,
)
from vitrina.nl2sql import nl2sql_request_sync
from vitrina.query_dashboard import load_query_dashboard_payload
from vitrina.queries import fetch_date_range, parse_iso_date_optional

_LOGGER = logging.getLogger(__name__)


def health(request) -> HttpResponse:
    return HttpResponse("ok\n", content_type="text/plain")


def index(request):
    """Главная страница: единый дашборд «Абонентская база»."""
    return subscriber_base(request)


def _dashboard_default_dates(
    request,
    *,
    table: str,
) -> tuple[date, date, date | None, date | None, bool]:
    meta = vitrina_by_table(table)
    range_mn: date | None = None
    range_mx: date | None = None
    if meta is not None:
        try:
            range_mn, range_mx = fetch_date_range(meta, filters={})
        except Exception:  # noqa: BLE001
            _LOGGER.exception("fetch_date_range %s", table)

    today = date.today()
    stub_from = today - timedelta(days=89)
    has_df = bool(str(request.GET.get("df") or "").strip())
    has_dt = bool(str(request.GET.get("dt") or "").strip())

    if not has_df and not has_dt:
        if range_mn and range_mx:
            df0, dt0 = range_mn, range_mx
        elif range_mn:
            df0 = dt0 = range_mn
        else:
            df0, dt0 = stub_from, today
    else:
        df0 = parse_iso_date_optional(request.GET.get("df"), stub_from)
        dt0 = parse_iso_date_optional(request.GET.get("dt"), today)
    return df0, dt0, range_mn, range_mx, not has_df and not has_dt


def subscriber_base(request):
    """Дашборд «Абонентская база»: два графика, фильтры, автообновление через API."""
    if vitrina_by_table(DASHBOARD_TABLE) is None:
        raise Http404("Витрина AB0 не настроена")

    df0, dt0, range_mn, range_mx, used_auto = _dashboard_default_dates(
        request, table=DASHBOARD_TABLE
    )
    dash_cfg = get_subscriber_dashboard_config()

    boot = {
        "apiUrl": reverse("subscriber_api"),
        "dateFrom": df0.isoformat(),
        "dateTo": dt0.isoformat(),
        "dataMinDate": range_mn.isoformat() if range_mn else None,
        "dataMaxDate": range_mx.isoformat() if range_mx else None,
        "abSeriesDefault": sorted(AB_SERIES_KEYS),
        "flowSeriesDefault": sorted(FLOW_SERIES_DEFAULT_KEYS),
    }
    return render(
        request,
        "vitrina/subscriber_base.html",
        {
            "dash_cfg": dash_cfg,
            "boot": boot,
            "date_from": df0,
            "date_to": dt0,
            "data_min_date": range_mn,
            "data_max_date": range_mx,
            "used_auto_date_range": used_auto,
        },
    )


def subscriber_api(request) -> JsonResponse:
    """JSON: графики AB/приток-отток и списки значений фильтров (GET, те же параметры, что у страницы)."""
    if vitrina_by_table(DASHBOARD_TABLE) is None:
        return JsonResponse({"ok": False, "error": "Витрина не настроена"}, status=404)

    df0, dt0, _mn, _mx, _auto = _dashboard_default_dates(request, table=DASHBOARD_TABLE)
    filt = parse_subscriber_base_filters(request.GET, date_from_default=df0, date_to_default=dt0)
    try:
        payload = build_subscriber_api_payload(filt, request.GET)
        return JsonResponse(payload, encoder=DjangoJSONEncoder)
    except Exception as ex:  # noqa: BLE001
        _LOGGER.exception("subscriber_api")
        return JsonResponse(
            {
                "ok": False,
                "error": str(ex),
                "config": get_subscriber_dashboard_config(),
            },
            status=500,
            encoder=DjangoJSONEncoder,
        )


def financial_dashboard(request):
    """Дашборд «Финансовые показатели»: ARPU и выручка по дням."""
    if vitrina_by_table(FINANCIAL_DASHBOARD_TABLE) is None:
        raise Http404("Витрина выручки не настроена")

    df0, dt0, range_mn, range_mx, used_auto = _dashboard_default_dates(
        request, table=FINANCIAL_DASHBOARD_TABLE
    )
    dash_cfg = get_financial_dashboard_config()
    boot = {
        "apiUrl": reverse("financial_api"),
        "dateFrom": df0.isoformat(),
        "dateTo": dt0.isoformat(),
        "dataMinDate": range_mn.isoformat() if range_mn else None,
        "dataMaxDate": range_mx.isoformat() if range_mx else None,
    }
    return render(
        request,
        "vitrina/financial.html",
        {
            "dash_cfg": dash_cfg,
            "boot": boot,
            "date_from": df0,
            "date_to": dt0,
            "data_min_date": range_mn,
            "data_max_date": range_mx,
            "used_auto_date_range": used_auto,
        },
    )


def financial_api(request) -> JsonResponse:
    if vitrina_by_table(FINANCIAL_DASHBOARD_TABLE) is None:
        return JsonResponse({"ok": False, "error": "Витрина не настроена"}, status=404)

    df0, dt0, _mn, _mx, _auto = _dashboard_default_dates(
        request, table=FINANCIAL_DASHBOARD_TABLE
    )
    filt = parse_subscriber_base_filters(request.GET, date_from_default=df0, date_to_default=dt0)
    try:
        payload = build_financial_api_payload(filt, request.GET)
        return JsonResponse(payload, encoder=DjangoJSONEncoder)
    except Exception as ex:  # noqa: BLE001
        _LOGGER.exception("financial_api")
        return JsonResponse(
            {
                "ok": False,
                "error": str(ex),
                "config": get_financial_dashboard_config(),
            },
            status=500,
            encoder=DjangoJSONEncoder,
        )


def query_dashboard(request):
    """Дашборд по запросу: NL2SQL (llm-api → sqlguard) → ClickHouse → визуализация."""
    return render(
        request,
        "vitrina/query_dashboard.html",
        {
            "api_url": reverse("query_dashboard_api"),
        },
    )


@require_POST
def query_dashboard_api(request) -> JsonResponse:
    raw = request.body.decode("utf-8").strip()
    if not raw:
        return JsonResponse({"ok": False, "error": "пустое тело"}, status=400)
    try:
        body = json.loads(raw)
    except json.JSONDecodeError:
        return JsonResponse({"ok": False, "error": "некорректный JSON"}, status=400)

    question = str(body.get("question", "")).strip()
    if not question and not body.get("request"):
        return JsonResponse({"ok": False, "error": "пустой запрос"}, status=400)

    request_override = body.get("request")
    if request_override is not None and not isinstance(request_override, dict):
        return JsonResponse({"ok": False, "error": "поле request должно быть объектом"}, status=400)

    defaults = body.get("defaults") if isinstance(body.get("defaults"), dict) else {}
    filters = body.get("filters") if isinstance(body.get("filters"), dict) else None

    result = load_query_dashboard_payload(
        question=question,
        date_from=defaults.get("date_from") or body.get("date_from"),
        date_to=defaults.get("date_to") or body.get("date_to"),
        filters=filters,
        request_override=request_override,
    )
    status = 200 if result.get("ok") else 422
    return JsonResponse(result, encoder=DjangoJSONEncoder, safe=False, status=status)


def detail(request, _table: str):
    """Старые URL витрин — на главный дашборд «Абонентская база»."""
    return redirect("vitrina_index")


@require_POST
def nl2sql_chat(request):
    raw = request.body.decode("utf-8").strip()
    if not raw:
        return JsonResponse({"ok": False, "error": "пустое тело"}, status=400)
    try:
        body = json.loads(raw)
    except json.JSONDecodeError:
        return JsonResponse({"ok": False, "error": "некорректный JSON"}, status=400)

    question = str(body.get("question", "")).strip()
    if not question and not body.get("request"):
        return JsonResponse({"ok": False, "error": "пустой вопрос"}, status=400)

    request_override = body.get("request")
    if request_override is not None and not isinstance(request_override, dict):
        return JsonResponse({"ok": False, "error": "поле request должно быть объектом"}, status=400)

    defaults = body.get("defaults") if isinstance(body.get("defaults"), dict) else {}
    filters = body.get("filters") if isinstance(body.get("filters"), dict) else None

    result = nl2sql_request_sync(
        question,
        date_from=defaults.get("date_from") or body.get("date_from"),
        date_to=defaults.get("date_to") or body.get("date_to"),
        filters=filters,
        request_override=request_override,
    )
    status = 200 if result.get("ok") else 422
    return JsonResponse(result, encoder=DjangoJSONEncoder, safe=False, status=status)
