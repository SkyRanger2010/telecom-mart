"""Подписи дашборда «Финансовые показатели» из JSON."""

from __future__ import annotations

import json
import logging
from copy import deepcopy
from pathlib import Path
from typing import Any

_LOGGER = logging.getLogger(__name__)

_LABELS_PATH = Path(__file__).resolve().parent / "financial_dashboard_labels.json"

_DEFAULT: dict[str, Any] = {
    "page": {"title": "Финансовые показатели", "subtitle": ""},
    "filters": {},
    "buttons": {"refresh": "Обновить", "reset": "Сбросить"},
    "charts": {"arpu": {}, "normalized_revenue": {}},
    "messages": {},
}


def get_financial_dashboard_config() -> dict[str, Any]:
    if not _LABELS_PATH.is_file():
        return deepcopy(_DEFAULT)
    try:
        with _LABELS_PATH.open(encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise ValueError("config root must be object")
        return data
    except Exception as exc:  # noqa: BLE001
        _LOGGER.exception("financial_dashboard_labels.json: %s", exc)
        return deepcopy(_DEFAULT)
