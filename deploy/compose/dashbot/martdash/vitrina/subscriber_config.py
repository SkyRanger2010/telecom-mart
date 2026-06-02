"""Подписи дашборда «Абонентская база» из JSON (см. subscriber_dashboard_labels.json)."""

from __future__ import annotations

import json
import logging
from copy import deepcopy
from pathlib import Path
from typing import Any

_LOGGER = logging.getLogger(__name__)

_LABELS_PATH = Path(__file__).resolve().parent / "subscriber_dashboard_labels.json"

_DEFAULT: dict[str, Any] = {
    "page": {"title": "Абонентская база", "subtitle": ""},
    "filters": {},
    "buttons": {"refresh": "Обновить", "reset": "Сбросить"},
    "charts": {"ab": {}, "flow": {}},
    "messages": {},
}


def get_subscriber_dashboard_config() -> dict[str, Any]:
    if not _LABELS_PATH.is_file():
        return deepcopy(_DEFAULT)
    try:
        with _LABELS_PATH.open(encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise ValueError("config root must be object")
        return data
    except Exception as exc:  # noqa: BLE001
        _LOGGER.exception("subscriber_dashboard_labels.json: %s", exc)
        return deepcopy(_DEFAULT)
