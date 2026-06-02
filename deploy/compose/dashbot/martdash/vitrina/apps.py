from __future__ import annotations

import logging

from django.apps import AppConfig

_LOGGER = logging.getLogger(__name__)


class VitrinaConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "vitrina"
    verbose_name = "Витрины KPI"

    def ready(self) -> None:
        try:
            from vitrina.serving_flow_columns import ensure_flow_metric_columns_at_startup

            ensure_flow_metric_columns_at_startup()
        except Exception:  # noqa: BLE001
            _LOGGER.exception("vitrina.ready: ensure_flow_metric_columns_at_startup")
