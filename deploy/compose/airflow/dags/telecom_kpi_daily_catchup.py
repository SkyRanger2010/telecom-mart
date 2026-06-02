"""Последовательная догонка ``telecom_kpi_daily`` по календарным дням.

Один прогон этого DAG запускает целевой DAG **по одному разу на каждый день inclusive**
из диапазона ``start_date`` … ``end_date``, **строго по порядку** (следующий день
начинается только после успешного завершения предыдущего прогона).

Источник диапазона (приоритет сверху вниз):

1. **Configuration JSON** при триггере, например::
       {"start_date": "2024-01-06", "end_date": "2024-01-15"}
2. **Params** DAG (если в UI заданы непустые строки).
3. Переменные окружения **на воркере/планировщике**::
       TELECOM_KPI_CATCHUP_START=2024-01-06
       TELECOM_KPI_CATCHUP_END=2024-01-15

Дополнительные ключи в ``conf`` (все опционально):

- ``poke_interval_seconds`` — пауза между опросами статуса (по умолчанию 60).
- ``timeout_seconds_per_day`` — максимальное ожидание одного дня в секундах
  (по умолчанию 86400).

Реализация: по умолчанию **Stable REST API** к ``api-server`` (без вызова CLI ``airflow``:
в изолированном Task SDK дочерний процесс часто падает с пустым metadata DB URL, даже если
в родительском процессе строка «есть» в ``airflow.configuration``).

Авторизация к ``/api/v2``: **Bearer JWT** — либо готовый ``TELECOM_KPI_CATCHUP_API_TOKEN``,
либо логин/пароль (``TELECOM_KPI_CATCHUP_API_*`` или ``AIRFLOW_ADMIN_*``) и запрос
``POST {API}/auth/token`` (стандарт FAB; Basic для API v2 не подходит — будет 401).

Принудительно CLI: ``TELECOM_KPI_CATCHUP_USE_CLI=1`` (нужен рабочий
``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`` в окружении подпроцесса).

Целевой DAG **не должен быть на паузе**. У ``telecom_kpi_daily`` ожидаем ``max_active_runs=1`` —
дочерние запросы всё равно выполняются последовательно этим DAG.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from datetime import date, timedelta
from typing import Any
from urllib.parse import quote

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

LOGGER = logging.getLogger(__name__)

RAW_TZ = "Europe/Moscow"
TARGET_DAG_ID = "telecom_kpi_daily"
START = pendulum.datetime(2024, 1, 1, tz=RAW_TZ)

# JWT из POST /auth/token (кэш на один прогон задачи; сбрасывается при 401).
_fab_jwt_cache: str | None = None


def _daterange_inclusive(start: date, end: date) -> list[date]:
    """Список календарных дней от ``start`` до ``end`` включительно.

    Args:
        start: Первый день.
        end: Последний день.

    Returns:
        Список дат в порядке возрастания.

    Raises:
        ValueError: Если ``start > end``.
    """
    if start > end:
        raise ValueError(f"start_date ({start}) позже end_date ({end})")
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _safe_run_id_fragment(value: str) -> str:
    """Санитизирует строку для использования в ``run_id`` Airflow.

    Приводит к нижнему регистру и заменяет недопустимые символы на ``_``.
    Допустимы: ``[a-z0-9_.-]``.

    Args:
        value: Исходная строка (например родительский run_id).

    Returns:
        Санитизированная строка.
    """
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789_-."
    return "".join((c if c in allowed else "_") for c in value.lower())


def _resolve_range_from_context(context: dict) -> tuple[date, date, int, int]:
    """Извлекает диапазон дат и параметры опроса из контекста задачи.

    Приоритет: Configuration JSON → Params → переменные окружения.

    Args:
        context: Контекст Airflow (dag_run, params).

    Returns:
        Кортеж ``(start_date, end_date, poke_interval_sec, timeout_sec_per_day)``.

    Raises:
        ValueError: Если диапазон не задан.
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}
    if not isinstance(conf, dict):
        conf = {}

    params_obj = context.get("params") or {}

    def _pick(*keys: str) -> str | None:
        """Возвращает первое непустое значение из conf или params по цепочке ключей.

        Приоритет: Configuration JSON → Params DAG.

        Args:
            *keys: Имена ключей для поиска (например ``"start_date"``, ``"from_date"``).

        Returns:
            Строковое значение или None.
        """
        for k in keys:
            v = conf.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
            v = params_obj.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
        return None

    start_s = _pick("start_date", "from_date")
    end_s = _pick("end_date", "to_date")
    if not start_s or not end_s:
        start_s = start_s or os.getenv("TELECOM_KPI_CATCHUP_START", "").strip()
        end_s = end_s or os.getenv("TELECOM_KPI_CATCHUP_END", "").strip()
    if not start_s or not end_s:
        raise ValueError(
            "Укажите диапазон: conf/params start_date и end_date (YYYY-MM-DD), "
            "или TELECOM_KPI_CATCHUP_START / TELECOM_KPI_CATCHUP_END в окружении."
        )

    start_d = date.fromisoformat(start_s)
    end_d = date.fromisoformat(end_s)

    poke = conf.get("poke_interval_seconds", params_obj.get("poke_interval_seconds", 60))
    tout = conf.get("timeout_seconds_per_day", params_obj.get("timeout_seconds_per_day", 86400))
    poke_i = int(poke) if poke is not None else 60
    tout_i = int(tout) if tout is not None else 86400
    if poke_i < 5:
        poke_i = 5
    if tout_i < 60:
        tout_i = 60

    return start_d, end_d, poke_i, tout_i


def _airflow_bin() -> str:
    """Возвращает путь к исполняемому файлу ``airflow``.

    Приоритет: ``TELECOM_KPI_CATCHUP_AIRFLOW_BIN`` → ``shutil.which`` → ``"airflow"``.

    Returns:
        Строка с путём или именем бинарника.
    """
    env = os.getenv("TELECOM_KPI_CATCHUP_AIRFLOW_BIN", "").strip()
    if env:
        return env
    return shutil.which("airflow") or "airflow"


def _resolve_sqlalchemy_conn_for_cli() -> str | None:
    """Строка подключения к метаданным для дочернего процесса ``airflow`` CLI."""
    for key in (
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "AIRFLOW_SQL_ALCHEMY_CONN",
    ):
        raw = os.getenv(key, "").strip()
        if raw:
            return raw
    try:
        from airflow.configuration import conf as airflow_conf

        raw = str(airflow_conf.get("database", "sql_alchemy_conn", fallback="")).strip()
        if raw:
            return raw
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("[DEBUG] catchup: airflow.configuration database sql_alchemy_conn: %s", exc)
    return None


def _subprocess_env_for_airflow_cli() -> dict[str, str]:
    """Сборка env для CLI: убираем пустые ключи SQL_ALCHEMY_CONN, чтобы не передавать в дочерний процесс ``''``."""
    env = {k: v for k, v in os.environ.items() if v is not None}
    for key in list(env.keys()):
        if "SQL_ALCHEMY_CONN" in key and not str(env.get(key, "")).strip():
            env.pop(key, None)
    conn = _resolve_sqlalchemy_conn_for_cli()
    if conn:
        env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = conn
    return env


def _use_rest_api() -> bool:
    """По умолчанию REST: см. docstring. CLI включается явно."""
    return os.getenv("TELECOM_KPI_CATCHUP_USE_CLI", "").strip().lower() not in {"1", "true", "yes", "on"}


def _rest_api_basic_credentials() -> tuple[str, str]:
    """Логин/пароль для POST /auth/token.

    Приоритет: ``TELECOM_KPI_CATCHUP_API_USER`` / ``TELECOM_KPI_CATCHUP_API_PASSWORD``,
    затем ``AIRFLOW_ADMIN_USER`` / ``AIRFLOW_ADMIN_PASSWORD``.

    Returns:
        Кортеж ``(username, password)``.
    """
    user = os.getenv("TELECOM_KPI_CATCHUP_API_USER", "").strip()
    password = os.getenv("TELECOM_KPI_CATCHUP_API_PASSWORD", "").strip()
    if user:
        return user, password
    return (
        os.getenv("AIRFLOW_ADMIN_USER", "").strip(),
        os.getenv("AIRFLOW_ADMIN_PASSWORD", "").strip(),
    )


def _has_rest_auth_credentials() -> bool:
    """Проверяет наличие учётных данных для REST API.

    Возвращает True если задан статический ``TELECOM_KPI_CATCHUP_API_TOKEN``
    или пара логин/пароль (через ``_rest_api_basic_credentials``).

    Returns:
        True если аутентификация возможна.
    """
    if os.getenv("TELECOM_KPI_CATCHUP_API_TOKEN", "").strip():
        return True
    u, p = _rest_api_basic_credentials()
    return bool(u and p)


def _api_base_url() -> str:
    """Базовый URL Airflow API (без завершающего слеша).

    По умолчанию: ``http://airflow-api-server:8080`` (сервис в Compose).
    Переопределение: ``TELECOM_KPI_CATCHUP_API_BASE``.

    Returns:
        Строка URL.
    """
    return os.getenv("TELECOM_KPI_CATCHUP_API_BASE", "http://airflow-api-server:8080").rstrip("/")


def _invalidate_jwt_cache() -> None:
    """Сбрасывает кэш JWT-токена (при 401 для автоматического перевыпуска)."""
    global _fab_jwt_cache
    _fab_jwt_cache = None


def _fab_fetch_jwt(username: str, password: str) -> str:
    """FAB: POST /auth/token → JWT для заголовка Authorization (см. providers-fab auth-manager/token)."""
    url = _api_base_url() + "/auth/token"
    payload = json.dumps({"username": username, "password": password}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"POST /auth/token HTTP {exc.code}: {err}") from exc

    if not isinstance(data, dict):
        raise RuntimeError(f"/auth/token: неожиданный ответ: {data!r}")
    token = data.get("access_token") or data.get("token")
    if not token:
        raise RuntimeError(f"/auth/token: нет access_token в ответе: {data!r}")
    return str(token)


def _bearer_token_for_api() -> str:
    """Статический токен из env или JWT, полученный по логину/паролю."""
    global _fab_jwt_cache
    fixed = os.getenv("TELECOM_KPI_CATCHUP_API_TOKEN", "").strip()
    if fixed:
        return fixed
    if _fab_jwt_cache:
        return _fab_jwt_cache
    u, p = _rest_api_basic_credentials()
    if not u or not p:
        raise RuntimeError(
            "Нужны TELECOM_KPI_CATCHUP_API_TOKEN или логин/пароль для POST /auth/token "
            "(TELECOM_KPI_CATCHUP_API_* либо AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD)."
        )
    _fab_jwt_cache = _fab_fetch_jwt(u, p)
    LOGGER.info("[INFO] catchup: JWT получен через POST /auth/token (user=%s)", u)
    return _fab_jwt_cache


def _api_request_headers(*, with_json_body: bool) -> dict[str, str]:
    """Собирает HTTP-заголовки для запросов к Airflow REST API.

    Включает ``Accept: application/json``, опционально ``Content-Type`` и
    ``Authorization: Bearer <token>`` (через ``_bearer_token_for_api``).

    Args:
        with_json_body: Добавлять ли ``Content-Type: application/json``.

    Returns:
        Словарь HTTP-заголовков.
    """
    h: dict[str, str] = {"Accept": "application/json"}
    if with_json_body:
        h["Content-Type"] = "application/json"
    h["Authorization"] = f"Bearer {_bearer_token_for_api()}"
    return h


def _api_json_request(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    _retry_after_jwt_refresh: bool = True,
) -> tuple[int, Any]:
    """HTTP-запрос к Airflow REST API с Bearer-аутентификацией.

    При 401 и отсутствии статического ``TELECOM_KPI_CATCHUP_API_TOKEN`` —
    автообновление JWT через ``POST /auth/token`` и повтор запроса.

    Args:
        method: HTTP-метод (GET, POST).
        path: Путь относительно ``_api_base_url()`` (например ``/api/v2/dags/...``).
        body: Тело JSON (для POST/PUT).
        _retry_after_jwt_refresh: Внутренний флаг для предотвращения бесконечной
            рекурсии при повторном 401.

    Returns:
        Кортеж ``(HTTP_status_code, parsed_json_body_or_None)``.

    Raises:
        RuntimeError: При HTTP-ошибках.
    """
    url = _api_base_url() + path
    has_body = body is not None and method.upper() != "GET"
    data = json.dumps(body).encode("utf-8") if has_body else None
    req = urllib.request.Request(
        url,
        data=data,
        headers=_api_request_headers(with_json_body=has_body),
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:  # noqa: S310 - URL из конфига инфраструктуры
            payload = resp.read().decode("utf-8")
            if not payload.strip():
                return resp.status, None
            return resp.status, json.loads(payload)
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        if (
            exc.code == 401
            and _retry_after_jwt_refresh
            and not os.getenv("TELECOM_KPI_CATCHUP_API_TOKEN", "").strip()
        ):
            _invalidate_jwt_cache()
            LOGGER.warning("[WARN] catchup: HTTP 401 — обновляю JWT через POST /auth/token и повторяю запрос")
            return _api_json_request(method, path, body, _retry_after_jwt_refresh=False)
        raise RuntimeError(f"HTTP {exc.code} {method} {path}: {err_body}") from exc


def _infer_state_from_cli_output(text: str) -> str | None:
    """Возвращает нижний регистр terminal state или None, если ещё не финиш."""
    t = (text or "").lower()
    # Порядок важен: success раньше, чем подстроки вроде upstream_failed.
    for token in ("upstream_failed", "failed", "success"):
        if token in t:
            return token
    return None


def _dag_run_state_cli(dag_id: str, run_id: str) -> str:
    """Запрашивает состояние DagRun через CLI ``airflow dags state``.

    Args:
        dag_id: Идентификатор DAG.
        run_id: Идентификатор прогона.

    Returns:
        Сырой вывод stdout+stderr как одна строка.
    """
    proc = subprocess.run(
        [_airflow_bin(), "dags", "state", dag_id, run_id],
        capture_output=True,
        text=True,
        check=False,
        env=_subprocess_env_for_airflow_cli(),
    )
    return ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()


def _dag_run_state_rest(dag_id: str, run_id: str) -> str:
    """Запрашивает состояние DagRun через REST API ``GET /api/v2/dags/{id}/dagRuns/{run_id}``.

    Args:
        dag_id: Идентификатор DAG.
        run_id: Идентификатор прогона.

    Returns:
        Строковое состояние (``"success"``, ``"failed"``, …) или JSON ответа.
    """
    path = f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/{quote(run_id, safe='')}"
    _status, payload = _api_json_request("GET", path)
    if isinstance(payload, dict) and payload.get("state") is not None:
        return str(payload.get("state", "")).lower()
    return json.dumps(payload) if payload is not None else ""


def _dag_run_state(dag_id: str, run_id: str, *, use_rest: bool) -> str:
    """Диспетчер: возвращает состояние DagRun через REST API или CLI.

    Args:
        dag_id: Идентификатор DAG.
        run_id: Идентификатор прогона.
        use_rest: True → REST API, False → CLI.

    Returns:
        Строковое представление состояния.
    """
    if use_rest:
        return _dag_run_state_rest(dag_id, run_id)
    return _dag_run_state_cli(dag_id, run_id)


def _trigger_child_cli(dag_id: str, run_id: str, conf_dict: dict) -> None:
    """Триггерит дочерний DAG через CLI ``airflow dags trigger``.

    Требует ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`` в окружении подпроцесса.

    Args:
        dag_id: Идентификатор целевого DAG.
        run_id: Уникальный run_id для дочернего прогона.
        conf_dict: Конфигурация (``process_date`` и др.).

    Raises:
        RuntimeError: Если нет строки подключения к БД или ошибка subprocess.
    """
    env = _subprocess_env_for_airflow_cli()
    if not env.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "").strip():
        raise RuntimeError(
            "Не задана строка подключения к БД метаданных для CLI. "
            "Задайте AIRFLOW__DATABASE__SQL_ALCHEMY_CONN в окружении исполнителя или "
            "переключитесь на REST: TELECOM_KPI_CATCHUP_USE_REST_API=1 и "
            "TELECOM_KPI_CATCHUP_API_TOKEN (или USER/PASSWORD для Basic) + TELECOM_KPI_CATCHUP_API_BASE."
        )
    cmd = [
        _airflow_bin(),
        "dags",
        "trigger",
        dag_id,
        "-c",
        json.dumps(conf_dict, ensure_ascii=False),
        "-r",
        run_id,
        "-o",
        "json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if proc.returncode != 0:
        raise RuntimeError(
            f"trigger failed rc={proc.returncode} stderr={proc.stderr!r} stdout={proc.stdout!r}"
        )


def _trigger_child_rest(dag_id: str, run_id: str, conf_dict: dict) -> None:
    """Триггерит дочерний DAG через REST API ``POST /api/v2/dags/{id}/dagRuns``.

    Args:
        dag_id: Идентификатор целевого DAG.
        run_id: Уникальный run_id для дочернего прогона.
        conf_dict: Конфигурация в теле запроса.
    """
    body: dict[str, Any] = {
        "dag_run_id": run_id,
        "logical_date": None,
        "conf": conf_dict,
    }
    path = f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns"
    _api_json_request("POST", path, body)


def _trigger_child(dag_id: str, run_id: str, conf_dict: dict, *, use_rest: bool) -> None:
    """Диспетчер: триггерит дочерний DAG через REST API или CLI.

    Args:
        dag_id: Идентификатор целевого DAG.
        run_id: Уникальный run_id.
        conf_dict: Конфигурация DagRun.
        use_rest: True → REST API, False → CLI.
    """
    if use_rest:
        _trigger_child_rest(dag_id, run_id, conf_dict)
    else:
        _trigger_child_cli(dag_id, run_id, conf_dict)


def _parse_terminal_state(raw: str, *, use_rest: bool) -> str | None:
    """None = ещё не терминальное состояние (или не распознано)."""
    if use_rest:
        t = (raw or "").strip().lower()
        if t in {"success", "failed", "upstream_failed"}:
            return t
        return None
    return _infer_state_from_cli_output(raw)


def _wait_until_terminal(
    dag_id: str,
    run_id: str,
    *,
    use_rest: bool,
    poke_interval: int,
    timeout_seconds: int,
) -> None:
    """Ожидает terminal state (success/failed) для указанного DagRun.

    Args:
        dag_id: Идентификатор целевого DAG.
        run_id: Идентификатор прогона.
        use_rest: Режим опроса через REST API (True) или CLI (False).
        poke_interval: Пауза между опросами в секундах.
        timeout_seconds: Максимальное время ожидания в секундах.

    Raises:
        RuntimeError: Если DagRun завершился с ошибкой.
        TimeoutError: Если превышен таймаут ожидания.
    """
    deadline = time.monotonic() + timeout_seconds
    terminal_ok = {"success"}
    terminal_bad = {"failed", "upstream_failed"}
    while time.monotonic() < deadline:
        raw = _dag_run_state(dag_id, run_id, use_rest=use_rest)
        token = _parse_terminal_state(raw, use_rest=use_rest)
        if token in terminal_ok:
            LOGGER.info("[INFO] catchup: %s run_id=%s -> %s", dag_id, run_id, token)
            return
        if token in terminal_bad:
            raise RuntimeError(f"DagRun {dag_id!r} {run_id!r} завершился с состоянием {token!r}")
        LOGGER.debug("[DEBUG] catchup wait: %s %s raw=%r parsed=%r", dag_id, run_id, raw[:500], token)
        time.sleep(poke_interval)

    raise TimeoutError(
        f"Таймаут ожидания DagRun {dag_id!r} run_id={run_id!r} за {timeout_seconds} с"
    )


def run_sequential_catchup(**context) -> None:
    """Основная логика DAG: последовательный запуск ``telecom_kpi_daily`` по дням.

    Для каждого дня inclusive:
    1. Формирует ``run_id`` на основе родительского run_id и даты.
    2. Триггерит ``telecom_kpi_daily`` с ``conf={"process_date": "YYYY-MM-DD"}``.
    3. Ожидает terminal state (success/failed) с опросом каждые ``poke_interval`` секунд.

    Триггер через REST API (по умолчанию) или CLI (``TELECOM_KPI_CATCHUP_USE_CLI=1``).

    Args:
        context: Контекст задачи Airflow.
    """
    start_d, end_d, poke_i, tout_i = _resolve_range_from_context(context)
    days = _daterange_inclusive(start_d, end_d)
    use_rest = _use_rest_api()

    if use_rest:
        if not _has_rest_auth_credentials():
            raise RuntimeError(
                "Режим REST (по умолчанию): задайте TELECOM_KPI_CATCHUP_API_TOKEN "
                "или пару логин/пароль (TELECOM_KPI_CATCHUP_API_* либо AIRFLOW_ADMIN_USER/"
                "AIRFLOW_ADMIN_PASSWORD в .env для compose). Или принудительно CLI: "
                "TELECOM_KPI_CATCHUP_USE_CLI=1 при рабочем AIRFLOW__DATABASE__SQL_ALCHEMY_CONN."
            )
        auth_mode = (
            "Bearer (TELECOM_KPI_CATCHUP_API_TOKEN)"
            if os.getenv("TELECOM_KPI_CATCHUP_API_TOKEN", "").strip()
            else "JWT через POST /auth/token (TELECOM_KPI_CATCHUP_API_* или AIRFLOW_ADMIN_*)"
        )
        LOGGER.info("[INFO] catchup: режим REST API base=%s auth=%s", _api_base_url(), auth_mode)
        _ = _bearer_token_for_api()
    else:
        db_ok = bool(_resolve_sqlalchemy_conn_for_cli())
        LOGGER.info(
            "[INFO] catchup: режим CLI (TELECOM_KPI_CATCHUP_USE_CLI); строка БД метаданных %s",
            "найдена (env/configuration)" if db_ok else "НЕ найдена — задайте URL или отключите USE_CLI",
        )

    parent = context["dag_run"]
    parent_rid = _safe_run_id_fragment(parent.run_id if parent else "manual")

    LOGGER.info(
        "[INFO] catchup: target=%s days=%s..%s count=%s poke=%ss timeout/day=%ss",
        TARGET_DAG_ID,
        start_d.isoformat(),
        end_d.isoformat(),
        len(days),
        poke_i,
        tout_i,
    )

    # Последовательный цикл: триггер → ожидание terminal state для каждого дня.
    for d in days:
        day_tag = d.isoformat()
        conf_payload = {"process_date": day_tag}
        child_run_id = f"catchup__{parent_rid}__{day_tag}"
        # Airflow ограничивает run_id 250 символами; обрезаем родительский фрагмент при необходимости.
        if len(child_run_id) > 240:
            child_run_id = f"catchup__{parent_rid[:180]}__{day_tag}"
        LOGGER.info("[INFO] catchup: triggering %s conf=%s run_id=%s", TARGET_DAG_ID, conf_payload, child_run_id)
        _trigger_child(TARGET_DAG_ID, child_run_id, conf_payload, use_rest=use_rest)
        _wait_until_terminal(
            TARGET_DAG_ID,
            child_run_id,
            use_rest=use_rest,
            poke_interval=poke_i,
            timeout_seconds=tout_i,
        )

    LOGGER.info("[INFO] catchup: finished %s days for %s", len(days), TARGET_DAG_ID)


_DOC_MD = __doc__ or ""

with DAG(
    dag_id="telecom_kpi_daily_catchup",
    description="Последовательная догонка telecom_kpi_daily по диапазону дат",
    doc_md=_DOC_MD,
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    tags=["telecom-mart", "raw", "dds", "mart", "kpi", "backfill"],
    params={
        "start_date": Param(
            default=None,
            type=["null", "string"],
            title="Первый день inclusive (YYYY-MM-DD)",
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            title="Последний день inclusive (YYYY-MM-DD)",
        ),
        "poke_interval_seconds": Param(default=60, type="integer", title="Интервал опроса state, сек"),
        "timeout_seconds_per_day": Param(
            default=86400,
            type="integer",
            title="Таймаут ожидания одного дня, сек",
        ),
    },
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
) as dag:
    sequential_catchup = PythonOperator(
        task_id="sequential_catchup_telecom_kpi_daily",
        python_callable=run_sequential_catchup,
    )
