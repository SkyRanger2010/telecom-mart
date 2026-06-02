#!/usr/bin/env bash
# Запускается из docker-compose (root) перед стартом остальных сервисов Airflow.
set -euo pipefail

# entrypoint образа не вызывается — нет set_pythonpath_for_root_user. su -p сохраняет HOME=/root;
# CLI airflow (shebang /usr/python/bin/python3.12) тогда не видит site-packages в /home/airflow/.local.
AIRFLOW_USER_HOME_DIR="${AIRFLOW_USER_HOME_DIR:-/home/airflow}"
run_as_airflow() {
  HOME="${AIRFLOW_USER_HOME_DIR}" su -p airflow -c "$1"
}

mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins /opt/airflow/scripts
chown -R "${AIRFLOW_UID}:0" /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins /opt/airflow/scripts

export PGPASSWORD="${POSTGRES_PASSWORD}"
if ! psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc \
  "SELECT 1 FROM pg_database WHERE datname='${AIRFLOW_DB}'" | grep -q 1; then
  psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
    -c "CREATE DATABASE ${AIRFLOW_DB};"
fi

# Без -m на пустой БД Airflow 3.x вызывает initdb через ORM + stamp head — схема расходится
# с цепочкой alembic (нет revoked_token и др.). См. airflow.utils.db.upgradedb / --use-migration-files.
run_as_airflow 'airflow db migrate -m'

_revoked_ok() {
  psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${AIRFLOW_DB}" -tAc \
    "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='revoked_token' LIMIT 1" \
    | grep -qx 1
}

if ! _revoked_ok; then
  echo 'Airflow 3.2: после migrate нет таблицы revoked_token — повторный db migrate -m' >&2
  run_as_airflow 'airflow db migrate -m'
fi

if ! _revoked_ok; then
  echo "ERROR: БД ${AIRFLOW_DB} не соответствует Airflow 3.2 (нет revoked_token). Выполните airflow db migrate вручную или пересоздайте БД." >&2
  exit 1
fi

run_as_airflow "airflow users create --username $(printf '%q' "${AIRFLOW_ADMIN_USER}") --firstname Airflow --lastname Admin --role Admin --email $(printf '%q' "${AIRFLOW_ADMIN_EMAIL}") --password $(printf '%q' "${AIRFLOW_ADMIN_PASSWORD}")" || true
