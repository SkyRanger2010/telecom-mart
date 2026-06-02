# Конфигурация RAW-загрузчика (raw_load_config.json)

Основной конфигурационный файл для скриптов `load_raw_day.py`, `load_raw_period_batch.py`
и `load_raw_period_batched.py`.

## Структура

### `source` — подключение к PostgreSQL-источнику (HALK)

| Ключ | Назначение |
|------|-----------|
| `dsn_env` | Имя env-переменной с готовой строкой DSN (приоритет над отдельными параметрами) |
| `raw_load_source_id` | Идентификатор источника для lineage (колонка `raw_load_source`) |
| `host_env` / `port_env` / `db_env` / `user_env` / `password_env` / `sslmode_env` | Имена env-переменных для параметров подключения |
| `default_host` / `default_port` / `default_db` / `default_sslmode` | Значения по умолчанию, если env-переменные не заданы |
| `default_schema` | Схема по умолчанию для таблиц без явной схемы в `tables[]` |
| `log_schema` / `log_table` | Схема и таблица журнала изменений (обычно `public.global_log`) |

### `target` — подключение к Trino (Iceberg)

| Ключ | Назначение |
|------|-----------|
| `host_env` / `port_env` / `user_env` / `catalog_env` / `http_scheme_env` | Env-переменные для параметров Trino |
| `default_host` / `default_port` / `default_user` / `default_catalog` | Значения по умолчанию |
| `default_schema` | Схема по умолчанию в каталоге Iceberg (обычно `raw`) |
| `verify_connection_sql` | SQL для проверки подключения |

### `load` — параметры загрузки

| Ключ | Назначение |
|------|-----------|
| `batch_size` | Базовый размер батча (строк) |
| `source_fetch_batch_size` | Размер батча при чтении из PostgreSQL |
| `target_insert_batch_size` | Размер батча при записи в Trino |
| `batch_service.state_table` | Имя служебной таблицы состояния для period batch |
| `batch_service.loader_name` | Идентификатор загрузчика в служебной таблице |
| `batch_service.bootstrap_last_loaded_day` | Начальная дата для холодного старта (когда таблица состояния пуста) |
| `batch_service.daily_loader_name` | Идентификатор ежедневного загрузчика |

### `dds` — параметры DDS-слоя (используется другими скриптами)

| Ключ | Назначение |
|------|-----------|
| `schema` | Схема DDS |
| `state_table` | Служебная таблица состояния DDS |
| `loader_name` | Идентификатор загрузчика DDS |
| `scd2` | Включить SCD2 |

### `pipeline` — общие параметры конвейера

| Ключ | Назначение |
|------|-----------|
| `catalog` | Каталог Iceberg |
| `raw_schema` | Схема RAW-слоя |
| `validate_schema` | Схема для валидаций |
| `dds_schema` | Схема DDS-слоя |
| `mart_schema` | Схема витрин |

### `tables[]` — список таблиц для загрузки

Каждый элемент:
- `name` — полное имя `schema.table` или просто `table` (тогда используется `default_schema`)
- `mode` — `"incremental"` или `"full"`
- `primary_key` — список колонок PK (обязателен для `full`)

## Режимы загрузки

### `incremental`
Читает изменения из `global_log` за заданный период:
- События → `*__events` (дедупликация по `log_id`)
- Невалидные события → `*__events_invalid`
- Актуальное состояние → `*__state` (зеркало таблицы)

### `full`
Полный снимок таблицы на заданную дату:
- Данные → `*__snapshot` (партиция `snapshot_day`)
- Требует `primary_key` для построения `source_pkey`

## Env-переменные для тюнинга

| Переменная | По умолчанию | Назначение |
|-----------|-------------|-----------|
| `RAW_LOADER_MAX_EVENT_ROWS_PER_MERGE` | 20000 | Макс. строк в MERGE событий |
| `RAW_LOADER_MAX_TRINO_ROWS_PER_WRITE` | 20000 | Верхний предел строк на запись |
| `RAW_LOADER_TRINO_WRITE_RETRY_ATTEMPTS` | 6 | Число ретраев при ошибках сети |
| `RAW_LOADER_TRINO_WRITE_RETRY_BASE_SECONDS` | 2.0 | Базовая задержка ретрая |
| `RAW_LOADER_TRINO_WRITE_RETRY_MAX_SLEEP_SECONDS` | 30.0 | Макс. задержка ретрая |
| `RAW_LOADER_ICEBERG_HOTSPOT_RETRY_ATTEMPTS` | max(12, RETRY*2) | Ретраи для горячих таблиц |
| `RAW_LOADER_PREFER_FULL_UPDATE_PAYLOAD` | 1 | Использовать new_values вместо diff для UPDATE |
| `RAW_LOADER_UNIFIED_UPDATE_MERGE` | 1 | Унифицированный MERGE для UPDATE |
| `RAW_LOADER_LOG_LEVEL` | INFO | Уровень логирования |
