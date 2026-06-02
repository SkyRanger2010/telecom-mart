-- Метаданные Iceberg JDBC catalog (Trino, iceberg.jdbc-catalog.schema-version=V1).
-- 1) Создаём iceberg_tables как в Iceberg schema V0 (без iceberg_type в CREATE).
-- 2) ALTER ADD COLUMN IF NOT EXISTS iceberg_type — до первого запроса Trino столбец уже есть,
--    JdbcCatalog#updateSchemaIfRequired находит его через DatabaseMetaData и не выполняет свой ALTER,
--    который иначе может дать SQLException и Trino INTERNAL_ERROR «Cannot check and eventually update SQL schema».

CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

-- PostgreSQL ≥ 11.
ALTER TABLE iceberg_tables ADD COLUMN IF NOT EXISTS iceberg_type VARCHAR(5);

-- В Iceberg DDL property_key допускает NULL; для PostgreSQL в составе PRIMARY KEY — NOT NULL.

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255) NOT NULL,
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);
