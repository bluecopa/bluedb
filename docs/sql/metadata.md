# Metadata & introspection

[← SQL index](README.md)

bluedb exposes its catalog through read-only `GLUE_*` tables you can query like
any other table.

## `GLUE_OBJECTS`

Every table and index, with the table's creation time.

```sql
SELECT OBJECT_NAME, OBJECT_TYPE FROM GLUE_OBJECTS;
```

| OBJECT_NAME | OBJECT_TYPE |
|-------------|-------------|
| users       | TABLE       |
| users_email | INDEX       |
| orders      | TABLE       |

Columns: `OBJECT_NAME`, `OBJECT_TYPE` (`TABLE` / `INDEX`), `CREATED` (timestamp,
tables only).

```sql
-- Tables created in the last hour
SELECT OBJECT_NAME
FROM GLUE_OBJECTS
WHERE OBJECT_TYPE = 'TABLE'
  AND CREATED > NOW() - INTERVAL 1 HOUR;
```

## `GLUE_TABLES`

One row per table.

```sql
SELECT TABLE_NAME FROM GLUE_TABLES;
```

## `GLUE_TABLE_COLUMNS`

One row per column, with ordinal position and flags.

```sql
SELECT TABLE_NAME, COLUMN_NAME, COLUMN_ID
FROM GLUE_TABLE_COLUMNS
WHERE TABLE_NAME = 'users';
```

## `GLUE_INDEXES`

One row per index.

```sql
SELECT TABLE_NAME, INDEX_NAME FROM GLUE_INDEXES;
```

!!! note
    These tables are read-only and reflect the live catalog. They are
    the supported way to introspect schema; there is no `information_schema`.
