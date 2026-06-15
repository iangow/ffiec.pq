# Scan FFIEC Parquet files into DuckDB

Create a lazy `tbl` backed by DuckDB by scanning one or more FFIEC
Parquet files. Files may be selected either by FFIEC schedule name or by
an explicit Parquet filename or glob pattern. No data is read eagerly;
evaluation occurs only when the result is collected.

## Usage

``` r
ffiec_scan_pqs(
  conn,
  schedule = NULL,
  pq_file = NULL,
  data_dir = NULL,
  schema = "ffiec",
  prefix = "",
  union_by_name = TRUE,
  keep_filename = FALSE
)
```

## Arguments

- conn:

  A valid DuckDB connection.

- schedule:

  Optional character scalar giving the FFIEC schedule name (e.g. `"rc"`,
  `"rci"`, `"rcb"`). All matching Parquet files of the form
  `{prefix}{schedule}_YYYYMMDD.parquet` are scanned.

- pq_file:

  Optional character scalar giving a Parquet filename or glob pattern.
  May be a base name or relative path within the resolved Parquet
  directory, or a fully qualified path when `data_dir` is not supplied.

- data_dir:

  Optional parent directory for Parquet files. If `NULL`, the
  environment variable `DATA_DIR` is used.

- schema:

  Schema name used to resolve the Parquet directory (default `"ffiec"`).
  Set to `NULL` to treat `data_dir` (or `DATA_DIR`) as the final Parquet
  directory.

- prefix:

  Optional filename prefix used when the Parquet files were created
  (default `""`).

- union_by_name:

  Logical; whether to union Parquet files by column name when scanning
  multiple files (passed to DuckDB's `read_parquet()`). Default is
  `TRUE`.

- keep_filename:

  Logical; whether to keep the original Parquet file name (passed to
  DuckDB's `read_parquet()`). Default is `FALSE`.

## Value

A lazy `tbl` backed by DuckDB.

## Details

This function is intended for use with Parquet files produced by
\[ffiec_process()\]. It validates that the requested files exist on disk
before constructing the DuckDB query.

Exactly one of `schedule` or `pq_file` must be supplied.

**Directory resolution**

By default, Parquet files are read from a schema subdirectory:
`file.path(data_dir, schema)`. If `data_dir` is `NULL`, `DATA_DIR` is
used as the parent directory.

If `schema = NULL`, no schema subdirectory is appended; in this case,
`data_dir` (or `DATA_DIR`) is treated as the final directory containing
Parquet files.

When `schedule` is used, files are selected with the glob pattern
`{prefix}{schedule}_*.parquet` in the resolved directory.

When `pq_file` is used, the value is treated as a filename or glob
pattern relative to the resolved directory. If neither `data_dir` nor
`DATA_DIR` is available, `pq_file` may instead be a fully qualified path
to an existing Parquet file.

A fast filesystem check is performed using
[`Sys.glob()`](https://rdrr.io/r/base/Sys.glob.html). If no matching
files are found, the function errors before issuing any DuckDB query.
