# Check primary-key and non-NULL constraints in FFIEC Parquet files

Scans FFIEC Parquet files into DuckDB and verifies that a specified set
of columns jointly satisfies two integrity constraints:

## Usage

``` r
ffiec_check_pq_keys(
  conn,
  schedule = NULL,
  cols,
  data_dir = NULL,
  schema = "ffiec",
  prefix = ""
)
```

## Arguments

- conn:

  A valid DuckDB connection.

- schedule:

  Optional character scalar giving the FFIEC schedule to check (e.g.
  `"rc"`, `"rci"`). Passed to \[ffiec_scan_pqs()\].

- cols:

  Character vector of column names that together define the primary key
  and must also be non-missing.

- data_dir:

  Optional parent directory containing FFIEC Parquet files. If `NULL`,
  the directory is resolved using the `DATA_DIR` environment variable.

- schema:

  Schema name used to resolve the Parquet directory (default `"ffiec"`).
  Set to `NULL` to treat `data_dir` as the final directory.

- prefix:

  Optional filename prefix used when the Parquet files were created
  (default `""`).

## Value

A tibble with one row per checked schedule and columns:

- schedule:

  FFIEC schedule identifier.

- ok:

  Logical; `TRUE` if no violations were detected.

- null_violations:

  A tibble listing columns and counts of missing values, or empty if
  none were found.

- pk_violations:

  A tibble of duplicated key combinations, or empty if the key is
  unique.

## Details

- No missing values (non-NULL constraint)

- Uniqueness across rows (primary-key constraint)

The function operates lazily via DuckDB and only materializes rows
involved in violations. It is intended as a lightweight validation tool
for Parquet files produced by \[ffiec_process()\].

## Examples

``` r
if (FALSE) { # \dontrun{
library(duckdb)
con <- DBI::dbConnect(duckdb::duckdb())

# Check IDRSSD-date uniqueness for the RC schedule
ffiec_check_pq_keys(
  conn = con,
  schedule = "rc",
  cols = c("IDRSSD", "date")
)

# Check all schedules
schedules <- ffiec_list_pqs() |> dplyr::distinct(schedule) |> dplyr::pull()
results <- purrr::map_dfr(
  schedules,
  \(s) ffiec_check_pq_keys(con, s, c("IDRSSD", "date"))
)
} # }
```
