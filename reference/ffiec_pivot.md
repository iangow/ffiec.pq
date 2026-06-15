# Pivot long FFIEC data to wide format using DuckDB

Creates a wide table from a DuckDB-backed lazy `tbl` containing long
FFIEC data (typically columns like `IDRSSD`, `date`, `item`, `value`).
This is a lightweight, fast alternative to \[tidyr::pivot_wider()\] that
pushes the pivot operation into DuckDB.

## Usage

``` r
ffiec_pivot(
  data,
  id_cols = c("IDRSSD", "date"),
  names_from = "item",
  values_from = "value",
  items = NULL,
  values_fn = "first"
)
```

## Arguments

- data:

  A DuckDB-backed lazy `tbl` containing the long data to pivot.

- id_cols:

  Character vector of identifier columns that define unique rows in the
  wide output (default `c("IDRSSD", "date")`).

- names_from:

  Character scalar giving the column whose values become output column
  names (default `"item"`).

- values_from:

  Character scalar giving the column that supplies cell values (default
  `"value"`).

- items:

  Optional character vector of values of `names_from` to include. When
  supplied, the input is filtered to `names_from %in% items` before
  pivoting, and DuckDB is instructed to pivot only those columns. This
  is recommended when you want a stable set/order of output columns.

- values_fn:

  Character scalar naming a DuckDB aggregate function used to resolve
  multiple values per `id_cols + names_from` combination. The default is
  `"any_value"`, which is appropriate when uniqueness holds. Other
  common choices include `"sum"`, `"max"`, or `"min"`.

## Value

A DuckDB-backed lazy `tbl` in wide format.

## Details

The function is designed for use with tables returned by
\[ffiec_scan_pqs()\] (or any DuckDB-backed lazy `tbl`). The DuckDB
connection is inferred from `data`.

This helper supports a minimal subset of the \[tidyr::pivot_wider()\]
interface: `data`, `id_cols`, `names_from`, and `values_from`, plus
optional `items` and `values_fn`.

For best results, ensure that `id_cols` combined with `names_from`
uniquely identify rows in `data`. If not, choose an appropriate
`values_fn` to aggregate duplicates.

## Examples

``` r
if (FALSE) { # \dontrun{
db <- DBI::dbConnect(duckdb::duckdb())

# Long table: IDRSSD, date, item, value
ffiec_float <- ffiec_scan_pqs(db, schedule = "ffiec_float")

fields <- c("RCFD0081", "RCON0081", "RCFD0071", "RCON0071")

wide <- ffiec_pivot(
  data = ffiec_float,
  id_cols = c("IDRSSD", "date"),
  names_from = "item",
  values_from = "value",
  items = fields,
  values_fn = "any_value"
)

wide |> dplyr::glimpse()
} # }
```
