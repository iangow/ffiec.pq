# ffiec.pq

`ffiec.pq` provides tools for processing FFIEC Call Report bulk data
into Parquet files that can be analyzed from R, Python, DuckDB, and
other columnar data tools.

The package is designed around a local data repository:

- raw FFIEC bulk zip files live under `RAW_DATA_DIR/ffiec`
- processed Parquet files are written under `DATA_DIR/ffiec`

If `RAW_DATA_DIR` or `DATA_DIR` is not set, `ffiec.pq` will ask for the
directory the first time it needs it in an interactive R session.

## Installation

``` r

install.packages("pak")
pak::pak("iangow/ffiec.pq")
```

## Basic workflow

Process the FFIEC bulk files:

``` r

library(ffiec.pq)

# If RAW_DATA_DIR or DATA_DIR is not already set, this will prompt for them.
results <- ffiec_process(use_multicore = FALSE)
```

Read the resulting Parquet data lazily through DuckDB:

``` r

library(DBI)
library(dplyr)
library(duckdb)

db <- dbConnect(duckdb::duckdb())

por <- ffiec_scan_pqs(db, "por")
por |>
  count(date) |>
  arrange(desc(date)) |>
  collect()
```

See the package website article “Data curation: the case of Call
Reports” for a longer discussion of obtaining, processing, and using the
data.
