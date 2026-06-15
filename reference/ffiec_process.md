# Process FFIEC call report bulk data

High-level convenience wrapper that processes FFIEC Call Report bulk zip
files into Parquet format, optionally creating item-level metadata.

## Usage

``` r
ffiec_process(
  zipfiles = NULL,
  raw_data_dir = NULL,
  data_dir = NULL,
  schema = "ffiec",
  create_item_pqs = TRUE,
  keep_process_data = NULL,
  use_multicore = FALSE
)
```

## Arguments

- zipfiles:

  Optional character vector of FFIEC bulk zip file paths. If `NULL`, zip
  files are discovered automatically from the resolved raw data
  directory.

- raw_data_dir:

  Optional parent directory containing FFIEC bulk zip files. If provided
  and `schema` is not `NULL`, files are expected under
  `file.path(raw_data_dir, schema)`. If `NULL`, the environment variable
  `RAW_DATA_DIR` is used.

- data_dir:

  Optional parent directory for Parquet output. If provided and `schema`
  is not `NULL`, files are written under `file.path(data_dir, schema)`.
  If `NULL`, the environment variable `DATA_DIR` is used.

- schema:

  Schema name used to resolve input and output directories (default
  `"ffiec"`). If `NULL`, directories are resolved directly without
  appending a schema subdirectory.

- create_item_pqs:

  Logical; if `TRUE`, create or update FFIEC item metadata Parquet files
  as part of processing.

- keep_process_data:

  Logical; whether to write the processing log returned by
  `ffiec_process()` to `"ffiec_process_data.parquet"` in the resolved
  output directory. If `NULL`, defaults to `TRUE` when `zipfiles` is
  `NULL` and `FALSE` when `zipfiles` is supplied.

- use_multicore:

  Logical; whether to attempt parallel execution when reading Parquet
  metadata. If `TRUE` and the optional packages future and furrr are
  installed, operations are parallelized using a multisession plan.
  Defaults to `FALSE`.

## Value

A tibble describing written Parquet files.

## Details

Input zip files may be supplied explicitly or discovered automatically
from a resolved raw data directory. Output Parquet files are written to
a resolved data directory.
