# List generated FFIEC Parquet files

Lists Parquet files produced by
[`ffiec_process()`](https://iangow.github.io/ffiec.pq/reference/ffiec_process.md)
and returns their filenames, full paths, and inferred FFIEC schedule
identifiers.

## Usage

``` r
ffiec_list_pqs(data_dir = NULL, schema = "ffiec", all_files = FALSE)
```

## Arguments

- data_dir:

  Optional parent directory for Parquet output. If provided and `schema`
  is not `NULL`, files are written under `file.path(data_dir, schema)`.
  If `NULL`, the environment variable `DATA_DIR` is used.

- schema:

  Character scalar identifying the schema subdirectory (default
  `"ffiec"`). Set to `NULL` to use the resolved directory directly
  without appending a schema subdirectory.

- all_files:

  Logical; if `TRUE`, return all Parquet files in the resolved
  directory. If `FALSE` (default), only files with names ending in
  `_YYYYMMDD.parquet` are returned.

## Value

A tibble with one row per Parquet file and columns:

- base_name:

  File name without directory.

- full_name:

  Full path to the Parquet file.

- schedule:

  Inferred FFIEC schedule identifier extracted from the file name.

## Examples

``` r
if (FALSE) { # \dontrun{
# List FFIEC Parquet files using DATA_DIR/ffiec
ffiec_list_pqs()

# List files from an explicit directory (no schema subdir)
ffiec_list_pqs(data_dir = "/data/parquet/ffiec", schema = NULL)
} # }
```
