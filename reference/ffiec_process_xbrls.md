# Process FFIEC XBRL bulk zip files

Reads FFIEC bulk XBRL zip files (each containing many `*.xbrl.xml` inner
files), extracts reported XBRL facts, and writes a Parquet file per
zipfile into the resolved Parquet output directory.

## Usage

``` r
ffiec_process_xbrls(
  zipfiles = NULL,
  raw_data_dir = NULL,
  data_dir = NULL,
  schema = "ffiec",
  use_multicore = FALSE,
  ns_prefix = "cc",
  prefix = ""
)
```

## Arguments

- zipfiles:

  Optional character vector of FFIEC bulk XBRL zip file paths. If
  `NULL`, zip files are discovered automatically in the resolved input
  directory (using `ffiec_list_zips(type = "xbrl")`).

- raw_data_dir:

  Optional parent directory containing FFIEC bulk zip files.

- data_dir:

  Optional parent directory for Parquet output.

- schema:

  Schema subdirectory name (default `"ffiec"`). Set to `NULL` to use
  `raw_data_dir` / `data_dir` directly.

- use_multicore:

  Logical; if `TRUE` and packages `future` and `furrr` are installed,
  process zip files in parallel using
  [`future::multisession()`](https://future.futureverse.org/reference/multisession.html).

- ns_prefix:

  XML namespace prefix to extract (default `"cc"`).

- prefix:

  Optional filename prefix for output Parquet file names.

## Value

A tibble with one row per processed zipfile, including the output
Parquet basename. The attribute `"out_dir"` contains the resolved output
directory.

## Details

Directory resolution follows the same rules as
[`ffiec_process()`](https://iangow.github.io/ffiec.pq/reference/ffiec_process.md):
`raw_data_dir` / `RAW_DATA_DIR` for inputs and `data_dir` / `DATA_DIR`
for outputs, optionally with a schema subdirectory when `schema` is
non-`NULL`.
