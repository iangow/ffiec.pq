# List FFIEC bulk zip files

Scans a directory for FFIEC Call Report bulk zip files and returns a
tibble with zipfile paths and report dates parsed from the filenames.

## Usage

``` r
ffiec_list_zips(raw_data_dir = NULL, schema = "ffiec", type = "tsv")
```

## Arguments

- raw_data_dir:

  Optional parent directory containing FFIEC bulk zip files. If provided
  and `schema` is not `NULL`, files are expected under
  `file.path(raw_data_dir, schema)`. If `NULL`, the environment variable
  `RAW_DATA_DIR` is used.

- schema:

  Schema name used to resolve the input directory (default `"ffiec"`).
  Set to `NULL` to use the resolved directory directly without appending
  a schema subdirectory.

- type:

  Character scalar indicating the bulk file type to list. Supported
  values are:

  `"tsv"`

  :   Tab-delimited Call Report bulk files (filenames of the form
      `"FFIEC CDR Call Bulk All Schedules MMDDYYYY.zip"`).

  `"xbrl"`

  :   XBRL Call Report bulk files (filenames of the form
      `"FFIEC CDR Call Bulk XBRL MMDDYYYY.zip"`).

## Value

A tibble with columns:

- zipfile:

  Full path to the bulk zip file.

- date:

  Report date as a `Date`.

## Details

This function is typically used to discover bulk zip files prior to
calling \[ffiec_process()\].

Filenames are expected to contain an 8-digit `MMDDYYYY` date token,
which is parsed and returned as a `Date`. An error is raised if any
matching file does not conform to this naming convention.

## Examples

``` r
if (FALSE) { # \dontrun{
# List TSV bulk zip files using RAW_DATA_DIR/ffiec
ffiec_list_zips()

# List XBRL bulk zip files
ffiec_list_zips(type = "xbrl")

# List bulk zip files from an explicit directory (no schema subdirectory)
ffiec_list_zips(raw_data_dir = "/data/raw/ffiec", schema = NULL)
} # }
```
