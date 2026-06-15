# FFIEC item master table

A reference table containing one row per FFIEC data item that appears in
the non-POR Call Report Parquet datasets managed by `ffiec.pq`.

## Usage

``` r
ffiec_items
```

## Format

A tibble with one row per item and the following columns:

- item:

  FFIEC item identifier formed as `mnemonic + item_code` (e.g.,
  `"RCFD0010"`).

- mnemonic:

  Schedule mnemonic (e.g., `"RCFD"`, `"RCON"`).

- item_code:

  Four-character FFIEC item code (character).

- item_name:

  Human-readable item name, normalized for presentation.

- data_type:

  Arrow data type used for item.

## Details

Each item corresponds to a unique combination of mnemonic and item code
(e.g., `"RCFD0010"`). Item names are lightly normalized for presentation
(proper case for protected regulatory acronyms is preserved).

This table represents the union of items found in the Parquet datasets
(excluding POR schedules). Items defined in the MDRM but never observed
in the Parquet data are excluded.

## See also

[`ffiec_item_details`](https://iangow.github.io/ffiec.pq/reference/ffiec_item_details.md)

## Examples

``` r
ffiec_items
dplyr::filter(ffiec_items, stringr::str_detect(item_name, "Tier 1"))
```
