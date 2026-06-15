# FFIEC item metadata and reporting details

A reference table describing FFIEC data items over time, including
reporting form, effective date ranges, confidentiality status, and
descriptive metadata as provided in the MDRM.

## Usage

``` r
ffiec_item_details
```

## Format

A tibble with one row per item–period–reporting form combination and the
following columns:

- item:

  FFIEC item identifier (e.g., `"RCFD0010"`).

- reporting_form:

  Reporting form to which the item applies (e.g., FFIEC 031, 041, 051).

- start_date:

  Date on which the item definition becomes effective.

- end_date:

  Date on which the item definition ceases to be effective (NA if still
  active).

- confidentiality:

  Confidentiality indicator from the MDRM.

- description:

  Long-form item description from the MDRM.

- seriesglossary:

  Series glossary text, if provided.

- itemtype:

  Item type classification from the MDRM.

## Details

Items may appear multiple times if their definitions or reporting
attributes change over time.

This table reflects MDRM metadata only for items that appear in the
Parquet datasets managed by `ffiec.pq`. Date fields are stored as `Date`
objects; MDRM sentinel dates (e.g., 9999-12-31) are treated as missing.

The `itemtype` column classifies items according to MDRM item type
codes. In the current Parquet datasets managed by `ffiec.pq`, the
following item types are observed:

- `F`:

  Financial / reported. The item is submitted directly by the reporter.

- `D`:

  Derived. The item is derived from other stored variables.

- `R`:

  Rate. The item is stored as a decimal value (e.g., 28% stored as
  0.28).

- `P`:

  Percentage. The item is stored as a percentage value (e.g., 28% stored
  as 28).

- `S`:

  Structure. The item describes an institutional characteristic.

Other MDRM item types defined by the Federal Reserve (e.g., projected or
examination/supervision items) do not appear in the Parquet datasets and
are therefore not represented in this table.

## See also

[`ffiec_items`](https://iangow.github.io/ffiec.pq/reference/ffiec_items.md)

## Examples

``` r
# Active definitions for a single item
dplyr::filter(ffiec_item_details, item == "RCFD2170", is.na(end_date))
```
