# Finalize a parsed schedule when the strict read is clean

\`ffiec_finalize_if_clean()\` applies post-read transformations that are
only appropriate when a schedule has been read \*\*cleanly\*\* (i.e., no
warnings and no recorded parsing problems). When the read is not clean,
the function returns immediately without modifying the data and signals
that finalization was not performed.

Currently, the only finalization step is to apply FFIEC date overrides
via \[apply_ffiec_date_overrides()\], which can coerce certain date-like
fields according to \`ffiec_col_overrides\`.

## Usage

``` r
ffiec_finalize_if_clean(
  df,
  ok,
  warnings,
  problems,
  ffiec_col_overrides,
  zipfile,
  inner_file,
  repairs_applied = character(0),
  debug = FALSE
)
```

## Arguments

- df:

  A tibble returned by \[readr::read_tsv()\]. May be \`NULL\` if the
  read failed hard upstream.

- ok:

  Logical. Strict read status. Finalization occurs only when
  \`isTRUE(ok)\`.

- warnings:

  Character vector of warnings captured during the read. Included for
  downstream reporting/debugging.

- problems:

  Tibble of parsing problems from \[readr::problems()\]. Included for
  downstream reporting/debugging.

- ffiec_col_overrides:

  Named character vector describing FFIEC column-type overrides. Columns
  with override \`"D"\` are treated as date columns for the override
  pass.

- zipfile:

  Character scalar. Path to the ZIP being processed (passed through to
  \[apply_ffiec_date_overrides()\] for context/debug output).

- inner_file:

  Character scalar. Member file name within the ZIP (passed through to
  \[apply_ffiec_date_overrides()\] for context/debug output).

- repairs_applied:

  Character vector of repairs/events already recorded by earlier stages
  (e.g., \`"embedded-newline"\`, \`"fallback-slow-path"\`).

- debug:

  Logical. If \`TRUE\`, downstream override routines may emit diagnostic
  messages.

## Value

A list containing:

- ok:

  Logical. \`TRUE\` if finalization was performed; \`FALSE\` otherwise.

- df:

  The finalized data frame when \`ok = TRUE\`, otherwise the input
  \`df\` unchanged.

- repairs:

  Character vector of repairs/events, including any repairs reported by
  \[apply_ffiec_date_overrides()\].

- warnings:

  Warnings captured during the read (passed through).

- problems:

  Readr parsing problems (passed through).

## Details

This function intentionally performs \*\*no repairs\*\* when the strict
read is not clean. It is meant to be used as part of a two-phase
strategy:

1.  Read strictly and exit early when clean.

2.  Otherwise, fall back to a repair/canonicalization path and re-read.

## See also

\[apply_ffiec_date_overrides()\] for the date override logic.
