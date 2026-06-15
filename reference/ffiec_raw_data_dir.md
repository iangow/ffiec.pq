# Resolve the FFIEC raw data repository directory

Resolves the root directory used for raw source data from an explicit
argument or the \`RAW_DATA_DIR\` environment variable. In an interactive
session, when neither is available, the helper asks the user to choose
or enter a directory and offers to persist it in either project-level
\`.Renviron\` or user-level \`~/.Renviron\`.

## Usage

``` r
ffiec_raw_data_dir(raw_data_dir = NULL, prompt = interactive(), fallback = ".")
```

## Arguments

- raw_data_dir:

  Optional raw data repository root.

- prompt:

  If \`TRUE\`, prompt interactively when \`raw_data_dir\` and
  \`RAW_DATA_DIR\` are both missing.

- fallback:

  Directory used as the default when prompting.

## Value

A directory path as a character string, or \`NULL\` when prompting is
disabled and no directory can be resolved.
