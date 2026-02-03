# data-raw/ffiec_schema.R

library(xml2)
library(dplyr, warn.conflicts = FALSE)
library(purrr)
library(tibble)
library(ffiec.pq)
library(DBI)
library(tidyr)

raw_dir <- file.path(Sys.getenv("RAW_DATA_DIR"), "ffiec")
zipfiles <- list.files(raw_dir, pattern = "^_.*", full.names = TRUE)

safe_close <- function(con) {
  try({
    if (isOpen(con)) close(con)
  }, silent = TRUE)
  invisible(NULL)
}

read_concepts_xsd <- function(zipfile) {
  contents <- unzip(zipfile, list = TRUE)$Name

  path <- contents[grepl("concepts\\.xsd$", contents)]
  stopifnot(length(path) == 1)

  con <- unz(zipfile, path)
  on.exit(safe_close(con), add = TRUE)

  xsd <- xml2::read_xml(con)

  nodes <- xml2::xml_find_all(
    xsd,
    ".//*[local-name()='element']"
  )

  if (length(nodes) == 0) {
    stop("No <element> nodes found in concepts.xsd in ", basename(zipfile))
  }

  purrr::map_dfr(
    nodes,
    ~ tibble::as_tibble(as.list(xml2::xml_attrs(.x)))
  )
}
ffiec_schema <-
  purrr::map(zipfiles, read_concepts_xsd) |>
  dplyr::bind_rows() |>
  dplyr::distinct() |>
  dplyr::select(dplyr::any_of(c("name", "type")))

db <- dbConnect(duckdb::duckdb())

ffiec_schema_db <- copy_to(db, ffiec_schema)

xbrl <- ffiec_scan_pqs(db, "xbrl") |> rename(name = item)
xbrl |>
  count(date) |>
  arrange(desc(date))

xbrl |>
  distinct(date) |>
  count()

# Boolean types ----
boolean_types_raw <-
  xbrl |>
  filter(is.na(unitRef),
         value %in% c("true", "false")) |>
  compute()

boolean_types <-
  boolean_types_raw |>
  distinct(name) |>
  mutate(type = "xbrli:booleanItemType") |>
  compute()

boolean_types_raw |>
  count(name, value) |>
  compute() |>
  pivot_wider(id_cols = name, names_from = value, values_from = n,
              names_prefix = "val_")

new_boolean_types <-
  boolean_types |>
  anti_join(ffiec_schema_db, by = join_by(name)) |>
  compute()

new_boolean_types

# String types ----
string_types <-
  xbrl |>
  filter(is.na(unitRef),
         !value %in% c("true", "false")) |>
  distinct(name) |>
  mutate(type = "xbrli:stringItemType") |>
  compute()

new_string_types <-
  string_types |>
  anti_join(ffiec_schema_db, by = join_by(name)) |>
  compute()

new_string_types |> count()

# Pure types ----
pure_types <-
  xbrl |>
  filter(unitRef == "PURE") |>
  distinct(name) |>
  mutate(type = "xbrli:pureItemType") |>
  compute()

# Check that these are always pure
pure_types |>
  inner_join(xbrl, by = join_by(name)) |>
  count(name, unitRef) |>
  count(unitRef)

new_pure_types <-
  pure_types |>
  anti_join(ffiec_schema_db,
            by = join_by(name)) |>
  compute()

new_pure_types

# Integer types ----
integer_types_raw <-
  xbrl |>
  filter(unitRef == "NON-MONETARY",
         decimals == 0) |>
  compute()

integer_types <-
  integer_types_raw |>
  distinct(name) |>
  anti_join(string_types, by = join_by(name)) |>
  anti_join(boolean_types, by = join_by(name)) |>
  anti_join(pure_types, by = join_by(name)) |>
  mutate(type = "xbrli:integerItemType") |>
  compute()

new_integer_types <-
  integer_types |>
  anti_join(ffiec_schema_db,
            by = join_by(name)) |>
  compute()

# Check everything works as an integer
integer_types_raw |>
  mutate(value_dbl = as.double(value),
         value_int = as.integer(value)) |>
  filter(value_dbl != value_int) |>
  count() |>
  collect()

# Other types ----
other_types <-
  xbrl |>
  distinct(name) |>
  anti_join(string_types, by = join_by(name)) |>
  anti_join(boolean_types, by = join_by(name)) |>
  anti_join(pure_types, by = join_by(name)) |>
  anti_join(integer_types, by = join_by(name)) |>
  mutate(type = "xbrli:monetaryItemType") |>
  compute()

new_other_types <-
  other_types |>
  anti_join(ffiec_schema,
            by = join_by(name),
            copy = TRUE) |>
  compute()

new_other_types |>
  count()

# Combine all new types ----
new_types <-
  new_string_types |>
  union(new_boolean_types) |>
  union(new_pure_types) |>
  union(new_integer_types) |>
  union(new_other_types)

# Check that there's no dupes
new_types |>
  count(name) |>
  count(n, name = "new_name")

ffiec_schema <-
  ffiec_schema_db |>
  union(new_types) |>
  collect()

usethis::use_data(ffiec_schema, overwrite = TRUE)
