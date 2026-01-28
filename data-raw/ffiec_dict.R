library(dplyr)
library(purrr)
library(arrow)
library(stringr)
library(stringi)
library(readr)
library(tidyr)

zip_path <- file.path(Sys.getenv("RAW_DATA_DIR"), "ffiec", "MDRM.zip")

# Helper: open a fresh connection each time
mdrm_con <- function() unz(zip_path, "MDRM_CSV.csv", open = "rb")

# --- get column names from the header row (your skip/n_max logic) ---
con <- mdrm_con(); on.exit(close(con), add = TRUE)
mdrm_sample <- readr::read_csv(con, skip = 1, n_max = 1, show_col_types = FALSE)

col_names <- ffiec.pq:::clean_por_cols(colnames(mdrm_sample))

dt_fmt <- "%m/%d/%Y %I:%M:%S %p"
sentinel <- "12/31/9999 12:00:00 AM"

# --- read the full file from inside the zip ---
con <- mdrm_con()
mdrm <- readr::read_csv(
  con,
  skip = 2,
  col_names = col_names,
  trim_ws = TRUE,
  col_types = readr::cols(.default = readr::col_character()),
  show_col_types = FALSE
) |>
  mutate(
    across(
      c(start_date, end_date),
      ~ readr::parse_date(
        .x,
        format = dt_fmt,
        na = c("", "NA", sentinel)
      )
    )
  ) |>
  mutate(item = paste0(mnemonic, item_code), .before = everything())

close(con)

escape_regex <- function(x) {
  str_replace_all(x, "([\\\\.^$|()\\[\\]{}*+?])", "\\\\\\1")
}

make_key <- function(term) {
  pat <- escape_regex(stringr::str_to_lower(term))

  left  <- "(?<![[:alnum:]_])"
  right <- "(?![[:alnum:]_])"

  paste0(left, pat, right)
}

make_key_word_boundary <- function(term) {
  pat <- escape_regex(str_to_lower(term))
  paste0("\\b", pat, "\\b")
}

make_initialisms <- function(canonical_terms) {
  keys <- purrr::map_chr(canonical_terms, make_key_word_boundary)
  setNames(canonical_terms, keys)
}

# Exclude "U.S." here since it's handled by fix_us()
canonical_initialisms <- c(
  "HCs", "ASU", "SSFA", "MMDA", "MMDAs", "ACH", "FDIC", "CECL",
  "PPP", "PPPLF", "Federal Reserve", "CMO", "CMOs", "REMICs", "MBS",
  "FR Y-9C", "FFIEC", "FNMA", "FHLMC", "GNMA", "LEI", "Schedule RC-Q",
  "RC-Q", "GCE", "GCEs", "IBF", "MBA", "RC-C", "FHLB", "IRAs", "Keogh",
  "Federal Deposit Insurance Act",
  "Federal Deposit Insurance Commission",
  "Federal Regulation K",
  "Federal Regulation D",
  "Federal Reserve Act",
  "Home Owners' Loan Act",
  "Federal Home Loan Bank",
  "HOLA",
  "QTL",
  "Internal Revenue Service",
  "Domestic Building and Loan Association",
  "IRS", "DBLA",
  "Federal"
)

initialisms <- make_initialisms(canonical_initialisms)

fix_initialisms <- function(x, dict) {
  purrr::reduce(
    names(dict),
    \(acc, pat) str_replace_all(acc, regex(pat, ignore_case = TRUE), dict[[pat]]),
    .init = x
  )
}

fix_us <- function(x) {
  str_replace_all(
    x,
    regex("(?<![[:alnum:]_])((?:non-)?)u\\.s\\.(?![[:alnum:]_])", ignore_case = TRUE),
    "\\1U.S."
  )
}

fix_lower_words <- function(x, words) {
  reduce(
    words,
    \(acc, w)
    str_replace_all(
      acc,
      regex(paste0("\\b", w, "\\b"), ignore_case = TRUE),
      w
    ),
    .init = x
  )
}

lower_words <- c(
  "addressees"
)

fix_item_name_case <- function(x) {
  x |>
    str_to_sentence() |>
    fix_us() |>
    fix_initialisms(initialisms) |>
    fix_lower_words(lower_words)
}

cols_union <-
  ffiec.pq::ffiec_list_pqs() |>
  filter(schedule != "por") |>
  pull(full_name) |>
  map(\(f) arrow::read_parquet(f, as_data_frame = FALSE)$schema$names) |>
  unlist() |>
  unique() |>
  sort()

ffiec_items <-
  mdrm |>
  filter(item %in% cols_union) |>
  select(item, mnemonic, item_code, item_name) |>
  mutate(item_name = fix_item_name_case(item_name)) |>
  distinct()

ffiec_item_details <-
  mdrm |>
  filter(item %in% cols_union) |>
  select(item, reporting_form, start_date, end_date, confidentiality,
         description,
         seriesglossary, itemtype) |>
  distinct()

pq_cols <- function(path) {
  arrow::read_parquet(path, as_data_frame = FALSE)$schema$names
}

ffiec_item_schedules <-
  ffiec.pq::ffiec_list_pqs() |>
  mutate(
    date = as.Date(str_extract(base_name, "\\d{8}"), format = "%Y%m%d"),
    cols = map(full_name, pq_cols)
  ) |>
  select(schedule, date, cols) |>
  unnest(cols, names_repair = "minimal") |>
  rename(item = cols) |>
  group_by(item, schedule) |>
  summarise(
    dates = list(sort(unique(date))),
    .groups = "drop"
  ) |>
  filter(item != "IDRSSD")

