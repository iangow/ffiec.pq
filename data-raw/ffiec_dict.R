# data-raw/ffiec_items.R

library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(readr)
library(arrow)

# ---- Paths ----
zip_path <- file.path(Sys.getenv("RAW_DATA_DIR"), "ffiec", "MDRM.zip")
stopifnot(file.exists(zip_path))

mdrm_con <- function() unz(zip_path, "MDRM_CSV.csv", open = "rb")

# ---- Read MDRM from zip ----
# con <- mdrm_con(); on.exit(close(con), add = TRUE)
mdrm_sample <- read_csv(
  con <- mdrm_con(),
  skip = 1,
  n_max = 1,
  show_col_types = FALSE)
close(con)

col_names <- ffiec.pq:::clean_por_cols(colnames(mdrm_sample))

dt_fmt   <- "%m/%d/%Y %I:%M:%S %p"
sentinel <- "12/31/9999 12:00:00 AM"

mdrm <- read_csv(
  con <- mdrm_con(),
  skip = 2,
  col_names = col_names,
  trim_ws = TRUE,
  col_types = cols(.default = col_character()),
  show_col_types = FALSE
) |>
  mutate(
    across(
      c(start_date, end_date),
      ~ parse_date(.x, format = dt_fmt, na = c("", "NA", sentinel))
    ),
    item = paste0(mnemonic, item_code),
    .before = everything()
  )
close(con)

# ---- Text normalization (light-touch) ----
escape_regex <- function(x) {
  str_replace_all(x, "([\\\\.^$|()\\[\\]{}*+?])", "\\\\\\1")
}

# ICU-safe token boundaries are generally more reliable than \\b, but since
# you decided to just list "Schedule RC-Q" etc. as canonical phrases, we can
# still use \\b for many terms. If you run into punctuation issues again,
# swap this to token boundaries.
make_key_word_boundary <- function(term) {
  pat <- escape_regex(str_to_lower(term))
  paste0("\\b", pat, "\\b")
}

make_key_token <- function(term) {
  pat <- escape_regex(str_to_lower(term))
  paste0("(?<![[:alnum:]_])", pat, "(?![[:alnum:]_])")
}

make_initialisms <- function(canonical_terms) {
  keys <- map_chr(canonical_terms, make_key_token)
  setNames(canonical_terms, keys)
}

canonical_initialisms <- c(
  # acronyms / initialisms / protected phrases
  "HCs", "ASU", "SSFA", "MMDA", "MMDAs", "ACH", "FDIC", "CECL",
  "PPP", "PPPLF", "Federal Reserve", "CMO", "CMOs", "REMICs", "MBS",
  "FR Y-9C", "FFIEC", "FNMA", "FHLMC", "GNMA", "LEI",
  "Tier 1", "Tier 2", "MSAs", "Puerto Rico",
  # schedules / codes you want forced
  "Schedule RC-E",
  "Schedule RC-Q", "RC-Q", "RC-C", "Schedule RC",
  "Schedule HC", "Schedule HC-L", "Schedule RI",
  # misc
  "DTAs", "DTLs",
  "GCE", "GCEs", "IBF", "MBA", "FHLB", "IRAs", "Keogh",
  "Federal Deposit Insurance Act",
  "Federal Deposit Insurance Commission",
  "Federal Regulation K",
  "Federal Regulation D",
  "Federal Reserve Act",
  "Home Owners' Loan Act",
  "Federal Home Loan Bank",
  "HOLA", "QTL", "GAAP",
  "Internal Revenue Service",
  "Domestic Building and Loan Association",
  "IRS", "DBLA",
  "Federal"
)

initialisms <- make_initialisms(canonical_initialisms)

fix_initialisms <- function(x, dict) {
  reduce(
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

lower_words <- c("addressees")

fix_lower_words <- function(x, words) {
  reduce(
    words,
    \(acc, w) str_replace_all(acc, regex(paste0("\\b", w, "\\b"), ignore_case = TRUE), w),
    .init = x
  )
}

fix_item_name_case <- function(x) {
  x |>
    str_to_sentence() |>
    fix_us() |>
    fix_initialisms(initialisms) |>
    fix_lower_words(lower_words)
}

# ---- Parquet schema helpers (arrow-version-proof) ----
pq_cols <- function(path) {
  arrow::read_parquet(path, as_data_frame = FALSE)$schema$names
}

# ---- List parquet files ----
pqs <- ffiec.pq::ffiec_list_pqs()

# Union of all columns across non-POR schedules
cols_union <-
  pqs |>
  filter(schedule != "por") |>
  pull(full_name) |>
  map(pq_cols) |>
  unlist(use.names = FALSE) |>
  unique() |>
  sort()

# ---- ffiec_items ----
ffiec_items <-
  mdrm |>
  filter(item %in% cols_union) |>
  select(item, mnemonic, item_code, item_name) |>
  mutate(item_name = fix_item_name_case(item_name)) |>
  distinct() |>
  arrange(item)

# ---- ffiec_item_details ----
ffiec_item_details <-
  mdrm |>
  filter(item %in% cols_union) |>
  select(
    item, reporting_form, start_date, end_date, confidentiality,
    description, seriesglossary, itemtype
  ) |>
  distinct() |>
  arrange(item, reporting_form, start_date)

# ---- ffiec_item_schedules ----
ffiec_item_schedules <-
  pqs |>
  filter(schedule != "por") |>
  mutate(
    date = as.Date(str_extract(base_name, "\\d{8}"), format = "%Y%m%d"),
    cols = map(full_name, pq_cols)
  ) |>
  select(schedule, date, cols) |>
  unnest(cols, names_repair = "minimal") |>
  rename(item = cols) |>
  filter(item != "IDRSSD") |>
  group_by(item, schedule) |>
  summarise(
    dates = list(sort(unique(date))),
    .groups = "drop"
  ) |>
  arrange(item, schedule)

# ---- Save as package data ----
usethis::use_data(ffiec_items, ffiec_item_details, ffiec_item_schedules,
                  overwrite = TRUE)

