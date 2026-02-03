library(xml2)
library(dplyr)
library(purrr)
library(tibble)
library(tidyr)

#' @keywords internal
#' @noRd
process_xbrl_inner_file <- function(zipfile, inner_file, ns_prefix = "cc", prefix="") {

  path <- unz(zipfile, inner_file)
  doc <- read_xml(path)

  # all elements with the given prefix (e.g., <cc:...>)
  nodes <- xml_find_all(doc, paste0(".//", ns_prefix, ":*"))

  # helper: local tag name without the prefix
  local_name <- function(x) {
    nm <- xml_name(x)
    sub("^[^:]+:", "", nm)
  }

  df <-
    tibble(
      item       = map_chr(nodes, local_name),
      contextRef = xml_attr(nodes, "contextRef"),
      unitRef    = xml_attr(nodes, "unitRef"),
      decimals   = xml_attr(nodes, "decimals"),
      value      = xml_text(nodes),
      attrs      = map(nodes, \(n) xml_attrs(n))  # all attributes as named character vector
    )

  df |>
    separate_wider_regex(contextRef,
                         c(schedule = "^[^_]+", "_",
                           IDRSSD = "[0-9]+", "_",
                           date="[0-9]{4}-[0-9]{2}-[0-9]{2}")) |>
    mutate(n_attrs = lengths(attrs)) |>
    select(-attrs) |>
    select(IDRSSD, date, schedule, item, everything())
}

process_xbrl_zip <- function(zipfile, out_dir, prefix="") {

  date <-
    zipfile |>
    basename() |>
    stringr::str_extract("\\d{8}") |>
    as.Date(format = "%m%d%Y")

  date_raw <-
    date |>
    format("%Y%m%d")

  if (is.null(out_dir)) {
    stop("Provide `data_dir` or set DATA_DIR.", call. = FALSE)
  }

  inner_files <-
    unzip(zipfile, list = TRUE) |>
    filter(grepl("\\.xbrl\\.xml$", Name)) |>
    select(Name) |>
    pull()

  res <- map(inner_files, \(x) process_xbrl_inner_file(zipfile, x))
  df <- bind_rows(res)

  out_path <- file.path(out_dir, sprintf("%sxbrl_%s.parquet", prefix, date_raw))
  arrow::write_parquet(df, out_path)

  tibble::tibble(
    zipfile     = basename(zipfile),               # <-- merge schedule/kind
    date_raw    = date_raw,
    date        = date,
    parquet     = basename(out_path),
  )
}

ffiec_process_xbrls <- function(zipfiles = NULL,
                                raw_data_dir = NULL, data_dir = NULL,
                                schema = "ffiec",
                                use_multicore = FALSE) {

  dirs <- ffiec.pq:::resolve_dirs(
    raw_data_dir = raw_data_dir,
    data_dir = data_dir,
    schema = schema
  )
  in_dir  <- dirs$in_dir
  out_dir <- dirs$out_dir

  if (is.null(out_dir)) {
    stop("Provide `data_dir` or set `DATA_DIR`.", call. = FALSE)
  }

  # Only required if we need to discover zipfiles
  if (is.null(zipfiles) && is.null(in_dir)) {
    stop("Provide `zipfiles`, or `raw_data_dir`, or set RAW_DATA_DIR.", call. = FALSE)
  }

  if (is.null(zipfiles)) {
    zipfiles <- ffiec_list_zips(raw_data_dir = raw_data_dir,
                                schema = schema, type = "xbrl")$zipfile

    if (length(zipfiles) == 0L) {
      stop("No FFIEC XBRL zip files found in the resolved input directory.", call. = FALSE)
    }
  }

  zipfiles <- normalizePath(zipfiles, mustWork = TRUE)

  use_parallel <- isTRUE(use_multicore) &&
    requireNamespace("future", quietly = TRUE) &&
    requireNamespace("furrr", quietly = TRUE)

  out <- if (use_parallel) {

    old_plan <- future::plan()
    on.exit(future::plan(old_plan), add = TRUE)

    future::plan(future::multisession)

    furrr::future_map_dfr(
      zipfiles,
      process_xbrl_zip,
      out_dir = out_dir
    )

  } else {

    purrr::map_dfr(
      zipfiles,
      process_xbrl_zip,
      out_dir = out_dir
    )
  }

  attr(out, "out_dir") <- out_dir
  out
}




