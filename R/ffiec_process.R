## FFIEC processing ----

#' Combine multipart schedules by key
#'
#' Performs a full join across a list of data frames, coalescing overlapping
#' columns (beyond the join key) where present.
#'
#' @param dfs List of data frames to join.
#' @param key Join key column name. Default is \code{"IDRSSD"}.
#'
#' @return A data frame containing the combined columns/rows.
#' @keywords internal
#' @noRd
combine_call_parts <- function(dfs, key = "IDRSSD") {
  stopifnot(length(dfs) >= 1)

  Reduce(function(x, y) {
    z <- dplyr::full_join(x, y, by = key, suffix = c("", ".y"))

    overlap <- intersect(names(x), names(y))
    overlap <- setdiff(overlap, key)

    for (nm in overlap) {
      y_nm <- paste0(nm, ".y")
      if (y_nm %in% names(z)) {
        z[[nm]] <- dplyr::coalesce(z[[nm]], z[[y_nm]])
        z[[y_nm]] <- NULL
      }
    }

    z
  }, dfs)
}

#' Read schedule files for a schedule/date and combine parts
#'
#' Reads one or more internal TSV files from a bulk zip, combines multipart
#' schedules by \code{IDRSSD}, appends a \code{date} column, and converts
#' percent-encoded pure item columns to numeric proportions when applicable.
#'
#' @param zipfile Path to the bulk FFIEC zip file.
#' @param files_tbl 1+ row tibble subset for a single schedule/date.
#' @param schema Arrow schema mapping with at least columns \code{name} and \code{type}.
#' @param xbrl_to_readr Named character vector mapping XBRL types to readr spec codes.
#'
#' @return A tibble of the combined schedule.
#' @keywords internal
#' @noRd
read_schedule_all_parts <- function(zipfile, files_tbl, schema, xbrl_to_readr) {
  dfs <- purrr::map(
    files_tbl$file,
    ~ read_call_from_zip(zipfile, .x, schema, xbrl_to_readr)
  )

  repairs <- ffiec_union_repairs(dfs)

  ok <- all(purrr::map_lgl(dfs, ffiec_get_ok))

  df <- combine_call_parts(dfs, key = "IDRSSD") |>
    dplyr::mutate(date = files_tbl$date[[1]])

  df <- fix_pure_percent_cols(df, schema)

  # attach repairs (existing) + attach ok (new)
  df <- ffiec_set_repairs(df, repairs)
  attr(df, "ok") <- ok

  df
}

#' @keywords internal
#' @noRd
ffiec_get_repairs <- function(x) {
  r <- attr(x, "repairs", exact = TRUE)
  if (is.null(r)) character(0) else unique(as.character(r))
}

#' @keywords internal
#' @noRd
ffiec_union_repairs <- function(xs) {
  unique(unlist(lapply(xs, ffiec_get_repairs), use.names = FALSE))
}

#' @keywords internal
#' @noRd
ffiec_set_repairs <- function(x, repairs) {
  attr(x, "repairs") <- unique(as.character(repairs))
  x
}

# ---- INTERNAL: schedules ----

#' Resolve and validate multipart count for a schedule/date group
#'
#' Uses the filename-encoded multipart information (if present) to determine
#' the expected number of parts and validates it against the number of files
#' actually found in the zip.
#'
#' @param files_tbl A tibble corresponding to a single schedule/date group
#'   (i.e., one element of group_split()).
#' @param zipfile Path to the zip file (used only for error messages).
#'
#' @return An integer scalar giving the resolved number of parts.
#' @keywords internal
#' @noRd
resolve_n_parts <- function(files_tbl, zipfile) {
  stopifnot(nrow(files_tbl) >= 1)

  found_parts <- nrow(files_tbl)

  # Claimed multipart count from filenames (may be all NA)
  claimed <- files_tbl$n_parts[!is.na(files_tbl$n_parts)]
  claimed_parts <- if (length(claimed)) max(claimed) else NA_integer_

  if (!is.na(claimed_parts) && claimed_parts != found_parts) {
    stop(
      sprintf(
        "Multipart count mismatch for schedule '%s' (%s) in zip '%s': claimed n_parts=%d, found %d file(s).",
        files_tbl$schedule[[1]],
        files_tbl$date_raw[[1]],
        basename(zipfile),
        claimed_parts,
        found_parts
      ),
      call. = FALSE
    )
  }

  dplyr::coalesce(claimed_parts, found_parts)
}

#' Process schedule files from a bulk zip into Parquet
#'
#' Reads all schedule files within a bulk zip, combines multipart schedules by
#' \code{IDRSSD}, fixes percent-encoded pure item columns, and writes one Parquet
#' file per schedule-date combination.
#'
#' @param zipfile Path to the bulk zip file.
#' @param inside_files A tibble as returned by \code{get_cr_files()}.
#' @param schema Arrow schema mapping.
#' @param xbrl_to_readr XBRL-to-readr type mapping.
#' @param out_dir Output directory for Parquet files.
#' @param prefix Prefix to be appended to the beginning of parquet file names.
#'
#' @return A tibble with columns \code{schedule}, \code{date_raw}, \code{date},
#'   \code{parquet}, and \code{n_parts}.
#' @keywords internal
#' @noRd
process_zip_schedules <- function(zipfile, inside_files, schema,
                                  xbrl_to_readr, out_dir, prefix="") {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  targets <- inside_files |>
    dplyr::filter(!is.na(.data$schedule)) |>
    dplyr::mutate(schedule = tolower(.data$schedule)) |>
    dplyr::arrange(.data$schedule, .data$date_raw, .data$part, .data$file)

  groups <- dplyr::group_split(targets, .data$schedule, .data$date_raw, .keep = TRUE)

  empty_results <- tibble::tibble(
    type        = character(),
    kind        = character(),
    date_raw    = character(),
    date        = as.Date(character()),
    parquet     = character(),
    zipfile     = character(),
    n_parts     = integer(),
    repairs     = list(),
    ok          = logical(),
    inner_files = list()
  )

  results <- purrr::map_dfr(groups, function(g) {
    schedule <- g$schedule[[1]]
    date_raw <- g$date_raw[[1]]
    date     <- g$date[[1]]

    # Validate multipart structure and resolve number of parts
    n_parts <- resolve_n_parts(g, zipfile)

    if (n_parts > 1) {
      # Missing part numbers
      if (any(is.na(g$part))) {
        stop(
          sprintf(
            "Missing part number for multipart schedule '%s' (%s) in zip '%s'.",
            schedule, date_raw, basename(zipfile)
          ),
          call. = FALSE
        )
      }

      # Duplicate part numbers
      if (anyDuplicated(g$part)) {
        stop(
          sprintf(
            "Duplicate part numbers for schedule '%s' (%s) in zip '%s'.",
            schedule, date_raw, basename(zipfile)
          ),
          call. = FALSE
        )
      }

      # Contiguity check: parts must be exactly 1..n_parts
      expected <- seq_len(n_parts)
      actual   <- sort(g$part)

      if (!identical(actual, expected)) {
        stop(
          sprintf(
            "Non-contiguous part numbers for schedule '%s' (%s) in zip '%s': expected {%s}, found {%s}.",
            schedule, date_raw, basename(zipfile),
            paste(expected, collapse = ", "),
            paste(actual, collapse = ", ")
          ),
          call. = FALSE
        )
      }

      g <- dplyr::arrange(g, .data$part)
    }

    # Read + combine parts (single group g)
    df <- read_schedule_all_parts(zipfile, g, schema, xbrl_to_readr)
    ok <- ffiec_get_ok(df)
    repairs <- ffiec_get_repairs(df)

    out_path <- file.path(out_dir, sprintf("%s%s_%s.parquet", prefix, schedule, date_raw))
    arrow::write_parquet(df, out_path)

    tibble::tibble(
      type        = "schedule",
      kind        = schedule,                 # <-- merge schedule/kind
      date_raw    = date_raw,
      date        = date,
      parquet     = out_path,
      zipfile     = basename(zipfile),
      n_parts     = n_parts,
      repairs     = list(repairs),
      ok          = ok,
      inner_files = list(basename(g$file))    # <-- list-column (1+ files)
    )
  })

  results <- dplyr::bind_rows(empty_results, results)

  list(
    out_dir = out_dir,
    files   = results
  )
}

# ---- POR helpers ----

#' Split a tab-delimited line into fields
#'
#' @param line A single character string.
#'
#' @return Character vector of fields.
#' @keywords internal
#' @noRd
split_tsv_line <- function(line) strsplit(line, "\t", fixed = TRUE)[[1]]

#' Process non-schedule POR files from a bulk zip into Parquet
#'
#' @param zipfile Path to the bulk zip file.
#' @param inside_files A tibble as returned by \code{get_cr_files()}.
#' @param out_dir Output directory for Parquet files.
#' @param prefix Prefix appended to Parquet file names.
#'
#' @return A tibble with \code{kind}, \code{date}, \code{date_raw}, \code{inner_file}, \code{parquet}.
#' @keywords internal
#' @noRd
process_zip_por <- function(zipfile, inside_files, out_dir, prefix="") {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  targets <- inside_files |>
    dplyr::filter(is.na(.data$schedule))

  if (nrow(targets) == 0) {
    return(tibble::tibble())
  }

  purrr::map_dfr(seq_len(nrow(targets)), function(i) {
    row <- targets[i, ]
    inner_file <- row$file[[1]]
    date <- row$date[[1]]
    date_raw <- row$date_raw[[1]]
    kind <- por_kind(inner_file)

    df <- read_por_from_zip(zipfile, inner_file) |>
      dplyr::mutate(
        date = date,
        dplyr::across(
          dplyr::any_of(c(
            "financial_institution_zip_code",
            "fdic_certificate_number",
            "occ_charter_number",
            "ots_docket_number",
            "primary_aba_routing_number"
          )),
          parse_id_zero_na
        )
      )

    out_path <- file.path(
      out_dir,
      sprintf("%s_%s.parquet", kind, date_raw)
    )
    arrow::write_parquet(df, out_path)

    tibble::tibble(
      type        = "por",
      kind        = kind,
      date        = date,
      date_raw    = date_raw,
      parquet     = basename(out_path),
      zipfile     = basename(zipfile),
      n_parts     = 1,
      repairs     = list(character(0)),
      inner_files = list(basename(inner_file))
    )
  })
}

#' @keywords internal
#' @noRd
resolve_dirs <- function(raw_data_dir = NULL, data_dir = NULL,
                         schema = "ffiec") {

  in_dir  <- resolve_in_dir(raw_data_dir = raw_data_dir, schema = schema)
  out_dir <- resolve_out_dir(data_dir = data_dir, schema = schema)

  if (!is.null(in_dir))  in_dir  <- normalizePath(in_dir,  mustWork = TRUE)
  if (!is.null(out_dir)) out_dir <- normalizePath(out_dir, mustWork = FALSE)

  list(in_dir = in_dir, out_dir = out_dir)
}

#' @keywords internal
#' @noRd
resolve_in_dir <- function(raw_data_dir = NULL, schema = NULL) {
  base <- if (!is.null(raw_data_dir) && nzchar(raw_data_dir)) {
    raw_data_dir
  } else {
    Sys.getenv("RAW_DATA_DIR", unset = "")
  }

  if (!nzchar(base)) return(NULL)
  if (is.null(schema)) base else file.path(base, schema)
}

#' @keywords internal
#' @noRd
resolve_out_dir <- function(data_dir = NULL, schema = NULL) {
  base <- if (!is.null(data_dir) && nzchar(data_dir)) {
    data_dir
  } else {
    Sys.getenv("DATA_DIR", unset = "")
  }

  if (!nzchar(base)) return(NULL)
  if (is.null(schema)) base else file.path(base, schema)
}

# ---- PUBLIC: process one zip / many zips ----

#' Process one FFIEC bulk zip file
#'
#' @param zipfile Path to a bulk FFIEC zip file.
#' @param out_dir Output directory for Parquet files. If \code{NULL}, uses
#'   \code{DATA_DIR} and \code{schema}.
#' @param schema Subdirectory name under \code{DATA_DIR} (and \code{RAW_DATA_DIR} in
#'   the multi-zip function). Defaults to \code{"ffiec"}.
#' @keywords internal
#' @noRd
process_ffiec_zip <- function(zipfile, out_dir = NULL) {

  schema_tbl   <- ffiec_schema
  xbrl_to_readr <- default_xbrl_to_readr()

  inside <- get_cr_files(zipfile)

  temp_dir <- tempdir()

  # ---- schedules (returns list(out_dir=..., files=...)) ----
  sched_res <- process_zip_schedules(
    zipfile       = zipfile,
    inside_files  = inside,
    schema        = schema_tbl,
    xbrl_to_readr = xbrl_to_readr,
    out_dir       = temp_dir
  )

  sched <- sched_res$files |>
    dplyr::mutate(
      zipfile     = basename(zipfile),
      repairs     = dplyr::coalesce(.data$repairs, list(character(0))),
      inner_files = dplyr::coalesce(.data$inner_files, list(character(0)))
    )

  pqs <- sched$parquet
  date_raw <-
    zipfile |>
    basename() |>
    stringr::str_extract("\\d{8}") |>
    as.Date(format = "%m%d%Y") |>
    format("%Y%m%d")

  db <- DBI::dbConnect(duckdb::duckdb())

  for (dtype in names(arrow_types)) {
    make_long_pq(conn = db,
                 pqs = pqs,
                 dtype = dtype,
                 out_dir = out_dir,
                 date_raw = date_raw,
                 prefix = "ffiec_")
  }
  DBI::dbDisconnect(db)

  # ---- POR (returns tibble) ----
  por <- process_zip_por(
    zipfile      = zipfile,
    inside_files = inside,
    out_dir      = out_dir
  ) |>
    dplyr::mutate(
      zipfile     = basename(zipfile),
      repairs     = dplyr::coalesce(.data$repairs, list(character(0))),
      inner_files = dplyr::coalesce(.data$inner_files, list(character(0))),
      ok = TRUE
    )

  out <- dplyr::bind_rows(sched, por) |>
    dplyr::select(-.data$date_raw) |>
    dplyr::relocate(
      .data$type, .data$kind, .data$date,
      .data$parquet, .data$zipfile, .data$n_parts, .data$repairs, .data$inner_files
    ) |>
    dplyr::arrange(.data$date, .data$type, .data$kind)

  attr(out, "out_dir") <- out_dir
  out
}

#' Process FFIEC call report bulk data
#'
#' High-level convenience wrapper that processes FFIEC Call Report bulk
#' zip files into Parquet format, optionally creating item-level metadata.
#'
#' Input zip files may be supplied explicitly or discovered automatically
#' from a resolved raw data directory. Output Parquet files are written to
#' a resolved data directory.
#'
#' @param zipfiles Optional character vector of FFIEC bulk zip file paths.
#'   If \code{NULL}, zip files are discovered automatically from the resolved
#'   raw data directory.
#' @param raw_data_dir Optional parent directory containing FFIEC bulk zip
#'   files. If provided and \code{schema} is not \code{NULL}, files are
#'   expected under \code{file.path(raw_data_dir, schema)}. If \code{NULL},
#'   the environment variable \code{RAW_DATA_DIR} is used.
#' @param data_dir Optional parent directory for Parquet output. If provided
#'   and \code{schema} is not \code{NULL}, files are written under
#'   \code{file.path(data_dir, schema)}. If \code{NULL}, the environment
#'   variable \code{DATA_DIR} is used.
#' @param schema Schema name used to resolve input and output directories
#'   (default \code{"ffiec"}). If \code{NULL}, directories are resolved
#'   directly without appending a schema subdirectory.
#' @param create_item_pqs Logical; if \code{TRUE}, create or update FFIEC
#'   item metadata Parquet files as part of processing.
#' @param keep_process_data Logical; whether to write the processing log
#'   returned by \code{ffiec_process()} to
#'   \code{"ffiec_process_data.parquet"} in the resolved output directory.
#'   If \code{NULL}, defaults to \code{TRUE} when \code{zipfiles} is
#'   \code{NULL} and \code{FALSE} when \code{zipfiles} is supplied.
#' @param use_multicore Logical; whether to attempt parallel execution when
#'   reading Parquet metadata. If \code{TRUE} and the optional packages
#'   \pkg{future} and \pkg{furrr} are installed, operations are parallelized
#'   using a multisession plan. Defaults to \code{FALSE}.
#'
#' @return A tibble describing written Parquet files.
#' @export
ffiec_process <- function(zipfiles = NULL,
                          raw_data_dir = NULL, data_dir = NULL,
                          schema = "ffiec",
                          create_item_pqs = TRUE,
                          keep_process_data = NULL,
                          use_multicore = FALSE) {

  dirs <- resolve_dirs(
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

  if (is.null(keep_process_data)) {
    keep_process_data <- is.null(zipfiles)
  }

  if (is.null(zipfiles)) {
    zipfiles <- ffiec_list_zips(raw_data_dir = raw_data_dir,
                                schema = schema)$zipfile

    keep <- stringr::str_detect(
      basename(zipfiles),
      stringr::regex("^FFIEC CDR Call Bulk All Schedules \\d{8}\\.zip$", ignore_case = TRUE)
    )

    zipfiles <- zipfiles[keep]

    if (length(zipfiles) == 0L) {
      stop("No FFIEC bulk zip files found in the resolved input directory.", call. = FALSE)
    }
  }

  zipfiles <- normalizePath(zipfiles, mustWork = TRUE)

  if (isTRUE(create_item_pqs)) {
    ffiec_create_item_pqs(data_dir = data_dir, schema = schema,
                          overwrite = TRUE)
  }

  use_parallel <- isTRUE(use_multicore) &&
    requireNamespace("future", quietly = TRUE) &&
    requireNamespace("furrr", quietly = TRUE)

  out <- if (use_parallel) {

    old_plan <- future::plan()
    on.exit(future::plan(old_plan), add = TRUE)

    future::plan(future::multisession)

    furrr::future_map_dfr(
      zipfiles,
      process_ffiec_zip,
      out_dir = out_dir
    )

  } else {

    purrr::map_dfr(
      zipfiles,
      process_ffiec_zip,
      out_dir = out_dir
    )
  }

  if (FALSE) {
    ffiec_create_item_schedules_pq(data_dir = data_dir, schema = schema,
                                   overwrite = TRUE)
  }

  if (isTRUE(keep_process_data)) {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("Package 'arrow' is required to write Parquet files.", call. = FALSE)
    }
    arrow::write_parquet(out, file.path(out_dir, "ffiec_process_data.parquet"))
  }

  attr(out, "out_dir") <- out_dir
  out
}

#' List generated FFIEC Parquet files
#'
#' Lists Parquet files produced by \code{ffiec_process()} and returns their
#' filenames, full paths, and inferred FFIEC schedule identifiers.
#'
#' @param data_dir Optional parent directory for Parquet output. If provided
#'   and \code{schema} is not \code{NULL}, files are written under
#'   \code{file.path(data_dir, schema)}. If \code{NULL}, the environment
#'   variable \code{DATA_DIR} is used.
#' @param schema Character scalar identifying the schema subdirectory
#'   (default \code{"ffiec"}). Set to \code{NULL} to use the resolved
#'   directory directly without appending a schema subdirectory.
#' @param all_files Logical; if \code{TRUE}, return all Parquet files in the
#'   resolved directory. If \code{FALSE} (default), only files with names
#'   ending in \code{_YYYYMMDD.parquet} are returned.
#'
#' @return A tibble with one row per Parquet file and columns:
#' \describe{
#'   \item{base_name}{File name without directory.}
#'   \item{full_name}{Full path to the Parquet file.}
#'   \item{schedule}{Inferred FFIEC schedule identifier extracted from the file name.}
#' }
#'
#' @examples
#' \dontrun{
#' # List FFIEC Parquet files using DATA_DIR/ffiec
#' ffiec_list_pqs()
#'
#' # List files from an explicit directory (no schema subdir)
#' ffiec_list_pqs(data_dir = "/data/parquet/ffiec", schema = NULL)
#' }
#'
#' @export
ffiec_list_pqs <- function(data_dir = NULL,
                           schema = "ffiec",
                           all_files = FALSE) {

  out_dir <- resolve_out_dir(data_dir = data_dir, schema = schema)
  if (is.null(out_dir)) {
    stop("Provide `data_dir` or set DATA_DIR.", call. = FALSE)
  }

  out_dir <- normalizePath(out_dir, mustWork = FALSE)

  pattern <- if (isTRUE(all_files)) {
    "\\.parquet$"
  } else {
    "_\\d{8}\\.parquet$"
  }

  res <- list.files(
    out_dir,
    pattern = pattern,
    full.names = TRUE
  )

  base_name <- basename(res)

  tibble::tibble(
    base_name = base_name,
    full_name = res,
    schedule  = extract_schedule(base_name)
  )
}

#' @keywords internal
#' @noRd
extract_schedule <- function(files, prefix = "") {
  x <- basename(files)

  # lop off the prefix using fixed matching (NOT regex)
  x <- sub(paste0("^", prefix), "", x, fixed = TRUE)

  # now the filename is: schedule_YYYYMMDD.parquet
  sub("_\\d{8}\\.parquet$", "", x)
}
