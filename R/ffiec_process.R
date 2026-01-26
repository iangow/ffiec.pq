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

  df <- combine_call_parts(dfs, key = "IDRSSD") |>
    dplyr::mutate(date = files_tbl$date[[1]])

  df <- fix_pure_percent_cols(df, schema)

  ffiec_set_repairs(df, repairs)
}

#' @keywords internal
#' @noRd
ffiec_get_repairs <- function(x) {
  r <- attr(x, "ffiec_repairs", exact = TRUE)
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
  attr(x, "ffiec_repairs") <- unique(as.character(repairs))
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
#'
#' @return A tibble with columns \code{schedule}, \code{date_raw}, \code{date},
#'   \code{parquet}, and \code{n_parts}.
#' @keywords internal
#' @noRd
process_zip_schedules <- function(zipfile, inside_files, schema, xbrl_to_readr, out_dir) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  targets <- inside_files |>
    dplyr::filter(!is.na(.data$schedule)) |>
    dplyr::mutate(schedule = tolower(.data$schedule)) |>
    dplyr::arrange(.data$schedule, .data$date_raw, .data$part, .data$file)

  groups <- dplyr::group_split(targets, .data$schedule, .data$date_raw, .keep = TRUE)

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

    repairs <- attr(df, "ffiec_repairs", exact = TRUE)
    if (is.null(repairs)) repairs <- character(0)

    out_path <- file.path(out_dir, sprintf("sched_%s_%s.parquet", schedule, date_raw))
    arrow::write_parquet(df, out_path)

    tibble::tibble(
      type        = "schedule",
      kind        = schedule,                 # <-- merge schedule/kind
      date_raw    = date_raw,
      date        = date,
      parquet     = basename(out_path),
      zipfile     = basename(zipfile),
      n_parts     = n_parts,
      repairs     = list(repairs),
      inner_files = list(basename(g$file))    # <-- list-column (1+ files)
    )
  })

  list(
    out_dir = out_dir,
    files   = results
  )
}

# ---- DOR/POR helpers ----

#' Extract a single internal file from a zip to an output directory
#'
#' Extracts exactly one internal file from \code{zipfile} into \code{out_dir},
#' renaming it to \code{out_name}. This is useful for debugging or inspecting
#' the raw TSV content.
#'
#' @param zipfile Path to the zip file.
#' @param inner_file Internal file path inside the zip.
#' @param out_dir Output directory.
#' @param out_name Output filename to use.
#'
#' @return The full output path to the extracted file.
#' @keywords internal
#' @noRd
extract_one_inner_file <- function(zipfile, inner_file, out_dir, out_name) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  tmp <- tempfile("ffiec_unzip_")
  dir.create(tmp)

  extracted <- unzip(zipfile, files = inner_file, exdir = tmp)
  stopifnot(length(extracted) == 1L)

  out_path <- file.path(out_dir, out_name)

  ok <- file.rename(extracted, out_path)
  if (!ok) {
    ok2 <- file.copy(extracted, out_path, overwrite = TRUE)
    if (!ok2) stop("Failed to move/copy extracted file to output path: ", out_path)
  }

  unlink(tmp, recursive = TRUE)
  out_path
}

#' Split a tab-delimited line into fields
#'
#' @param line A single character string.
#'
#' @return Character vector of fields.
#' @keywords internal
#' @noRd
split_tsv_line <- function(line) strsplit(line, "\t", fixed = TRUE)[[1]]

#' Process non-schedule DOR/POR files from a bulk zip into Parquet
#'
#' @param zipfile Path to the bulk zip file.
#' @param inside_files A tibble as returned by \code{get_cr_files()}.
#' @param out_dir Output directory for Parquet files.
#'
#' @return A tibble with \code{kind}, \code{date}, \code{date_raw}, \code{inner_file}, \code{parquet}.
#' @keywords internal
#' @noRd
process_zip_dor <- function(zipfile, inside_files, out_dir) {
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
    kind <- dor_kind(inner_file)

    df <- read_dor_from_zip(zipfile, inner_file) |>
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
      sprintf("dor_%s_%s.parquet", kind, date_raw)
    )
    arrow::write_parquet(df, out_path)

    tibble::tibble(
      type        = "dor",
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
resolve_raw_dir <- function(raw_dir, schema) {
  if (!is.null(raw_dir)) return(raw_dir)

  parent <- Sys.getenv("RAW_DATA_DIR", unset = "")
  if (nzchar(parent)) return(file.path(parent, schema))

  NULL
}

#' @keywords internal
#' @noRd
resolve_out_dir <- function(out_dir, schema) {
  if (!is.null(out_dir)) return(out_dir)

  parent <- Sys.getenv("DATA_DIR", unset = "")
  if (nzchar(parent)) return(file.path(parent, schema))

  NULL
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
process_ffiec_zip <- function(zipfile, out_dir = NULL, schema = "ffiec") {
  out_dir <- resolve_out_dir(out_dir, schema)
  if (is.null(out_dir)) stop("Provide `out_dir` or set DATA_DIR.", call. = FALSE)

  out_dir <- normalizePath(out_dir, mustWork = FALSE)

  schema_tbl   <- get_ffiec_schema()
  xbrl_to_readr <- default_xbrl_to_readr()

  inside <- get_cr_files(zipfile)

  # ---- schedules (returns list(out_dir=..., files=...)) ----
  sched_res <- process_zip_schedules(
    zipfile       = zipfile,
    inside_files  = inside,
    schema        = schema_tbl,
    xbrl_to_readr = xbrl_to_readr,
    out_dir       = out_dir
  )

  sched <- sched_res$files |>
    dplyr::mutate(
      zipfile     = basename(zipfile),
      repairs     = dplyr::coalesce(.data$repairs, list(character(0))),
      inner_files = dplyr::coalesce(.data$inner_files, list(character(0)))
    )

  # ---- DOR/POR (returns tibble) ----
  dor <- process_zip_dor(
    zipfile      = zipfile,
    inside_files = inside,
    out_dir      = out_dir
  ) |>
    dplyr::mutate(
      zipfile     = basename(zipfile),
      repairs     = dplyr::coalesce(.data$repairs, list(character(0))),
      inner_files = dplyr::coalesce(.data$inner_files, list(character(0)))
    )

  out <- dplyr::bind_rows(sched, dor) |>
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
#' High-level convenience wrapper around
#' \code{process_ffiec_zip()} and \code{process_ffiec_zips()}.
#'
#' @param zipfile Path to a single bulk FFIEC zip file.
#' @param zipfiles Optional vector of bulk zip file paths.
#' @param raw_dir Directory containing bulk zip files.
#' @param out_dir Output directory for Parquet files.
#' @param schema Subdirectory name under \code{RAW_DATA_DIR} and \code{DATA_DIR}.
#'   Defaults to \code{"ffiec"}.
#'
#' @return A tibble describing written Parquet files.
#' @export
ffiec_process <- function(zipfiles = NULL, raw_dir = NULL, out_dir = NULL, schema = "ffiec") {
  out_dir <- resolve_out_dir(out_dir, schema)
  if (is.null(out_dir)) stop("Provide `out_dir` or set DATA_DIR.", call. = FALSE)
  out_dir <- normalizePath(out_dir, mustWork = FALSE)

  if (is.null(zipfiles)) {
    raw_dir <- resolve_raw_dir(raw_dir, schema)
    if (is.null(raw_dir)) {
      stop("Provide `zipfiles`, or provide `raw_dir`, or set RAW_DATA_DIR.", call. = FALSE)
    }

    zipfiles <- ffiec_list_zips(raw_dir = raw_dir)$zipfile
  }

  zipfiles <- normalizePath(zipfiles, mustWork = TRUE)

  out <- purrr::map_dfr(
    zipfiles,
    process_ffiec_zip,
    out_dir = out_dir,
    schema = schema
  )

  attr(out, "out_dir") <- out_dir
  out
}
