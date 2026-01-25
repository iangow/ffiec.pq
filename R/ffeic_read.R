#' Extract header field names from an internal TSV file inside a zip
#'
#' Opens an internal file within a zip via \code{unz()} and reads the first line,
#' splitting on tab characters.
#'
#' @param zipfile Path to the zip file.
#' @param inner_file Internal file name inside the zip.
#'
#' @return Character vector of header field names.
#' @keywords internal
#' @noRd
get_header_from_zip_tsv <- function(zipfile, inner_file) {
  con <- unz(zipfile, inner_file)
  on.exit(close(con), add = TRUE)

  header <- readLines(con, n = 1)
  strsplit(header, "\t", fixed = TRUE)[[1]]
}

#' Repair FFIEC TSV files with embedded newlines in text fields
#'
#' FFIEC call report TSV files occasionally contain **embedded newline characters
#' inside free-text (TEXT*) fields**, typically used for narrative explanations.
#' These embedded newlines break the one-record-per-line structure expected by
#' standard TSV parsers, causing downstream parsing errors (e.g., mismatched
#' column counts).
#'
#' This function heuristically reconstructs broken records by:
#' \enumerate{
#'   \item Identifying candidate record-start lines using a conservative pattern
#'         (lines beginning with an IDRSSD-like numeric field).
#'   \item Estimating a data-dependent minimum number of tab-delimited fields
#'         required for a line to plausibly represent a new record.
#'   \item Treating any line that fails this test as a continuation of the
#'         previous record, and concatenating it with a space separator.
#' }
#'
#' The minimum field-count threshold is inferred from the distribution of
#' field counts among apparent record-start lines, using a low quantile
#' (5th percentile) minus a user-supplied tolerance. This approach is robust to:
#' \itemize{
#'   \item trailing empty fields being omitted,
#'   \item schedule-specific column counts,
#'   \item and unusually short continuation lines that begin with digits
#'         (e.g., narrative lines starting with dollar amounts).
#' }
#'
#' @param lines Character vector of raw lines read from a TSV file (typically
#'   via \code{readLines()}).
#' @param expected_cols Integer scalar giving the expected number of columns
#'   in the TSV (used as a fallback if no valid row-start lines are detected).
#' @param tolerance Non-negative integer scalar giving the number of fields
#'   subtracted from the inferred minimum row width when determining whether
#'   a line represents a new record. Higher values make the repair more permissive.
#'
#' @return A character vector of repaired TSV lines, suitable for re-parsing
#'   with \code{readr::read_tsv()}.
#'
#' @details
#' This function is intentionally conservative: it only joins lines when there
#' is strong evidence that a newline occurred inside a text field. It does not
#' attempt to validate semantic correctness of repaired records.
#'
#' @keywords internal
#' @noRd
repair_ffiec_tsv_lines <- function(lines, expected_cols, tolerance = 10L) {
  stopifnot(
    is.numeric(expected_cols), length(expected_cols) == 1L, expected_cols >= 2,
    is.numeric(tolerance), length(tolerance) == 1L, tolerance >= 0
  )

  # tabs + 1
  n_fields <- function(x) 1L + stringr::str_count(x, "\t")

  # Keep non-empty lines
  lines <- lines[nzchar(lines)]

  if (length(lines) == 0L) return(character(0))

  is_row_start <- grepl("^\\d+\\t", lines)

  # Field counts among row-start candidates
  row_counts <- n_fields(lines[is_row_start])

  # Derive a robust "typical minimum" for real rows:
  # use a low quantile of row-start counts (so trailing-empty omissions don't hurt),
  # then subtract tolerance, and apply a floor to avoid catching "1540\t\tExplanation..."
  if (length(row_counts) == 0L) {
    # fallback: just use expected_cols - tolerance, but keep it sane
    min_fields_new_row <- max(3L, as.integer(expected_cols) - as.integer(tolerance))
  } else {
    q05 <- as.integer(stats::quantile(row_counts, probs = 0.05, names = FALSE, type = 7))
    min_fields_new_row <- max(3L, q05 - as.integer(tolerance))
  }

  out <- character(0)

  for (ln in lines) {
    if (length(out) == 0L) {
      out <- c(out, ln)
      next
    }

    looks_like_row_start <- grepl("^\\d+\\t", ln)
    fields <- n_fields(ln)

    if (looks_like_row_start && fields >= min_fields_new_row) {
      out <- c(out, ln)
    } else {
      out[length(out)] <- paste0(out[length(out)], " ", ln)
    }
  }

  out
}

#' Read a schedule TSV from within a zip file
#'
#' @param zipfile Path to the bulk FFIEC zip file.
#' @param inner_file Internal file path inside the zip.
#' @param schema Arrow schema mapping.
#' @param xbrl_to_readr XBRL-to-readr type mapping.
#'
#' @return A tibble with columns parsed according to the schema where possible.
#' @keywords internal
#' @noRd
read_call_from_zip <- function(zipfile, inner_file, schema, xbrl_to_readr) {
  raw_cols <- get_header_from_zip_tsv(zipfile, inner_file)
  cols <- clean_cols(raw_cols)

  ffiec_col_overrides <- default_ffiec_col_overrides()
  colspec <- make_colspec(
    cols, schema, xbrl_to_readr,
    ffiec_col_overrides = ffiec_col_overrides
  )

  expected_cols <- length(cols)

  con <- unz(zipfile, inner_file)
  on.exit(try(close(con), silent = TRUE), add = TRUE)

  lines <- readLines(con, warn = FALSE)

  if (length(lines) == 0L) {
    return(tibble::tibble())
  }

  # Keep header + (typically) description row intact; only repair *data* lines
  if (length(lines) <= 2L) {
    lines2 <- lines
  } else {
    head2 <- lines[1:2]
    data_lines <- lines[-c(1L, 2L)]
    data_lines <- repair_ffiec_tsv_lines(
      data_lines,
      expected_cols = expected_cols,
      tolerance = 10L
    )
    lines2 <- c(head2, data_lines)
  }

  tmp <- tempfile(fileext = ".tsv")
  writeLines(lines2, tmp, useBytes = TRUE)

  df <- readr::read_tsv(
    tmp,
    skip = 2,
    col_names = cols,
    na = c("", "NA", "CONF"),
    col_types = colspec,
    progress = FALSE,
    show_col_types = FALSE
  )

  if (isTRUE(getOption("ffiec.pq.debug", FALSE))) {
    probs <- readr::problems(df)
    if (nrow(probs) > 0) {
      message("Parsing issues in ", inner_file)
      message("Repaired TSV written to: ", tmp)
      print(probs)

      # Optional: print a small excerpt around the first bad row
      r0 <- probs$row[[1]]
      lo <- max(1L, r0 - 2L)
      hi <- min(length(lines2), r0 + 2L)
      message("Excerpt around first problem row (in repaired file):")
      print(lines2[lo:hi])
    } else {
      # Optional: clean up temp file if no issues and debugging is on
      # unlink(tmp)
    }
  } else {
    # Optional: if you don't want to litter temp files in normal runs
    # unlink(tmp)
  }

  df
}

#' Create a readr column specification for FFIEC schedule TSVs
#'
#' @param cols Character vector of column names from the TSV header.
#' @param schema Arrow schema mapping with at least columns \code{name} and \code{type}.
#' @param xbrl_to_readr Named character vector mapping XBRL types to readr spec codes.
#'
#' @return A \code{readr} column specification suitable for \code{readr::read_tsv()}.
#' @keywords internal
#' @noRd
make_colspec <- function(cols, schema, xbrl_to_readr, ffiec_col_overrides = character()) {
  cols <- clean_cols(cols)

  schema_map <- schema |>
    dplyr::transmute(
      name,
      spec = xbrl_to_readr[.data$type]
    ) |>
    dplyr::filter(!is.na(.data$spec)) |>
    dplyr::distinct(.data$name, .keep_all = TRUE) |>
    tibble::deframe()

  spec <- lapply(cols, function(nm) {
    if (nm == "IDRSSD") {
      readr::col_integer()
    } else if (nm %in% names(ffiec_col_overrides)) {
      code <- ffiec_col_overrides[[nm]]
      switch(
        code,
        d = readr::col_double(),
        l = readr::col_logical(),
        c = readr::col_character(),
        stop("Unknown override code for ", nm, ": '", code, "'", call. = FALSE)
      )
    } else if (nm %in% names(schema_map)) {
      switch(
        schema_map[[nm]],
        d = readr::col_double(),
        l = readr::col_logical(),
        c = readr::col_character()
      )
    } else {
      readr::col_character()
    }
  })

  names(spec) <- cols
  do.call(readr::cols_only, spec)
}

#' Read a DOR/POR TSV from within a zip file
#'
#' Reads a non-schedule file (DOR/POR) from a bulk zip without extracting it.
#' Detects whether a description row is present and adjusts \code{skip}.
#'
#' If the column \code{last_date_time_submission_updated_on} is present, it is parsed
#' as Eastern time and converted to UTC.
#'
#' @param zipfile Path to the bulk zip file.
#' @param inner_file Internal file path inside the zip.
#'
#' @return A tibble containing the parsed file.
#' @export
read_dor_from_zip <- function(zipfile, inner_file) {

  safe_close <- function(con) {
    try(close(con), silent = TRUE)
    invisible(NULL)
  }

  con1 <- unz(zipfile, inner_file)
  on.exit(safe_close(con1), add = TRUE)

  lines <- readLines(con1, n = 2)
  if (length(lines) < 1) stop("Empty file inside zip: ", inner_file)

  header <- clean_dor_cols(split_tsv_line(lines[1]))

  skip <- 1L
  if (length(lines) >= 2 && "IDRSSD" %in% header) {
    row2 <- split_tsv_line(lines[2])
    id_pos <- match("IDRSSD", header)
    id_val <- if (!is.na(id_pos) && id_pos <= length(row2)) row2[[id_pos]] else NA_character_
    id_is_data <- !is.na(readr::parse_integer(id_val, na = c("", "NA")))
    if (!id_is_data) skip <- 2L
  }

  safe_close(con1)

  spec <- stats::setNames(vector("list", length(header)), header)
  for (nm in header) spec[[nm]] <- readr::col_character()
  if ("IDRSSD" %in% header) spec[["IDRSSD"]] <- readr::col_integer()

  con2 <- unz(zipfile, inner_file)
  on.exit(safe_close(con2), add = TRUE)

  df <- readr::read_tsv(
    con2,
    skip = skip,
    col_names = header,
    col_types = do.call(readr::cols_only, spec),
    progress = FALSE,
    show_col_types = FALSE
  )

  if (getOption("ffiec.pq.debug", FALSE)) {
    probs <- readr::problems(df)
    if (nrow(probs) > 0) {
      message("Parsing issues in ", inner_file)
      print(probs)
    }
  }

  ts_col <- "last_date_time_submission_updated_on"
  if (ts_col %in% names(df) && is.character(df[[ts_col]])) {
    x <- trimws(df[[ts_col]])
    x[x == ""] <- NA_character_
    ts_et <- as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%S", tz = "America/New_York")
    df[[ts_col]] <- lubridate::with_tz(ts_et, tzone = "UTC")
  }

  df
}

#' Clean header column names for DOR/POR TSVs
#'
#' @param cols Character vector of raw column names.
#'
#' @return Character vector of cleaned column names.
#' @keywords internal
#' @noRd
clean_dor_cols <- function(cols) {
  cols <- clean_cols(cols)

  cols <- stringr::str_replace_all(cols, "[^A-Za-z0-9]+", "_")
  cols <- stringr::str_replace_all(cols, "_{2,}", "_")
  cols <- stringr::str_replace_all(cols, "^_+|_+$", "")
  cols <- tolower(cols)

  # preserve common FFIEC identifier casing
  cols[cols == "idrssd"] <- "IDRSSD"

  cols
}

#' Clean raw header column names for schedule TSVs
#'
#' @param cols Character vector of raw column names.
#'
#' @return Character vector of cleaned column names.
#' @keywords internal
#' @noRd
clean_cols <- function(cols) {
  cols <- trimws(cols)
  gsub('^"|"$', "", cols)
}

#' Convert percent-encoded strings to numeric proportions
#'
#' @param x Character vector.
#'
#' @return Numeric vector.
#' @keywords internal
#' @noRd
pct_to_prop <- function(x) {
  x <- trimws(x)
  x[x == ""] <- NA_character_
  readr::parse_number(x) / 100
}

#' Identify pureItemType columns present in a data frame
#'
#' @param df A data frame.
#' @param schema Arrow schema mapping.
#'
#' @return Character vector of column names.
#' @keywords internal
#' @noRd
pure_cols_present <- function(df, schema) {
  pure_names <- schema |>
    dplyr::filter(.data$type == "xbrli:pureItemType") |>
    dplyr::pull(.data$name) |>
    unique()

  intersect(names(df), pure_names)
}

#' Fix percent-encoded pureItemType columns in a schedule data frame
#'
#' @param df A data frame.
#' @param schema Arrow schema mapping.
#'
#' @return The input data frame with affected columns converted.
#' @keywords internal
#' @noRd
fix_pure_percent_cols <- function(df, schema) {
  cols <- pure_cols_present(df, schema)
  if (length(cols) == 0) return(df)

  df |>
    dplyr::mutate(dplyr::across(dplyr::all_of(cols), function(x) {
      if (is.character(x) && any(grepl("%", x, fixed = TRUE), na.rm = TRUE)) {
        pct_to_prop(x)
      } else {
        x
      }
    }))
}
