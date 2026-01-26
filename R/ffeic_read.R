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

#' Repair embedded newlines in FFIEC tab-delimited schedule files
#'
#' FFIEC schedule files occasionally contain TEXT fields with embedded newlines.
#' When a file is read using `readLines()`, those embedded newlines split a single
#' logical record across multiple lines, which later causes `readr::read_tsv()`
#' to report inconsistent column counts.
#'
#' This helper stitches continuation lines back onto the previous record.
#' A line is treated as the start of a new record only if it looks like a row
#' start (begins with digits + tab) AND it contains at least a minimum number of
#' tab-delimited fields. The minimum is derived from the observed distribution
#' of field counts among row-start candidates, with a user-controlled tolerance.
#'
#' @param lines Character vector of lines (typically from `readLines()`).
#' @param expected_cols Integer scalar: expected number of columns in the file.
#' @param tolerance Integer scalar: how much to relax the minimum field-count
#'   threshold (default 10). Larger values are more aggressive about treating
#'   digit-leading lines as continuations rather than new rows.
#'
#' @return Character vector of repaired lines.
#' @keywords internal
#' @noRd
repair_ffiec_tsv_lines <- function(lines, expected_cols, tolerance = 10L) {
  stopifnot(
    is.numeric(expected_cols), length(expected_cols) == 1L, expected_cols >= 1,
    is.numeric(tolerance), length(tolerance) == 1L, tolerance >= 0
  )

  n_fields <- function(x) 1L + stringr::str_count(x, "\t")

  # keep non-empty lines
  lines <- lines[nzchar(lines)]
  if (length(lines) == 0L) return(character(0))

  # Minimum fields required to accept "digits+tab" as a true new record.
  # This is the key guard against "1540\t\tDetailed explanation..." being misread as a new row.
  min_fields_new_row <- max(1L, as.integer(expected_cols) - as.integer(tolerance))

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

  # ---- helper: collapse overflow fields into last column (replace extra tabs) ----
  collapse_overflow_into_last <- function(lines, expected_cols, sep = " ") {
    stopifnot(is.numeric(expected_cols), length(expected_cols) == 1L, expected_cols >= 1L)

    n_fields <- function(x) 1L + stringr::str_count(x, "\t")
    is_data  <- grepl("^\\d+\\t", lines)
    too_many <- is_data & (n_fields(lines) > expected_cols)

    if (!any(too_many)) return(lines)

    fix_one <- function(ln) {
      parts <- strsplit(ln, "\t", fixed = TRUE)[[1]]
      if (length(parts) <= expected_cols) return(ln)
      head <- if (expected_cols > 1L) parts[seq_len(expected_cols - 1L)] else character(0)
      tail <- parts[expected_cols:length(parts)]
      last <- paste(tail, collapse = sep)     # <-- critical: NOT "\t"
      paste(c(head, last), collapse = "\t")
    }

    lines[too_many] <- vapply(lines[too_many], fix_one, character(1))
    lines
  }

  # ---- header ----
  raw_cols <- get_header_from_zip_tsv(zipfile, inner_file)
  cols <- clean_cols(raw_cols)
  expected_cols <- length(cols)

  if (!is.numeric(expected_cols) || expected_cols < 1L) {
    stop("Header parse produced 0 columns for inner_file: ", inner_file, call. = FALSE)
  }

  # ---- colspec ----
  ffiec_col_overrides <- default_ffiec_col_overrides()
  colspec <- make_colspec(
    cols,
    schema,
    xbrl_to_readr,
    ffiec_col_overrides = ffiec_col_overrides
  )

  # ---- read raw lines ----
  con <- unz(zipfile, inner_file)
  on.exit(try(close(con), silent = TRUE), add = TRUE)

  lines <- readLines(con, warn = FALSE)

  if (length(lines) == 0L) {
    return(tibble::as_tibble(stats::setNames(replicate(expected_cols, logical(0), simplify = FALSE), cols)))
  }

  # Keep non-empty lines (your repair function assumes this)
  lines <- lines[nzchar(lines)]
  if (length(lines) == 0L) {
    return(tibble::as_tibble(stats::setNames(replicate(expected_cols, logical(0), simplify = FALSE), cols)))
  }

  # Data rows begin after the first two lines (header + "type" row)
  header_lines <- if (length(lines) >= 2L) lines[1:2] else lines
  data_lines   <- if (length(lines) > 2L)  lines[-(1:2)] else character(0)

  # ---- detect + repair embedded newlines (only if needed) ----
  needs_repair <- FALSE
  if (length(data_lines) > 0L) {
    field_counts <- 1L + stringr::str_count(data_lines, "\t")
    needs_repair <- any(field_counts < expected_cols, na.rm = TRUE)
  }

  data_lines2 <- data_lines
  if (needs_repair && length(data_lines) > 0L) {
    data_lines2 <- repair_ffiec_tsv_lines(
      data_lines,
      expected_cols = expected_cols,
      tolerance = 10L
    )
    if (isTRUE(getOption("ffiec.pq.debug", FALSE))) {
      message("Applied line repair for: ", inner_file)
    }
  }

  # ---- trim extra trailing tabs that create > expected_cols fields ----
  if (length(data_lines2) > 0L) {
    data_lines2 <- trim_extra_trailing_tabs(data_lines2, expected_cols = expected_cols)
  }

  # ---- NEW: collapse overflow tabs into the last column (only when last col is character) ----
  if (length(data_lines2) > 0L) {
    last_nm <- cols[[length(cols)]]

    # try to detect whether last column is character based on the colspec produced by make_colspec()
    last_is_char <- FALSE
    if (!is.null(colspec$cols) && !is.null(colspec$cols[[last_nm]])) {
      last_is_char <- inherits(colspec$cols[[last_nm]], "collector_character")
    }

    if (last_is_char) {
      before_any <- any((1L + stringr::str_count(data_lines2, "\t")) > expected_cols)
      if (before_any) {
        data_lines2 <- collapse_overflow_into_last(data_lines2, expected_cols = expected_cols, sep = " ")
        if (isTRUE(getOption("ffiec.pq.debug", FALSE))) {
          message("Collapsed overflow tabs into last column for: ", inner_file)
        }
      }
    }
  }

  # ---- write a clean temp TSV and parse with readr ----
  tmp <- tempfile(fileext = ".tsv")
  on.exit(try(unlink(tmp), silent = TRUE), add = TRUE)

  writeLines(c(header_lines, data_lines2), tmp, useBytes = TRUE)

  df <- readr::read_tsv(
    tmp,
    skip = 2,
    quote="",
    col_names = cols,
    na = c("", "NA", "CONF"),
    col_types = colspec,
    progress = FALSE,
    show_col_types = FALSE
  )

  date_cols <- names(ffiec_col_overrides)[ffiec_col_overrides == "D"]
  date_cols <- intersect(date_cols, names(df))

  if (length(date_cols)) {
    df <- df |>
      dplyr::mutate(
        dplyr::across(
          dplyr::all_of(date_cols),
          parse_ffiec_yyyymmdd
        )
      )
  }

  # ---- debug: report parsing problems ----
  if (isTRUE(getOption("ffiec.pq.debug", FALSE))) {
    probs <- readr::problems(df)
    if (nrow(probs) > 0) {
      message("Parsing issues in ", inner_file)
      print(probs)
      r0 <- probs$row[[1]]
      lo <- max(1L, r0 - 3L)
      hi <- min(length(data_lines2), r0 + 3L)
      message("Nearby repaired data lines (post-repair, post-trim):")
      print(data_lines2[lo:hi])
    }
  }

  df
}

parse_ffiec_yyyymmdd <- function(x) {
  readr::parse_date(
    x,
    format = "%Y%m%d",
    na = c("", "NA", "0", "00000000")
  )
}

collapse_overflow_tabs <- function(lines, expected_cols) {
  stopifnot(
    is.numeric(expected_cols), length(expected_cols) == 1L, expected_cols >= 1
  )

  # Count fields = tabs + 1
  n_fields <- function(x) 1L + stringr::str_count(x, "\t")

  # Only touch *data* lines (start with digits + tab) that have too many fields
  is_data <- grepl("^\\d+\\t", lines)
  too_many <- is_data & (n_fields(lines) > expected_cols)

  if (!any(too_many)) return(lines)

  fix_one <- function(ln) {
    parts <- strsplit(ln, "\t", fixed = TRUE)[[1]]
    if (length(parts) <= expected_cols) return(ln)

    head <- parts[seq_len(expected_cols - 1L)]
    tail <- parts[expected_cols:length(parts)]

    # Re-join overflow into last field.
    # (use "\t" to preserve embedded structure; use " " if you prefer flattening)
    last <- paste(tail, collapse = "\t")

    paste(c(head, last), collapse = "\t")
  }

  lines[too_many] <- vapply(lines[too_many], fix_one, character(1))
  lines
}

#' Trim extra trailing tab delimiters beyond the expected column count
#'
#' Some FFIEC schedule rows end with more tab delimiters than implied by the
#' header, creating "extra empty columns" at the end of the record. This helper
#' removes only *trailing* tab delimiters so that each line has at most
#' `expected_cols` fields, without altering interior tabs or non-empty fields.
#'
#' @param lines Character vector of TSV record lines (data rows, not header).
#' @param expected_cols Integer scalar, number of columns implied by the header.
#'
#' @return Character vector of lines, with excessive trailing tabs removed.
#' @keywords internal
#' @noRd
trim_extra_trailing_tabs <- function(lines, expected_cols) {
  stopifnot(
    is.numeric(expected_cols), length(expected_cols) == 1L, expected_cols >= 1
  )

  if (length(lines) == 0L) return(lines)

  count_tabs <- function(x) stringr::str_count(x, "\t")

  # Keep empty lines unchanged (you usually drop them earlier anyway)
  out <- lines

  # For each line, while it has too many fields AND ends with a tab, drop one tab.
  # Fields = tabs + 1, so "too many fields" means tabs >= expected_cols.
  too_many <- (count_tabs(out) + 1L) > expected_cols
  if (!any(too_many)) return(out)

  idx <- which(too_many)
  for (i in idx) {
    # only fix by trimming trailing tabs (do not touch interior structure)
    while ((count_tabs(out[i]) + 1L) > expected_cols && grepl("\t$", out[i])) {
      out[i] <- sub("\t$", "", out[i])
    }
  }

  out
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
        D = readr::col_character(),  # <- parse later
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
#' @keywords internal
#' @noRd
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
