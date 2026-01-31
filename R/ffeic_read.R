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
#' @param row_start_re A regular expression used to identify lines that
#'   plausibly begin a new TSV record. Lines matching this pattern and having
#'   at least `expected_cols - tolerance` tab-separated fields are treated as
#'   the start of a new row; other lines are appended to the previous row to
#'   repair embedded newlines within fields. The default (`"^\\\d+\\\t"`)
#'   assumes the first column is a numeric identifier followed by a tab.
#'
#' @return Character vector of repaired lines.
#' @keywords internal
#' @noRd
repair_ffiec_tsv_lines <- function(lines,
                                   expected_cols,
                                   tolerance = 0L,
                                   row_start_re = "^\\d+\\t") {
  stopifnot(
    is.numeric(expected_cols), length(expected_cols) == 1L, expected_cols >= 1,
    is.numeric(tolerance), length(tolerance) == 1L, tolerance >= 0
  )

  repairs <- character(0)
  add_repair <- function(x) repairs <<- unique(c(repairs, x))

  expected_cols <- as.integer(expected_cols)
  tolerance     <- as.integer(tolerance)

  # ---- helpers ----
  n_fields_raw <- function(x) 1L + stringr::str_count(x, "\t")
  norm_line    <- function(x) sub("\r$", "", x)
  norm_cont    <- function(x) trimws(norm_line(x))  # don't collapse whitespace; don't touch tabs
  is_data_row  <- function(x) grepl(row_start_re, x)

  detect_trailing_tab_mode <- function(x, max_check = 2000L, frac = 0.50) {
    x <- x[nzchar(x)]
    x <- x[is_data_row(x)]
    if (!length(x)) return(FALSE)
    x <- utils::head(x, max_check)
    mean(grepl("\t$", x)) >= frac
  }

  lines <- norm_line(lines)
  lines <- lines[nzchar(lines)]
  if (!length(lines)) {
    return(list(lines = character(0), repairs = character(0)))
  }

  trailing_tab_mode <- detect_trailing_tab_mode(lines)
  target_cols <- if (trailing_tab_mode) expected_cols + 1L else expected_cols

  if (isTRUE(getOption("ffiec.pq.debug", FALSE))) {
    message(
      "repair_ffiec_tsv_lines: expected_cols=", expected_cols,
      " trailing_tab_mode=", trailing_tab_mode,
      " target_cols=", target_cols
    )
  }

  min_fields_new_row <- max(1L, target_cols - tolerance)

  # Append continuation text *before* any trailing tabs, preserving them.
  append_cont <- function(cur, ln, sep = " ") {
    cont <- norm_cont(ln)
    if (!nzchar(cont)) return(cur)

    tabs <- sub("^(.*?)(\t*)$", "\\2", cur)
    base <- sub("(\t*)$", "", cur)

    if (grepl("^\t", ln)) {
      paste0(base, ln, tabs)
    } else {
      paste0(base, sep, cont, tabs)
    }
  }

  would_overshoot <- function(cur, ln) {
    (n_fields_raw(cur) + n_fields_raw(ln) - 1L) > target_cols
  }

  out <- character(0)
  cur <- lines[[1]]

  flush_cur <- function() out <<- c(out, cur)

  for (i in 2:length(lines)) {
    ln <- lines[[i]]

    looks_like_row_start <- is_data_row(ln)
    ln_fields  <- n_fields_raw(ln)
    cur_fields <- n_fields_raw(cur)

    is_strong_start <- looks_like_row_start && (ln_fields >= min_fields_new_row)

    if (cur_fields < target_cols) {
      if (looks_like_row_start && would_overshoot(cur, ln)) {
        flush_cur()
        cur <- ln
      } else if (is_strong_start) {
        flush_cur()
        cur <- ln
      } else {
        # we truly merged physical lines
        add_repair("embedded-newline")
        cur <- append_cont(cur, ln)
      }
    } else {
      if (looks_like_row_start) {
        flush_cur()
        cur <- ln
      } else {
        add_repair("embedded-newline")
        cur <- append_cont(cur, ln)
      }
    }
  }

  out <- c(out, cur)

  # ---- Post-pass: normalize field count on data rows to exactly target_cols ----

  # Trim ONLY trailing tabs (extra empty fields) down to target_cols.
  trim_trailing_tabs_to_target <- function(ln) {
    nf <- n_fields_raw(ln)
    if (nf <= target_cols) return(ln)

    # how many extra fields?
    extra <- nf - target_cols

    # Only safe if the line ends with at least `extra` trailing tabs.
    # (each trailing \t adds an empty final field)
    m <- regexpr("\t*$", ln)
    trailing_tabs <- attr(m, "match.length")
    if (is.na(trailing_tabs)) trailing_tabs <- 0L

    if (trailing_tabs >= extra) {
      add_repair("extra-trailing-tabs")
      # remove exactly `extra` tabs from the end
      sub(paste0("\t{", extra, "}$"), "", ln)
    } else {
      # Too many fields not explainable by trailing empties => could be embedded tabs.
      add_repair("embedded-tabs-suspect")
      ln
    }
  }

  pad_to_target <- function(ln) {
    if (!is_data_row(ln)) return(ln)

    # First: if too wide, try safe trim of trailing empties
    ln <- trim_trailing_tabs_to_target(ln)

    nf <- n_fields_raw(ln)

    # If still short, pad with tabs
    if (nf < target_cols) {
      ln <- paste0(ln, strrep("\t", target_cols - nf))
      return(ln)
    }

    # If in trailing-tab mode, ensure final \t exists
    if (trailing_tab_mode && !grepl("\t$", ln) && nf == target_cols) {
      return(paste0(ln, "\t"))
    }

    ln
  }

  out <- vapply(out, pad_to_target, character(1))

  list(lines = out, repairs = repairs)
}

pad_missing_trailing_tabs <- function(lines, expected_cols, row_start_re = "^\\d+\\t") {
  expected_cols <- as.integer(expected_cols)

  n_fields <- function(x) 1L + stringr::str_count(x, "\t")

  is_data <- grepl(row_start_re, lines)
  out <- lines

  idx <- which(is_data & nzchar(out) & (n_fields(out) < expected_cols))
  if (!length(idx)) return(out)

  for (i in idx) {
    missing <- expected_cols - n_fields(out[[i]])
    out[[i]] <- paste0(out[[i]], strrep("\t", missing))
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

  debug <- isTRUE(getOption("ffiec.pq.debug", FALSE))

  raw_cols <- get_header_from_zip_tsv(zipfile, inner_file)
  cols <- clean_cols(raw_cols)
  expected_cols <- length(cols)

  ffiec_col_overrides <- default_ffiec_col_overrides()
  colspec <- make_colspec(
    cols, schema, xbrl_to_readr,
    ffiec_col_overrides = ffiec_col_overrides
  )

  con <- unz(zipfile, inner_file)
  on.exit(close(con), add = TRUE)

  lines <- readLines(con, warn = FALSE)

  # count lines that do NOT end with \t
  bad_ends <- sum(!grepl("\t$", lines))

  if (bad_ends > 0L && debug) {
    message(
      "WARNING: ", bad_ends,
      " lines do not end with \\t; newline repair may be unsafe"
    )
  }

  txt <- paste(lines, collapse = "\n")

  txt2 <- gsub("(?<!\\t)\\n", " ", txt, perl = TRUE)

  # NOTE: no newline “repair” here
  df <- read_tsv_with_tab_repair(
    txt2,
    cols = cols,
    colspec = colspec,
    expected_cols = expected_cols
  )

  fin <- ffiec_finalize_if_clean(
    df = df,
    ok = isTRUE(attr(df, "ok")),
    warnings = attr(df, "warnings"),
    problems = attr(df, "problems"),
    ffiec_col_overrides = ffiec_col_overrides,
    zipfile = zipfile,
    inner_file = inner_file,
    repairs_applied = attr(df, "repairs"),
    debug = debug
  )

  df_out <- fin$df
  attr(df_out, "repairs")  <- fin$repairs
  attr(df_out, "warnings") <- fin$warnings
  attr(df_out, "problems") <- fin$problems
  df_out
}

#' @keywords internal
#' @noRd
fix_extra_tabs <- function(lines, expected_cols) {
  keep_tabs <- expected_cols - 1L

  vapply(lines, function(ln) {
    tabs <- gregexpr("\t", ln, fixed = TRUE)[[1]]
    if (tabs[1] == -1L) return(ln)            # no tabs at all
    if (length(tabs) <= keep_tabs) return(ln) # already OK

    # split into: prefix ends BEFORE the (keep_tabs+1)th tab
    cut_pos <- tabs[keep_tabs]  # position of the last tab we keep as delimiter
    prefix <- substr(ln, 1L, cut_pos)          # includes that tab
    rest   <- substr(ln, cut_pos + 1L, nchar(ln))

    # convert any remaining tabs in rest to spaces
    paste0(prefix, gsub("\t", " ", rest, fixed = TRUE))
  }, character(1))
}

#' @keywords internal
#' @noRd
read_tsv_quiet_check <- function(txt, cols, colspec, skip = 2L) {
  warn <- character(0)

  df <- withCallingHandlers(
    readr::read_tsv(
      I(txt),
      col_names = cols,
      col_types = colspec,
      skip = skip,
      quote = "",
      na = c("", "NA", "CONF"),
      progress = FALSE,
      show_col_types = FALSE
    ),
    warning = function(w) {
      warn <<- c(warn, conditionMessage(w))
      invokeRestart("muffleWarning")
    }
  )

  probs <- readr::problems(df)

  list(
    df = df,
    warnings = unique(warn),
    problems = probs,
    ok = (length(warn) == 0L) && (nrow(probs) == 0L)
  )
}

#' @keywords internal
#' @noRd
read_tsv_with_tab_repair <- function(txt,
                                     cols,
                                     colspec,
                                     skip = 2L,
                                     expected_cols = length(cols)) {

  res1 <- read_tsv_quiet_check(txt, cols, colspec, skip)

  if (res1$ok) {
    df <- res1$df
    attr(df, "problems") <- res1$problems
    attr(df, "warnings") <- res1$warnings
    attr(df, "ok")       <- TRUE
    attr(df, "repairs")  <- character(0)
    return(df)
  } else {
    # ---- second attempt: minimal tab repair ----
    lines <- strsplit(txt, "\n", fixed = TRUE)[[1]]
    if (length(lines) && identical(lines[[length(lines)]], "")) {
      lines <- lines[-length(lines)]
    }
    header <- lines[seq_len(skip)]
    data   <- lines[-seq_len(skip)]

    data_fixed <- fix_extra_tabs(data, expected_cols)

    txt_fixed <- paste(c(header, data_fixed), collapse = "\n")

    warn <- character(0)

    df2 <- withCallingHandlers(
      readr::read_tsv(
        I(txt_fixed),
        col_names = cols,
        col_types = colspec,
        skip = skip,
        quote = "",
        na = c("", "NA", "CONF"),
        progress = FALSE,
        show_col_types = FALSE
      ),
      warning = function(w) {
        warn <<- c(warn, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    )

    probs <- readr::problems(df2)
    ok <- (length(warn) == 0L) && (nrow(probs) == 0L)

    attr(df2, "problems") <- probs
    attr(df2, "warnings") <- unique(warn)
    attr(df2, "ok")       <- ok
    attr(df2, "repairs")  <- "tab-repair"
    df2
  }
}

#' Apply FFIEC date overrides, recording invalid-date repairs
#'
#' @return list(df=..., repairs=character())
#' @keywords internal
#' @noRd
apply_ffiec_date_overrides <- function(df, date_cols, zipfile, inner_file, debug = FALSE) {
  if (!length(date_cols)) return(list(df = df, repairs = character(0)))

  date_cols <- intersect(date_cols, names(df))
  if (!length(date_cols)) return(list(df = df, repairs = character(0)))

  # We’ll collect problems per column
  problems_by_col <- list()

  # Do the coercion without warnings
  df2 <- suppressWarnings({
    df |>
      dplyr::mutate(
        dplyr::across(
          dplyr::all_of(date_cols),
          ~ {
            parsed <- parse_ffiec_yyyymmdd_silent(.x)
            probs  <- attr(parsed, "ffiec_date_problems")
            if (is.data.frame(probs) && nrow(probs) > 0) {
              problems_by_col[[dplyr::cur_column()]] <<- probs
            }
            parsed
          }
        )
      )
  })

  # If any problems occurred, tag + optionally print details
  if (length(problems_by_col) > 0) {
    if (isTRUE(debug)) {
      n_bad <- sum(vapply(problems_by_col, nrow, integer(1)))
      cols_bad <- paste(names(problems_by_col), collapse = ", ")
      message(
        "Coerced invalid date(s) in ",
        basename(zipfile), " :: ", inner_file,
        " | column(s): ", cols_bad,
        " | failures: ", n_bad
      )

      # show up to a few examples per column
      for (nm in names(problems_by_col)) {
        p <- problems_by_col[[nm]]
        p <- dplyr::as_tibble(p)
        p_show <- utils::head(p, 5)
        message("  - ", nm, ":")
        print(p_show)
      }
    }

    return(list(df = df2, repairs = "coerced-invalid-dates"))
  }



  list(df = df2, repairs = character(0))
}

#' Treat zero-valued identifier fields as missing
#'
#' Many FFIEC identifier fields (e.g., FDIC certificate number, OCC charter
#' number, OTS docket number, routing numbers) use the literal value `"0"` to
#' indicate a missing or inapplicable identifier. This helper normalizes those
#' fields by converting `"0"` and empty strings to `NA`, while leaving all other
#' values unchanged.
#'
#' This function is intentionally conservative: it does **not** coerce types
#' and is designed to be used inside `dplyr::mutate(across(...))` pipelines.
#'
#' @param x A character vector containing identifier values.
#'
#' @return A character vector with `"0"` and `""` replaced by `NA`.
#'
#' @keywords internal
#' @noRd
parse_id_zero_na <- function(x) {
  x <- trimws(x)
  x[x %in% c("", "0")] <- NA_character_
  x
}


#' Parse FFIEC YYYYMMDD dates silently
#'
#' Returns a Date vector. Invalid tokens become NA.
#' Attaches an attribute "ffiec_date_problems" (a tibble) with parse failures.
#' Never warns.
#'
#' @keywords internal
#' @noRd
parse_ffiec_yyyymmdd_silent <- function(x) {
  x_chr <- as.character(x)

  # treat these as missing for date parsing
  na_tokens <- c("", "NA", "0", "00000000")

  probs <- NULL
  out <- withCallingHandlers(
    readr::parse_date(
      x_chr,
      format = "%Y%m%d",
      na = na_tokens
    ),
    warning = function(w) {
      # swallow parse warnings; we'll get details from problems()
      invokeRestart("muffleWarning")
    }
  )

  probs <- readr::problems(out)
  attr(out, "ffiec_date_problems") <- probs
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

#' Read a POR TSV from within a zip file
#'
#' Reads a non-schedule file (POR) from a bulk zip without extracting it.
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
read_por_from_zip <- function(zipfile, inner_file) {

  safe_close <- function(con) {
    try(close(con), silent = TRUE)
    invisible(NULL)
  }

  con1 <- unz(zipfile, inner_file)
  on.exit(safe_close(con1), add = TRUE)

  lines <- readLines(con1, n = 2)
  if (length(lines) < 1) stop("Empty file inside zip: ", inner_file)

  header <- clean_por_cols(split_tsv_line(lines[1]))

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

#' Clean header column names for POR TSVs
#'
#' @param cols Character vector of raw column names.
#'
#' @return Character vector of cleaned column names.
#' @keywords internal
#' @noRd
clean_por_cols <- function(cols) {
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

  # nothing to do
  if (all(is.na(x))) return(x)

  has_percent <- grepl("%$", x)
  has_number  <- grepl("[0-9]", x)

  # numeric-looking values that do NOT end with %
  bad <- has_number & !has_percent & !is.na(x)

  if (any(bad)) {
    stop(
      "pct_to_prop(): found numeric values not ending in '%': ",
      paste(unique(x[bad]), collapse = ", "),
      call. = FALSE
    )
  }

  # safe to parse
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

#' Prepare a robust TSV input source from a Call Report file inside a ZIP
#'
#' @description
#' `ffiec_prepare_tsv_input()` constructs an input object suitable for
#' `readr::read_tsv()` from an FFIEC Call Report schedule stored in a ZIP file.
#' It is designed to address a common corruption pattern in these files:
#' **illegal embedded newlines** that split a logical record across multiple
#' physical lines.
#'
#' The function reads the file once as raw lines, applies an embedded-newline
#' repair on the **data lines only**, and then decides whether to adopt the
#' repaired text based on a conservative structural check (field-count validity).
#'
#' - If the repair does **not** improve structural validity, the function returns
#'   a connection-based input (`unz()`), leaving the original bytes untouched.
#' - If the repair **does** improve structural validity, the function returns a
#'   text-based input containing the original header lines plus the repaired data
#'   lines.
#'
#' @param zipfile Character scalar. Path to a ZIP file.
#' @param inner_file Character scalar. The path/name of the TSV file within
#'   `zipfile` (as understood by [unz()]).
#' @param skip Integer scalar. Number of initial lines that `readr::read_tsv()`
#'   should skip. This is typically `2` for FFIEC schedule extracts (two-line
#'   header). The returned text input retains these header lines so that the same
#'   `skip` can be used for both connection and text inputs.
#' @param expected_cols Integer scalar. Expected number of columns (fields) in
#'   the schedule (typically `length(cols)` from the parsed header).
#' @param tolerance Integer scalar. Maximum number of line-stitching iterations
#'   (or similar safeguard) forwarded to [repair_ffiec_tsv_lines()]. Used to
#'   prevent runaway repairs on severely corrupted input.
#' @param row_start_re Character scalar. Regular expression identifying “data”
#'   lines (default matches lines beginning with a numeric ID followed by a tab).
#'   This is used to restrict structural checks to true record lines.
#'
#' @return A list with two elements:
#' \describe{
#'   \item{input}{A list describing the prepared input, with either:
#'     \describe{
#'       \item{kind = "connection"}{Includes `con_open`, a function that returns a
#'         fresh [unz()] connection each time it is called.}
#'       \item{kind = "text"}{Includes `txt`, a single character string containing
#'         the full TSV text (header + repaired data) separated by `\\n`.}
#'     }
#'   }
#'   \item{repairs}{A character vector of repairs applied. Currently either empty
#'     (`character(0)`) or `"embedded-newline"` when repaired text is adopted.}
#' }
#'
#' @details
#' **Structural validity gate.** The embedded-newline repair is adopted only if it
#' reduces the number of “bad” data lines, where a line is “bad” if it appears to
#' be a data record (`row_start_re`) but does not have exactly `expected_cols`
#' fields (i.e., `str_count("\\t") + 1 != expected_cols`). This prevents
#' downstream parse/type errors caused by applying delimiter-sensitive operations
#' (e.g., trimming tabs) before record boundaries are stabilized.
#'
#' **Line ending normalization.** Stray carriage returns (`\\r`) are removed from
#' line ends before analysis and repair.
#'
#' @seealso [repair_ffiec_tsv_lines()] for the embedded-newline stitching logic;
#'   [ffiec_read_tsv_strict()] for parsing and diagnostics using the prepared input.
#'
#' @keywords internal
ffiec_prepare_tsv_input <- function(zipfile,
                                    inner_file,
                                    skip = 2L,
                                    expected_cols,
                                    tolerance = 0L,
                                    row_start_re = "^\\d+\\t") {

  con_open <- function() unz(zipfile, inner_file)

  con <- unz(zipfile, inner_file)
  on.exit(try(close(con), silent = TRUE), add = TRUE)
  lines <- readLines(con, warn = FALSE)

  lines <- sub("\r$", "", lines)

  header_lines <- if (skip > 0L) utils::head(lines, skip) else character(0)
  data_lines   <- if (length(lines) > skip) lines[(skip + 1L):length(lines)] else character(0)

  if (!length(data_lines)) {
    return(list(input = list(kind = "connection", con_open = con_open),
                repairs = character(0)))
  }

  fixed <- repair_ffiec_tsv_lines(
    lines = data_lines,
    expected_cols = expected_cols,
    tolerance = tolerance,
    row_start_re = row_start_re
  )

  # Back-compat
  if (is.character(fixed)) {
    fixed <- list(lines = fixed, repairs = character(0))
  }

  stopifnot(is.list(fixed), !is.null(fixed$lines), is.character(fixed$lines))
  fixed_data <- fixed$lines
  repairs_from_lines <- if (is.null(fixed$repairs)) character(0) else fixed$repairs

  # Decide if anything meaningful happened
  repaired <- length(repairs_from_lines) > 0L || !identical(fixed_data, data_lines)

  if (!repaired) {
    return(list(
      input = list(kind = "connection", con_open = con_open),
      repairs = character(0)
    ))
  }

  txt <- paste(c(header_lines, fixed_data), collapse = "\n")
  txt <- paste0(txt, "\n")

  raw <- charToRaw(txt)
  con_open_fixed <- function() rawConnection(raw, open = "rb")

  list(
    input = list(kind = "connection", con_open = con_open_fixed),
    repairs = repairs_from_lines
  )
}

#' Read a prepared FFIEC TSV input with strict diagnostics
#'
#' @description
#' `ffiec_read_tsv_strict()` is a thin wrapper around [readr::read_tsv()] that
#' provides **strict** success criteria and consistent behavior across input
#' types produced by [ffiec_prepare_tsv_input()].
#'
#' It supports two input modes:
#' \describe{
#'   \item{Connection input}{A `con_open()` factory that returns a fresh connection
#'   (typically from [unz()]).}
#'   \item{Text input}{A single character string containing the full TSV text.}
#' }
#'
#' The function captures and muffles warnings, collects parsing problems via
#' [readr::problems()], and reports whether the read is “clean” (no warnings and
#' no recorded problems).
#'
#' @param prepped_input A list describing the prepared input. Must include
#'   `kind`, and either:
#'   \describe{
#'     \item{kind = "connection"}{A `con_open` function returning a fresh
#'       connection each call.}
#'     \item{kind = "text"}{A `txt` character string containing TSV content.}
#'   }
#' @param cols Character vector of column names passed to `col_names`.
#' @param colspec A readr column specification passed to `col_types`.
#' @param skip Integer scalar. Number of initial lines to skip (passed to
#'   [readr::read_tsv()] for both input modes to ensure identical behavior).
#' @param quote Character scalar. Quoting character(s) passed to
#'   [readr::read_tsv()]. For FFIEC schedule extracts this is typically `""`
#'   (no quoting).
#' @param na Character vector. Values to treat as missing (passed to `na`).
#' @param progress Logical. Whether readr should show progress.
#' @param show_col_types Logical. Whether readr should print inferred column types.
#'
#' @return A list with elements:
#' \describe{
#'   \item{ok}{Logical. `TRUE` iff no warnings were captured and
#'     `readr::problems(df)` has zero rows.}
#'   \item{df}{A tibble on success; `NULL` on hard error.}
#'   \item{warnings}{Character vector of captured warning messages (possibly empty).}
#'   \item{problems}{A tibble of parsing problems from [readr::problems()]. Empty
#'     tibble if none or if a hard error occurred before a data frame existed.}
#'   \item{error}{Present only on hard error (`try-error` object).}
#' }
#'
#' @details
#' - Warnings are captured and muffled so that callers can decide whether they
#'   should trigger a fallback/repair strategy.
#' - Connections opened via `prepped_input$con_open()` are always closed on exit.
#' - `skip` is applied in both connection and text modes to keep the parse
#'   contract identical regardless of where the input came from.
#'
#' @seealso [ffiec_prepare_tsv_input()] to construct `prepped_input`;
#'   [readr::read_tsv()] for the underlying parser.
#'
#' @keywords internal
ffiec_read_tsv_strict <- function(prepped_input,
                                  cols,
                                  colspec,
                                  skip = 2L,
                                  quote = "",
                                  na = c("", "NA", "CONF"),
                                  progress = FALSE,
                                  show_col_types = FALSE) {
  warn <- character(0)

  capture_warnings <- function(expr) {
    withCallingHandlers(
      expr,
      warning = function(w) {
        warn <<- c(warn, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    )
  }

  out <- try({
    df <- if (prepped_input$kind == "connection") {
      con <- prepped_input$con_open()
      on.exit(try(close(con), silent = TRUE), add = TRUE)

      capture_warnings(
        readr::read_tsv(
          con,
          skip = skip,
          quote = quote,
          col_names = cols,
          na = na,
          col_types = colspec,
          progress = progress,
          show_col_types = show_col_types
        )
      )
    } else {
      tmp <- tempfile(fileext = ".tsv")
      on.exit(try(unlink(tmp), silent = TRUE), add = TRUE)

      # prepped_input$txt should be a character vector of lines
      writeLines(prepped_input$txt, tmp, useBytes = TRUE)

      capture_warnings(
        readr::read_tsv(
          tmp,
          skip = skip,
          quote = quote,
          col_names = cols,
          na = na,
          col_types = colspec,
          progress = progress,
          show_col_types = show_col_types
        )
      )
    }

    probs <- readr::problems(df)
    ok <- (length(warn) == 0L) && (nrow(probs) == 0L)

    list(ok = ok, df = df, warnings = warn, problems = probs)
  }, silent = TRUE)

  if (inherits(out, "try-error")) {
    list(ok = FALSE, df = NULL, warnings = warn, problems = tibble::tibble(), error = out)
  } else {
    out
  }
}

#' Finalize a parsed schedule when the strict read is clean
#'
#' @description
#' `ffiec_finalize_if_clean()` applies post-read transformations that are only
#' appropriate when a schedule has been read **cleanly** (i.e., no warnings and
#' no recorded parsing problems). When the read is not clean, the function
#' returns immediately without modifying the data and signals that finalization
#' was not performed.
#'
#' Currently, the only finalization step is to apply FFIEC date overrides via
#' [apply_ffiec_date_overrides()], which can coerce certain date-like fields
#' according to `ffiec_col_overrides`.
#'
#' @param df A tibble returned by [readr::read_tsv()]. May be `NULL` if the read
#'   failed hard upstream.
#' @param ok Logical. Strict read status, typically from [ffiec_read_tsv_strict()].
#'   Finalization occurs only when `isTRUE(ok)`.
#' @param warnings Character vector of warnings captured during the read. Included
#'   for downstream reporting/debugging.
#' @param problems Tibble of parsing problems from [readr::problems()]. Included
#'   for downstream reporting/debugging.
#' @param ffiec_col_overrides Named character vector describing FFIEC column-type
#'   overrides. Columns with override `"D"` are treated as date columns for the
#'   override pass.
#' @param zipfile Character scalar. Path to the ZIP being processed (passed
#'   through to [apply_ffiec_date_overrides()] for context/debug output).
#' @param inner_file Character scalar. Member file name within the ZIP (passed
#'   through to [apply_ffiec_date_overrides()] for context/debug output).
#' @param repairs_applied Character vector of repairs/events already recorded by
#'   earlier stages (e.g., `"embedded-newline"`, `"fallback-slow-path"`).
#' @param debug Logical. If `TRUE`, downstream override routines may emit
#'   diagnostic messages.
#'
#' @return A list containing:
#' \describe{
#'   \item{ok}{Logical. `TRUE` if finalization was performed; `FALSE` otherwise.}
#'   \item{df}{The finalized data frame when `ok = TRUE`, otherwise the input `df`
#'     unchanged.}
#'   \item{repairs}{Character vector of repairs/events, including any repairs
#'     reported by [apply_ffiec_date_overrides()].}
#'   \item{warnings}{Warnings captured during the read (passed through).}
#'   \item{problems}{Readr parsing problems (passed through).}
#' }
#'
#' @details
#' This function intentionally performs **no repairs** when the strict read is not
#' clean. It is meant to be used as part of a two-phase strategy:
#' \enumerate{
#'   \item Read strictly and exit early when clean.
#'   \item Otherwise, fall back to a repair/canonicalization path and re-read.
#' }
#'
#' @seealso [ffiec_read_tsv_strict()] for strict read diagnostics;
#'   [apply_ffiec_date_overrides()] for the date override logic.
#'
#' @keywords internal
ffiec_finalize_if_clean <- function(df,
                                    ok,
                                    warnings,
                                    problems,
                                    ffiec_col_overrides,
                                    zipfile,
                                    inner_file,
                                    repairs_applied = character(0),
                                    debug = FALSE) {
  # If the read isn't clean, don't finalize
  if (!isTRUE(ok)) {
    return(list(ok = FALSE, df = df, repairs = repairs_applied,
                warnings = warnings, problems = problems))
  }

  # Apply date overrides on the clean path (your prior behavior)
  date_cols <- names(ffiec_col_overrides)[ffiec_col_overrides == "D"]

  res_dates <- apply_ffiec_date_overrides(
    df = df,
    date_cols = date_cols,
    zipfile = zipfile,
    inner_file = inner_file,
    debug = debug
  )

  df2 <- res_dates$df
  repairs2 <- union(repairs_applied, res_dates$repairs)

  list(ok = TRUE, df = df2, repairs = repairs2,
       warnings = warnings, problems = problems)
}

#' Attach FFIEC repair metadata to a data frame
#'
#' @description
#' `attach_repairs()` records the set of repairs or processing events applied
#' during ingestion by attaching them as an attribute on a data frame.
#'
#' The function performs minimal normalization:
#' - coerces the input to character,
#' - drops `NA` and empty values,
#' - removes duplicates.
#'
#' It does **not** interpret or validate the repair labels; it simply stores them.
#'
#' @param df A data frame or tibble to which repair metadata should be attached.
#' @param repairs Character vector of repair or processing labels
#'   (e.g., `"embedded-newline"`, `"fallback-slow-path"`,
#'   `"coerced-invalid-dates"`).
#'
#' @return The input data frame `df`, with a `"ffiec_repairs"` attribute attached.
#'
#' @details
#' Repair metadata is stored as a simple character vector in the
#' `"ffiec_repairs"` attribute. Downstream code may choose to surface this
#' attribute as a list-column or ignore it entirely.
#'
#' @keywords internal
attach_repairs <- function(df, repairs) {
  repairs <- as.character(repairs)
  repairs <- repairs[!is.na(repairs) & nzchar(repairs)]
  attr(df, "ffiec_repairs") <- unique(repairs)
  df
}
