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

#' @keywords internal
#' @noRd
ffiec_get_ok <- function(df) isTRUE(attr(df, "ok"))

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

  # ---- FAST PATH ----
  con_fast <- unz(zipfile, inner_file)
  res_fast <- try(
    {
      on.exit(try(close(con_fast), silent = TRUE), add = TRUE)
      read_tsv_quiet_check_con(con_fast, cols, colspec, skip = 2L)
    },
    silent = TRUE
  )

  if (!inherits(res_fast, "try-error")) {

    df_fast <- res_fast$df
    attr(df_fast, "warnings") <- res_fast$warnings
    attr(df_fast, "problems") <- res_fast$problems
    attr(df_fast, "ok")       <- isTRUE(res_fast$ok)
    attr(df_fast, "repairs")  <- character(0)

    if (attr(df_fast, "ok")) {
      fin <- ffiec_finalize_if_clean(
        df = df_fast,
        ok = TRUE,
        warnings = attr(df_fast, "warnings"),
        problems = attr(df_fast, "problems"),
        ffiec_col_overrides = ffiec_col_overrides,
        zipfile = zipfile,
        inner_file = inner_file,
        repairs_applied = attr(df_fast, "repairs"),
        debug = debug
      )

      return(ffiec_attach_diagnostics(fin$df, fin))
    }
  } else if (debug) {
    message("Fast-path error in ", inner_file, "; falling back to slow path.")
    message(conditionMessage(attr(res_fast, "condition")))
  }

  # ---- SLOW PATH ----
  con <- unz(zipfile, inner_file)
  on.exit(try(close(con), silent = TRUE), add = TRUE)

  lines <- readLines(con, warn = FALSE)
  txt <- paste(lines, collapse = "\n")
  txt2 <- gsub("(?<!\\t)\\n", " ", txt, perl = TRUE)
  rep_newline <- !identical(txt2, txt)

  df <- read_tsv_with_tab_repair(
    txt2,
    cols = cols,
    colspec = colspec,
    skip = 2L,
    expected_cols = expected_cols
  )

  repairs_applied <- attr(df, "repairs")
  if (rep_newline) repairs_applied <- union(repairs_applied, "newline-gsub")

  fin <- ffiec_finalize_if_clean(
    df = df,
    ok = isTRUE(attr(df, "ok")),
    warnings = attr(df, "warnings"),
    problems = attr(df, "problems"),
    ffiec_col_overrides = ffiec_col_overrides,
    zipfile = zipfile,
    inner_file = inner_file,
    repairs_applied = repairs_applied,
    debug = debug
  )

  ffiec_attach_diagnostics(fin$df, fin)
}

ffiec_attach_diagnostics <- function(df, fin) {
  attr(df, "repairs")  <- fin$repairs
  attr(df, "warnings") <- fin$warnings
  attr(df, "problems") <- fin$problems
  df
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
read_tsv_quiet_check_impl <- function(input, cols, colspec, skip = 2L) {
  warn <- character(0)

  df <- withCallingHandlers(
    readr::read_tsv(
      input,
      col_names = cols,
      col_types = colspec,
      skip = skip,
      quote = "",
      na = c("", "CONF"),
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
    ok = length(warn) == 0L && nrow(probs) == 0L
  )
}

#' @keywords internal
#' @noRd
read_tsv_quiet_check <- function(txt, cols, colspec, skip = 2L) {
  read_tsv_quiet_check_impl(I(txt), cols, colspec, skip)
}

#' @keywords internal
#' @noRd
read_tsv_quiet_check_con <- function(con, cols, colspec, skip = 2L) {
  read_tsv_quiet_check_impl(con, cols, colspec, skip)
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
        na = c("", "CONF"),
        progress = FALSE,
        show_col_types = FALSE
      ),
      warning = function(w) {
        warn <<- c(warn, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    )

    probs <- readr::problems(df2)
    ok <- length(warn) == 0L && nrow(probs) == 0L

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
  na_tokens <- c("", "0", "00000000")

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
        i = readr::col_integer(),
        c = readr::col_character(),
        D = readr::col_character(),  # <- parse later
        stop("Unknown override code for ", nm, ": '", code, "'", call. = FALSE)
      )
    } else if (nm %in% names(schema_map)) {
      switch(
        schema_map[[nm]],
        d = readr::col_double(),
        i = readr::col_integer(),
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
    id_is_data <- !is.na(readr::parse_integer(id_val, na = ""))
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
#' @param ok Logical. Strict read status.
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
#' @seealso [apply_ffiec_date_overrides()] for the date override logic.
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
