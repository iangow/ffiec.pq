#' List FFIEC bulk zip files
#'
#' Scans a directory for FFIEC “Call Bulk All Schedules” zip files and returns
#' a tibble with the zipfile paths and report dates parsed from the filenames.
#'
#' This function is typically used to discover bulk zip files prior to calling
#' [ffiec_process()].
#'
#' @param raw_data_dir Optional parent directory containing FFIEC bulk zip
#'   files. If provided and \code{schema} is not \code{NULL}, files are
#'   expected under \code{file.path(raw_data_dir, schema)}. If \code{NULL},
#'   the environment variable \code{RAW_DATA_DIR} is used.
#' @param schema Schema name used to resolve the input directory
#'   (default \code{"ffiec"}). Set to \code{NULL} to use the resolved
#'   directory directly without appending a schema subdirectory.
#'
#' @details
#' Filenames are expected to contain an 8-digit MMDDYYYY date token
#' (e.g., \code{"FFIEC CDR Call Bulk All Schedules 12312022.zip"}), which
#' is parsed and returned as a \code{Date}. An error is raised if any
#' matching file does not conform to this convention.
#'
#' @return A tibble with columns:
#' \describe{
#'   \item{zipfile}{Full path to the bulk zip file.}
#'   \item{date}{Report date as a \code{Date}.}
#' }
#'
#' @examples
#' \dontrun{
#' # List bulk zip files using RAW_DATA_DIR/ffiec
#' ffiec_list_zips()
#'
#' # List bulk zip files from an explicit directory (no schema subdir)
#' ffiec_list_zips(raw_data_dir = "/data/raw/ffiec", schema = NULL)
#' }
#'
#' @export
ffiec_list_zips <- function(raw_data_dir = NULL,
                            schema = "ffiec") {

  in_dir <- resolve_in_dir(raw_data_dir = raw_data_dir, schema = schema)

  if (is.null(in_dir) || !nzchar(in_dir)) {
    stop("Provide `raw_data_dir` or set `RAW_DATA_DIR`.", call. = FALSE)
  }

  in_dir <- normalizePath(in_dir, mustWork = FALSE)

  files <- list.files(
    in_dir,
    pattern = "^FFIEC CDR Call Bulk All Schedules \\d{8}\\.zip$",
    full.names = TRUE
  )

  if (length(files) == 0L) {
    return(tibble::tibble(
      zipfile = character(0),
      date = as.Date(character(0))
    ))
  }

  # Extract 8-digit token; keep only those that look like MMDDYYYY
  mmddyyyy <- stringr::str_extract(basename(files), "\\d{8}")
  keep_tok <- !is.na(mmddyyyy)

  if (!any(keep_tok)) {
    # No matching bulk zips found; return empty tibble rather than error
    return(tibble::tibble(
      zipfile = character(0),
      date = as.Date(character(0))
    ))
  }

  files2 <- files[keep_tok]
  tok2   <- mmddyyyy[keep_tok]

  date <- as.Date(tok2, format = "%m%d%Y")
  keep_date <- !is.na(date)

  # Drop files with unparseable date tokens (e.g., 13322025)
  files3 <- files2[keep_date]
  date3  <- date[keep_date]

  if (length(files3) == 0L) {
    return(tibble::tibble(
      zipfile = character(0),
      date = as.Date(character(0))
    ))
  }

  tibble::tibble(
    zipfile = normalizePath(files3, mustWork = FALSE),
    date = date3
  ) |>
    dplyr::arrange(.data$date, .data$zipfile)
}

#' Get a manifest of files inside an FFIEC call report bulk zip
#'
#' Lists the contents of a bulk zip file and parses schedule identifiers, report
#' date, and multi-part file indicators from the internal filenames.
#'
#' @param zip_file Path to a bulk FFIEC zip file.
#'
#' @return A tibble with columns \code{file}, \code{schedule}, \code{date},
#'   \code{date_raw}, \code{part}, and \code{n_parts}.
#' @keywords internal
#' @noRd
get_cr_files <- function(zip_file) {
  files <- unzip(zip_file, list = TRUE)$Name

  tibble::tibble(file = files) |>
    dplyr::filter(.data$file != "Readme.txt") |>
    dplyr::mutate(
      schedule = stringr::str_extract(basename(.data$file), "(?<=Schedule )[^ ]+"),
      date_raw_mmddyyyy = stringr::str_extract(basename(.data$file), "\\d{8}"),
      date = lubridate::mdy(.data$date_raw_mmddyyyy),
      date_raw = format(.data$date, "%Y%m%d"),
      part    = stringr::str_extract(basename(.data$file), "(?<=\\()\\d+(?= of )") |> as.integer(),
      n_parts = stringr::str_extract(basename(.data$file), "(?<=of )\\d+(?=\\))") |> as.integer()
    ) |>
    dplyr::select(-.data$date_raw_mmddyyyy)
}

#' Determine POR kind from an internal filename
#'
#' @param inner_file Internal file path within the zip.
#'
#' @return A scalar character string giving the kind.
#' @keywords internal
#' @noRd
por_kind <- function(inner_file) {
  bn <- basename(inner_file)
  if (grepl("Call Bulk POR", bn, fixed = TRUE)) return("por")
  "por"
}

#' Scan FFIEC Parquet files into DuckDB
#'
#' Create a lazy \code{tbl} backed by DuckDB by scanning one or more FFIEC
#' Parquet files. Files may be selected either by FFIEC schedule name
#' (using a glob pattern) or by an explicit Parquet filename. No data is
#' read eagerly; evaluation occurs only when the result is collected.
#'
#' This function is intended for use with Parquet files produced by
#' [ffiec_process()]. It validates that the requested files exist on
#' disk before constructing the DuckDB query.
#'
#' @param conn A valid DuckDB connection.
#' @param schedule Optional character scalar giving the FFIEC schedule
#'   name (e.g. \code{"rc"}, \code{"rci"}, \code{"rcb"}). All matching Parquet files
#'   of the form \code{\{prefix\}\{schedule\}_YYYYMMDD.parquet} will be scanned.
#' @param pq_file Optional character scalar giving a specific Parquet
#'   filename or glob pattern.
#' @param data_dir Optional parent directory for Parquet output. If provided
#'   and \code{schema} is not \code{NULL}, files are written under
#'   \code{file.path(data_dir, schema)}. If \code{NULL}, the environment
#'   variable \code{DATA_DIR} is used.
#' @param schema Schema name used to resolve default directories
#'   (default \code{"ffiec"}). Set to \code{NULL} to treat \code{data_dir}
#'   as the final Parquet directory.
#' @param prefix Optional filename prefix used when the Parquet files
#'   were created (default \code{""}).
#' @param union_by_name Logical; whether to union Parquet files by column
#'   name when scanning multiple files (passed to DuckDB's
#'   \code{read_parquet()}). Default is \code{TRUE}.
#'
#' @details
#' Exactly one of \code{schedule} or \code{pq_file} must be supplied.
#'
#' \strong{Directory resolution}
#'
#' Parquet files are located using the following rules:
#'
#' \itemize{
#'   \item If \code{schema} is non-\code{NULL} (the default), Parquet files are
#'   expected to live in a subdirectory named \code{schema}. In this case,
#'   \code{data_dir} is interpreted as the parent directory, and files are read
#'   from \code{file.path(data_dir, schema)}. If \code{data_dir} is \code{NULL},
#'   the environment variable \code{DATA_DIR} is used as the parent directory.
#'
#'   \item If \code{schema = NULL}, \code{data_dir} is interpreted as the final
#'   directory containing Parquet files directly (no schema subdirectory is
#'   appended). If \code{data_dir} is \code{NULL}, \code{DATA_DIR} is used as the
#'   Parquet directory.
#'
#'   \item If \code{data_dir} is \code{NULL} and \code{DATA_DIR} is unset, and
#'   \code{pq_file} is supplied, \code{pq_file} may be a fully qualified path
#'   to an existing Parquet file.
#' }
#'
#' When \code{schedule} is used, files are located using a glob pattern:
#' \code{\{prefix\}\{schedule\}_*.parquet}. When \code{pq_file} is used, the value
#' is treated as a filename or glob pattern relative to the resolved Parquet
#' directory (unless it is a full path as described above).
#'
#' A fast filesystem check is performed using \code{Sys.glob()}. If no files
#' match, the function errors before issuing any DuckDB query.
#'
#' @return A lazy \code{tbl} backed by DuckDB.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#'
#' # Scan all RC schedule files using DATA_DIR/ffiec
#' rc <- ffiec_scan_pqs(con, schedule = "rc")
#'
#' # Scan from a specific parent directory
#' rc <- ffiec_scan_pqs(con, schedule = "rc", data_dir = "/data/parquet")
#'
#' # Treat data_dir as the final directory (no schema subdir)
#' rc <- ffiec_scan_pqs(con, schedule = "rc", data_dir = "/data/ffiec", schema = NULL)
#'
#' # Scan a single Parquet file by name using DATA_DIR/ffiec
#' por <- ffiec_scan_pqs(con, pq_file = "por_20231231.parquet")
#'
#' # Scan a Parquet file via full path
#' por <- ffiec_scan_pqs(con, pq_file = "/tmp/por_20231231.parquet")
#'
#' # Work lazily
#' rc |> dplyr::count(date)
#' }
#'
#' @export
ffiec_scan_pqs <- function(conn,
                           schedule = NULL,
                           pq_file = NULL,
                           data_dir = NULL,
                           schema = "ffiec",
                           prefix = "",
                           union_by_name = TRUE) {

  stopifnot(DBI::dbIsValid(conn))

  # exactly one of schedule / pq_file
  n_specified <- sum(!is.null(schedule), !is.null(pq_file))
  if (n_specified != 1L) {
    stop("Provide exactly one of `schedule` or `pq_file`.", call. = FALSE)
  }

  # ---- Resolve parquet directory / file strategy ----
  # Rules:
  # - If schema is NULL: data_dir is the full parquet directory (no schema appended)
  # - If schema is non-NULL: data_dir is parent; parquet directory is data_dir/schema
  #   with fallback to DATA_DIR/schema when data_dir is NULL
  # - If data_dir is NULL AND DATA_DIR is unset AND pq_file is provided:
  #   allow pq_file to be a full path to an existing parquet file.

  data_dir_env <- Sys.getenv("DATA_DIR", unset = "")

  resolve_parquet_dir <- function(data_dir, schema) {
    if (is.null(schema)) {
      # data_dir is the final parquet directory
      if (!is.null(data_dir) && nzchar(data_dir)) return(data_dir)
      if (nzchar(data_dir_env)) return(data_dir_env)  # treat DATA_DIR as the parquet dir
      return(NULL)
    } else {
      # data_dir is parent of schema directory; fallback to DATA_DIR
      parent <- if (!is.null(data_dir) && nzchar(data_dir)) data_dir else if (nzchar(data_dir_env)) data_dir_env else ""
      if (!nzchar(parent)) return(NULL)
      file.path(parent, schema)
    }
  }

  pq_path <- resolve_parquet_dir(data_dir = data_dir, schema = schema)

  # If schedule mode: we must have a directory to glob
  if (!is.null(schedule) && is.null(pq_path)) {
    stop("Provide `data_dir` or set DATA_DIR.", call. = FALSE)
  }

  # If pq_file mode and we don't have a dir, allow pq_file as full path
  if (is.null(schedule) && is.null(pq_path)) {
    # pq_file is required here by earlier check
    if (file.exists(pq_file)) {
      # treat pq_file as full path
      glob <- normalizePath(pq_file, mustWork = TRUE)
      sql <- sprintf(
        "SELECT * FROM read_parquet(%s)",
        DBI::dbQuoteString(conn, glob)
      )
      return(dplyr::tbl(conn, dbplyr::sql(sql)))
    }

    stop(
      "Provide `data_dir` / `DATA_DIR`, or supply `pq_file` as a full path to an existing Parquet file.",
      call. = FALSE
    )
  }

  # We have a directory-based path
  stopifnot(nzchar(pq_path))
  pq_path <- normalizePath(pq_path, mustWork = FALSE)

  if (!is.null(schedule)) {
    glob <- file.path(pq_path, sprintf("%s%s_*.parquet", prefix, schedule))
    matches <- Sys.glob(glob)

    if (length(matches) == 0L) {
      stop(
        sprintf(
          "No parquet files found for schedule '%s' using pattern:\n  %s",
          schedule, glob
        ),
        call. = FALSE
      )
    }

    sql <- sprintf(
      "SELECT * FROM read_parquet(%s, union_by_name=%s)",
      DBI::dbQuoteString(conn, glob),
      if (isTRUE(union_by_name)) "true" else "false"
    )

  } else {
    # pq_file could be a base name or a relative path inside pq_path
    glob <- file.path(pq_path, pq_file)
    matches <- Sys.glob(glob)

    if (length(matches) == 0L) {
      stop(sprintf("Parquet file not found:\n  %s", glob), call. = FALSE)
    }

    sql <- sprintf(
      "SELECT * FROM read_parquet(%s)",
      DBI::dbQuoteString(conn, glob)
    )
  }

  dplyr::tbl(conn, dbplyr::sql(sql))
}
