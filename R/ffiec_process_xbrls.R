#' Process FFIEC XBRL bulk zip files
#'
#' Reads FFIEC bulk XBRL zip files (each containing many \code{*.xbrl.xml} inner
#' files), extracts reported XBRL facts, and writes a Parquet file per zipfile
#' into the resolved Parquet output directory.
#'
#' Directory resolution follows the same rules as \code{ffiec_process()}:
#' \code{raw_data_dir} / \code{RAW_DATA_DIR} for inputs and
#' \code{data_dir} / \code{DATA_DIR} for outputs, optionally with a schema
#' subdirectory when \code{schema} is non-\code{NULL}.
#'
#' @param zipfiles Optional character vector of FFIEC bulk XBRL zip file paths.
#'   If \code{NULL}, zip files are discovered automatically in the resolved input
#'   directory (using \code{ffiec_list_zips(type = "xbrl")}).
#' @param raw_data_dir Optional parent directory containing FFIEC bulk zip files.
#' @param data_dir Optional parent directory for Parquet output.
#' @param schema Schema subdirectory name (default \code{"ffiec"}). Set to
#'   \code{NULL} to use \code{raw_data_dir} / \code{data_dir} directly.
#' @param use_multicore Logical; if \code{TRUE} and packages \code{future} and
#'   \code{furrr} are installed, process zip files in parallel using
#'   \code{future::multisession()}.
#' @param ns_prefix XML namespace prefix to extract (default \code{"cc"}).
#' @param prefix Optional filename prefix for output Parquet file names.
#'
#' @return A tibble with one row per processed zipfile, including the output
#'   Parquet basename. The attribute \code{"out_dir"} contains the resolved
#'   output directory.
#'
#' @export
ffiec_process_xbrls <- function(zipfiles = NULL,
                                raw_data_dir = NULL,
                                data_dir = NULL,
                                schema = "ffiec",
                                use_multicore = FALSE,
                                ns_prefix = "cc",
                                prefix = "") {

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

  if (is.null(zipfiles)) {
    zipfiles <- ffiec_list_zips(
      raw_data_dir = raw_data_dir,
      schema = schema,
      type = "xbrl"
    )$zipfile

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
      out_dir = out_dir,
      ns_prefix = ns_prefix,
      prefix = prefix
    )

  } else {

    purrr::map_dfr(
      zipfiles,
      process_xbrl_zip,
      out_dir = out_dir,
      ns_prefix = ns_prefix,
      prefix = prefix
    )
  }

  attr(out, "out_dir") <- out_dir
  out
}


#' @keywords internal
#' @noRd
process_xbrl_zip <- function(zipfile, out_dir, ns_prefix = "cc", prefix = "") {

  if (!requireNamespace("xml2", quietly = TRUE)) {
    stop("Package 'xml2' is required to process XBRL files.", call. = FALSE)
  }

  x2 <- list(
    read_xml     = utils::getFromNamespace("read_xml", "xml2"),
    xml_find_all = utils::getFromNamespace("xml_find_all", "xml2"),
    xml_name     = utils::getFromNamespace("xml_name", "xml2"),
    xml_attr     = utils::getFromNamespace("xml_attr", "xml2"),
    xml_text     = utils::getFromNamespace("xml_text", "xml2"),
    xml_attrs    = utils::getFromNamespace("xml_attrs", "xml2")
  )

  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  if (!dir.exists(out_dir)) stop("Could not create output directory: ", out_dir, call. = FALSE)

  mmddyyyy <- stringr::str_extract(basename(zipfile), "\\d{8}")
  if (is.na(mmddyyyy)) {
    stop("Could not parse MMDDYYYY date from zip filename: ", basename(zipfile), call. = FALSE)
  }

  date     <- as.Date(mmddyyyy, format = "%m%d%Y")
  date_raw <- format(date, "%Y%m%d")

  inner_files <- utils::unzip(zipfile, list = TRUE) |>
    dplyr::filter(grepl("\\.xbrl\\.xml$", .data$Name)) |>
    dplyr::pull(.data$Name)

  res <- purrr::map(
    inner_files,
    ~ process_xbrl_inner_file(zipfile, .x, ns_prefix = ns_prefix, x2 = x2)
  )

  xbrl_df <- dplyr::bind_rows(res)

  out_path <- file.path(out_dir, sprintf("%sxbrl_%s.parquet", prefix, date_raw))
  arrow::write_parquet(xbrl_df, out_path)

  tibble::tibble(
    zipfile  = basename(zipfile),
    date_raw = date_raw,
    date     = date,
    parquet  = basename(out_path)
  )
}

#' @keywords internal
#' @noRd
#' @keywords internal
#' @noRd
process_xbrl_inner_file <- function(zipfile, inner_file, ns_prefix = "cc", x2) {

  stopifnot(is.list(x2))
  stopifnot(all(c(
    "read_xml", "xml_find_all", "xml_name", "xml_attr", "xml_text", "xml_attrs"
  ) %in% names(x2)))

  # unz() returns a connection-like object that xml2 can read
  con <- unz(zipfile, inner_file)
  on.exit(try(close(con), silent = TRUE), add = TRUE)

  doc <- x2$read_xml(con)

  # NOTE: this assumes the document defines the prefix (e.g., xmlns:cc="...")
  # If you ever hit "Undefined namespace prefix", you'll need xml_ns() mapping.
  nodes <- x2$xml_find_all(doc, paste0(".//", ns_prefix, ":*"))

  local_name <- function(x) {
    nm <- x2$xml_name(x)
    sub("^[^:]+:", "", nm)
  }

  df <- tibble::tibble(
    item       = purrr::map_chr(nodes, local_name),
    contextRef = x2$xml_attr(nodes, "contextRef"),
    unitRef    = x2$xml_attr(nodes, "unitRef"),
    decimals   = x2$xml_attr(nodes, "decimals"),
    value      = x2$xml_text(nodes),
    attrs      = purrr::map(nodes, x2$xml_attrs)  # named chr vector
  )

  df |>
    tidyr::separate_wider_regex(
      "contextRef",
      c(
        schedule = "^[^_]+", "_",
        IDRSSD   = "[0-9]+", "_",
        date     = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
      )
    ) |>
    dplyr::mutate(
      n_attrs = lengths(.data$attrs),
      IDRSSD  = as.integer(.data$IDRSSD),
      date    = as.Date(.data$date)
    ) |>
    dplyr::select(-.data$attrs) |>
    dplyr::select(.data$IDRSSD, .data$date, .data$schedule,
                  .data$item, dplyr::everything())
}
