#' @keywords internal
#' @noRd
extract_schedule_one <- function(path, prefix = "") {
  x <- basename(path)
  if (nzchar(prefix) && startsWith(x, prefix)) x <- substr(x, nchar(prefix) + 1L, nchar(x))
  sub("_\\d{8}\\.parquet$", "", x)
}

#' @keywords internal
#' @noRd
pq_cols_by_arrow_type <- function(path) {
  pq  <- arrow::ParquetFileReader$create(path)
  sch <- pq$GetSchema()

  tibble::tibble(
    name = vapply(sch$fields, \(f) f$name, character(1)),
    arrow_type = vapply(sch$fields, \(f) class(f$type)[[1]], character(1))
  )
}

pq_cols <- function(path, drop = c("IDRSSD", "date")) {
  sch <- arrow::ParquetFileReader$create(path)$GetSchema()
  setdiff(sch$names, drop)
}

#' @keywords internal
#' @noRd
get_schedule <- function(pq, prefix = "") {
  stopifnot(length(pq) == 1L)
  schedule <- extract_schedule_one(pq, prefix = prefix)
  item <- pq_cols(pq)
  if(length(item) >= 1) {
    data.frame(item, schedule)
  }
}

#' @keywords internal
#' @noRd
get_long <- function(conn, pq, dtype = "Float64", prefix = "") {
  stopifnot(DBI::dbIsValid(conn), length(pq) == 1L)

  sched <- extract_schedule_one(pq, prefix = prefix)
  types_tbl <- pq_cols_by_arrow_type(pq)

  cols <- types_tbl |>
    dplyr::filter(.data$arrow_type == dtype) |>
    dplyr::pull(.data$name)

  cols <- setdiff(cols, c("IDRSSD", "date"))
  if (length(cols) == 0L) return(NULL)

  subq <- dplyr::tbl(
    conn,
    dplyr::sql(sprintf(
      "FROM read_parquet(%s)",
      DBI::dbQuoteString(conn, normalizePath(pq, mustWork = TRUE))
    ))
  ) |>
    dplyr::select("IDRSSD", "date", dplyr::all_of(cols))

  sql <- paste0(
    "SELECT IDRSSD, date, item, value\n",
    "FROM (\n",
    "  UNPIVOT (", dbplyr::sql_render(subq), ")\n",
    "  ON COLUMNS(* EXCLUDE (IDRSSD, date))\n",
    "  INTO NAME item\n",
    "       VALUE value\n",
    ") AS u\n",
    "WHERE value IS NOT NULL\n"
  )

  dplyr::tbl(conn, dplyr::sql(sql))
}

#' @keywords internal
#' @noRd
get_longs <- function(conn, pqs, dtype = "Float64", prefix = "") {
  dfs <- lapply(pqs, function(x) get_long(conn, x, dtype = dtype, prefix = prefix))
  dfs <- Filter(Negate(is.null), dfs)
  stopifnot(length(dfs) > 0L)
  Reduce(dplyr::union_all, dfs)
}

#' @keywords internal
#' @noRd
get_schedules <- function(pqs, prefix = "") {
  dfs <- lapply(pqs, function(x) get_schedule(x, prefix = prefix))
  dfs <- Filter(Negate(is.null), dfs)
  stopifnot(length(dfs) > 0L)
  Reduce(dplyr::union_all, dfs)
}

arrow_types <- c(
  float = "Float64",
  int   = "Int32",
  str   = "Utf8",
  date  = "Date32",
  bool  = "Boolean"
)

#' @keywords internal
#' @noRd
make_long_pq <- function(conn, pqs, dtype = "float", out_dir, date_raw,
                          prefix = "", distinct = FALSE, overwrite = TRUE) {
  stopifnot(DBI::dbIsValid(conn))

  out <-
    get_longs(conn, pqs, dtype = arrow_types[[dtype]], prefix = prefix) |>
    dplyr::distinct()

  assert_no_dups(conn, out, keys = c("IDRSSD", "date", "item"))
  out_path <- file.path(out_dir,
                        sprintf("%s%s_%s.parquet", prefix, dtype, date_raw))
  copy_to_parquet(conn, out, path = out_path, overwrite = overwrite)
}

#' @keywords internal
#' @noRd
make_schedule_pq <- function(pqs, out_dir, date_raw,
                             prefix = "", overwrite = TRUE) {

  schedule_tbl <- get_schedules(pqs, prefix = prefix)
  out <- stats::aggregate(schedule ~ item, schedule_tbl, c)
  out_path <- file.path(out_dir, sprintf("%s%s_%s.parquet", prefix, "schedules", date_raw))
  arrow::write_parquet(out, sink = out_path)
}

#' @keywords internal
#' @noRd
assert_no_dups <- function(conn, tbl, keys = c("IDRSSD", "date", "item")) {
  stopifnot(DBI::dbIsValid(conn), is.character(keys), length(keys) > 0L)

  x_sql <- dbplyr::sql_render(tbl)
  key_sql <- paste(keys, collapse = ", ")

  sql <- paste0(
    "SELECT COUNT(*) AS n_dupe_groups\n",
    "FROM (\n",
    "  SELECT ", key_sql, ", COUNT(*) AS n\n",
    "  FROM (", x_sql, ") AS x\n",
    "  GROUP BY ", key_sql, "\n",
    "  HAVING COUNT(*) > 1\n",
    ") AS d"
  )

  n_dupes <- DBI::dbGetQuery(conn, sql)$n_dupe_groups[[1]]

  if (n_dupes > 0) {
    stop(sprintf("Found %d duplicate key groups on {%s}", n_dupes, key_sql), call. = FALSE)
  }

  invisible(TRUE)
}

#' @keywords internal
#' @noRd
copy_to_parquet <- function(conn, tbl, path, overwrite = TRUE) {
  stopifnot(DBI::dbIsValid(conn), length(path) == 1L)

  path <- normalizePath(path, mustWork = FALSE)
  if (file.exists(path) && !isTRUE(overwrite)) {
    stop("File exists: ", path, call. = FALSE)
  }

  sql <- paste0(
    "COPY (", dbplyr::sql_render(tbl), ")\n",
    "TO ", DBI::dbQuoteString(conn, path), " (FORMAT PARQUET)"
  )

  DBI::dbExecute(conn, sql)
  invisible(path)
}
