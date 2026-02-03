extract_schedule_one <- function(path, prefix = "") {
  x <- basename(path)
  if (nzchar(prefix) && startsWith(x, prefix)) x <- substr(x, nchar(prefix) + 1L, nchar(x))
  sub("_\\d{8}\\.parquet$", "", x)
}

pq_cols_by_arrow_type <- function(path) {
  pq  <- arrow::ParquetFileReader$create(path)
  sch <- pq$GetSchema()

  tibble::tibble(
    name = vapply(sch$fields, \(f) f$name, character(1)),
    arrow_type = vapply(sch$fields, \(f) class(f$type)[[1]], character(1))
  )
}

pqs <-
  ffiec.pq::ffiec_list_pqs() |>
  dplyr::filter(stringr::str_detect(base_name, "20250930"),
                !schedule %in% c("por", "xbrl"),
                !stringr::str_starts(base_name, "ffiec")) |>
  dplyr::pull(full_name)

stopifnot(
  is.character(pqs),
  length(pqs) > 0,
  all(file.exists(pqs))
)

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
    dplyr::select(IDRSSD, date, dplyr::all_of(cols))

  sql <- paste0(
    "SELECT IDRSSD, date, ",
    DBI::dbQuoteString(conn, sched), " AS schedule, item, value\n",
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

get_longs <- function(conn, pqs, dtype = "Float64", prefix = "") {
  dfs <- lapply(pqs, function(x) get_long(conn, x, dtype = dtype, prefix = prefix))
  dfs <- Filter(Negate(is.null), dfs)
  stopifnot(length(dfs) > 0L)
  Reduce(dplyr::union_all, dfs)
}

get_longs_agg <- function(conn, pqs, dtype = "Float64",
                          prefix = "", distinct = TRUE) {
  stopifnot(DBI::dbIsValid(conn))

  long_tbl <- get_longs(conn, pqs, dtype = dtype, prefix = prefix)
  long_sql <- dbplyr::sql_render(long_tbl)

  sched_expr <- if (isTRUE(distinct)) {
    "list_distinct(list(schedule)) AS schedule"
  } else {
    "list(schedule) AS schedule"
  }

  sql <- paste0(
    "SELECT IDRSSD, date, item, value, ", sched_expr, "\n",
    "FROM (", long_sql, ") AS x\n",
    "GROUP BY 1,2,3,4"
  )

  out <- dplyr::tbl(conn, dplyr::sql(sql))
  assert_no_dups(conn, out, keys = c("IDRSSD", "date", "item"))
  out
}

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

db <- DBI::dbConnect(duckdb::duckdb())

get_longs_agg(db, pqs, dtype = "Float64")
get_longs_agg(db, pqs, dtype = "Int32")
get_longs_agg(db, pqs, dtype = "Date32")
get_longs_agg(db, pqs, dtype = "Utf8")
get_longs_agg(db, pqs, dtype = "Boolean")
