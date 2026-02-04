#' Pivot long FFIEC data to wide format using DuckDB
#'
#' Creates a wide table from a DuckDB-backed lazy \code{tbl} containing long
#' FFIEC data (typically columns like \code{IDRSSD}, \code{date}, \code{item},
#' \code{value}). This is a lightweight, fast alternative to
#' [tidyr::pivot_wider()] that pushes the pivot operation into DuckDB.
#'
#' The function is designed for use with tables returned by [ffiec_scan_pqs()]
#' (or any DuckDB-backed lazy \code{tbl}). The DuckDB connection is inferred
#' from \code{data}.
#'
#' @param data A DuckDB-backed lazy \code{tbl} containing the long data to pivot.
#' @param id_cols Character vector of identifier columns that define unique rows
#'   in the wide output (default \code{c("IDRSSD", "date")}).
#' @param names_from Character scalar giving the column whose values become
#'   output column names (default \code{"item"}).
#' @param values_from Character scalar giving the column that supplies cell
#'   values (default \code{"value"}).
#' @param items Optional character vector of values of \code{names_from} to
#'   include. When supplied, the input is filtered to \code{names_from \%in\% items}
#'   before pivoting, and DuckDB is instructed to pivot only those columns. This
#'   is recommended when you want a stable set/order of output columns.
#' @param values_fn Character scalar naming a DuckDB aggregate function used to
#'   resolve multiple values per \code{id_cols + names_from} combination. The
#'   default is \code{"any_value"}, which is appropriate when uniqueness holds.
#'   Other common choices include \code{"sum"}, \code{"max"}, or \code{"min"}.
#'
#' @details
#' This helper supports a minimal subset of the [tidyr::pivot_wider()] interface:
#' \code{data}, \code{id_cols}, \code{names_from}, and \code{values_from}, plus
#' optional \code{items} and \code{values_fn}.
#'
#' For best results, ensure that \code{id_cols} combined with \code{names_from}
#' uniquely identify rows in \code{data}. If not, choose an appropriate
#' \code{values_fn} to aggregate duplicates.
#'
#' @return A DuckDB-backed lazy \code{tbl} in wide format.
#'
#' @examples
#' \dontrun{
#' db <- DBI::dbConnect(duckdb::duckdb())
#'
#' # Long table: IDRSSD, date, item, value
#' ffiec_float <- ffiec_scan_pqs(db, schedule = "ffiec_float")
#'
#' fields <- c("RCFD0081", "RCON0081", "RCFD0071", "RCON0071")
#'
#' wide <- ffiec_pivot(
#'   data = ffiec_float,
#'   id_cols = c("IDRSSD", "date"),
#'   names_from = "item",
#'   values_from = "value",
#'   items = fields,
#'   values_fn = "any_value"
#' )
#'
#' wide |> dplyr::glimpse()
#' }
#'
#' @export
ffiec_pivot <- function(data,
                        id_cols = c("IDRSSD", "date"),
                        names_from = "item",
                        values_from = "value",
                        items = NULL,
                        values_fn = "first") {

  stopifnot(
    inherits(data, "tbl"),
    is.character(id_cols), length(id_cols) >= 1L,
    is.character(names_from), length(names_from) == 1L,
    is.character(values_from), length(values_from) == 1L
  )

  # Extract DBI connection from lazy tbl
  conn <- dbplyr::remote_con(data)
  stopifnot(DBI::dbIsValid(conn))

  x_sql <- dbplyr::sql_render(data)

  # Optional IN-list + pre-filter
  in_list_sql <- NULL
  where_sql <- ""
  if (!is.null(items)) {
    stopifnot(is.character(items), length(items) >= 1L)
    in_list_sql <- paste(DBI::dbQuoteString(conn, items), collapse = ", ")
    where_sql <- paste0(
      "WHERE ", names_from, " IN (", in_list_sql, ")\n"
    )
  }

  # Select only what pivot needs
  base_sql <- paste0(
    "SELECT ", paste(c(id_cols, names_from, values_from), collapse = ", "), "\n",
    "FROM (", x_sql, ") AS x\n",
    where_sql
  )

  on_sql <- if (!is.null(in_list_sql)) {
    paste0(names_from, " IN (", in_list_sql, ")")
  } else {
    names_from
  }

  sql <- paste0(
    "SELECT *\n",
    "FROM (\n",
    "  PIVOT (", base_sql, ")\n",
    "  ON ", on_sql, "\n",
    "  USING ", values_fn, "(", values_from, ")\n",
    "  GROUP BY ", paste(id_cols, collapse = ", "), "\n",
    ") AS p"
  )

  dplyr::tbl(conn, dplyr::sql(sql))
}
