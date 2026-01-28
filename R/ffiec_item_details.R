#' FFIEC item metadata and reporting details
#'
#' A reference table describing FFIEC data items over time, including reporting
#' form, effective date ranges, confidentiality status, and descriptive metadata
#' as provided in the MDRM.
#'
#' Items may appear multiple times if their definitions or reporting attributes
#' change over time.
#'
#' @format A tibble with one row per item–period–reporting form combination and
#' the following columns:
#' \describe{
#'   \item{item}{FFIEC item identifier (e.g., \code{"RCFD0010"}).}
#'   \item{reporting_form}{Reporting form to which the item applies
#'     (e.g., FFIEC 031, 041, 051).}
#'   \item{start_date}{Date on which the item definition becomes effective.}
#'   \item{end_date}{Date on which the item definition ceases to be effective
#'     (NA if still active).}
#'   \item{confidentiality}{Confidentiality indicator from the MDRM.}
#'   \item{description}{Long-form item description from the MDRM.}
#'   \item{seriesglossary}{Series glossary text, if provided.}
#'   \item{itemtype}{Item type classification from the MDRM.}
#' }
#'
#' @details
#' This table reflects MDRM metadata only for items that appear in the Parquet
#' datasets managed by \code{ffiec.pq}. Date fields are stored as \code{Date}
#' objects; MDRM sentinel dates (e.g., 9999-12-31) are treated as missing.
#'
#' The \code{itemtype} column classifies items according to MDRM item type codes.
#' In the current Parquet datasets managed by \code{ffiec.pq}, the following
#' item types are observed:
#'
#' \describe{
#'   \item{\code{F}}{Financial / reported. The item is submitted directly by the reporter.}
#'   \item{\code{D}}{Derived. The item is derived from other stored variables.}
#'   \item{\code{R}}{Rate. The item is stored as a decimal value
#'     (e.g., 28\% stored as 0.28).}
#'   \item{\code{P}}{Percentage. The item is stored as a percentage value
#'     (e.g., 28\% stored as 28).}
#'   \item{\code{S}}{Structure. The item describes an institutional characteristic.}
#' }
#'
#' Other MDRM item types defined by the Federal Reserve (e.g., projected or
#' examination/supervision items) do not appear in the Parquet datasets and are
#' therefore not represented in this table.
#'
#' @seealso \code{\link{ffiec_items}}, \code{\link{ffiec_item_schedules}}
#'
#' @examples
#' # Active definitions for a single item
#' dplyr::filter(ffiec_item_details, item == "RCFD2170", is.na(end_date))
"ffiec_item_details"
