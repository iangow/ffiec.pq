#' FFIEC item master table
#'
#' A reference table containing one row per FFIEC data item that appears in the
#' non-POR Call Report Parquet datasets managed by \code{ffiec.pq}.
#'
#' Each item corresponds to a unique combination of mnemonic and item code
#' (e.g., \code{"RCFD0010"}). Item names are lightly normalized for presentation
#' (proper case for protected regulatory acronyms is preserved).
#'
#' @format A tibble with one row per item and the following columns:
#' \describe{
#'   \item{item}{FFIEC item identifier formed as \code{mnemonic + item_code}
#'     (e.g., \code{"RCFD0010"}).}
#'   \item{mnemonic}{Schedule mnemonic (e.g., \code{"RCFD"}, \code{"RCON"}).}
#'   \item{item_code}{Four-character FFIEC item code (character).}
#'   \item{item_name}{Human-readable item name, normalized for presentation.}
#'   \item{data_type}{Arrow data type used for item.}
#' }
#'
#' @details
#' This table represents the union of items found in the Parquet datasets
#' (excluding POR schedules). Items defined in the MDRM but never observed
#' in the Parquet data are excluded.
#'
#' @seealso \code{\link{ffiec_item_details}}
#'
#' @examples
#' ffiec_items
#' dplyr::filter(ffiec_items, stringr::str_detect(item_name, "Tier 1"))
"ffiec_items"
