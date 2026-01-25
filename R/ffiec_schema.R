#' FFIEC XBRL element schema mapping
#'
#' A dataset describing FFIEC XBRL concept elements extracted from FFIEC taxonomy
#' \code{concepts.xsd} files. Used to map element names to XBRL types when parsing
#' FFIEC call report files.
#'
#' @format A tibble with (at minimum) the following columns:
#' \describe{
#'   \item{name}{Element name (e.g., call report field identifier).}
#'   \item{type}{XBRL type (e.g., \code{"xbrli:monetaryItemType"}, \code{"xbrli:pureItemType"}).}
#' }
#'
#' @source Derived from FFIEC taxonomy concepts.xsd files (see data-raw/make_ffiec_schema.R).
"ffiec_schema"
