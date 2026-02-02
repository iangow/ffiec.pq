#' Load FFIEC schema mapping
#'
#' Returns the FFIEC XBRL schema bundled with the package.
#'
#' @return A tibble containing schema metadata.
#' @keywords internal
#' @noRd
get_ffiec_schema <- function() {
  utils::data("ffiec_schema", package = "ffiec.pq", envir = environment())
  ffiec_schema
}

#' Default XBRL-to-readr mapping
#' @keywords internal
#' @noRd
default_xbrl_to_readr <- function() {
  c(
    "xbrli:monetaryItemType"              = "d",
    "ffieci:nonNegativeMonetaryItemType"  = "d",
    "xbrli:integerItemType"               = "i",
    "xbrli:nonNegativeIntegerItemType"    = "i",
    "xbrli:pureItemType"                  = "c",
    "xbrli:booleanItemType"               = "l",
    "xbrli:stringItemType"                = "c"
  )
}

#' Default FFIEC column type overrides
#' @keywords internal
#' @noRd
default_ffiec_col_overrides <- function() {
  c(
    "RCON8678" = "c",  # text / mixed content
    "RCON9999" = "D",   # date-like, parse later
    "RIAD9106" = "D")
}
