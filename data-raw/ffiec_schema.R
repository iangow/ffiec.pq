# data-raw/ffiec_schema.R

library(xml2)
library(dplyr)
library(purrr)
library(tibble)

raw_dir <- file.path(Sys.getenv("RAW_DATA_DIR"), "ffiec")
zipfiles <- list.files(raw_dir, pattern = "^_.*", full.names = TRUE)

safe_close <- function(con) {
  try({
    if (isOpen(con)) close(con)
  }, silent = TRUE)
  invisible(NULL)
}

read_concepts_xsd <- function(zipfile) {
  contents <- unzip(zipfile, list = TRUE)$Name

  path <- contents[grepl("concepts\\.xsd$", contents)]
  stopifnot(length(path) == 1)

  con <- unz(zipfile, path)
  on.exit(safe_close(con), add = TRUE)

  xsd <- xml2::read_xml(con)

  nodes <- xml2::xml_find_all(
    xsd,
    ".//*[local-name()='element']"
  )

  if (length(nodes) == 0) {
    stop("No <element> nodes found in concepts.xsd in ", basename(zipfile))
  }

  purrr::map_dfr(
    nodes,
    ~ tibble::as_tibble(as.list(xml2::xml_attrs(.x)))
  )
}
ffiec_schema <-
  purrr::map(zipfiles, read_concepts_xsd) |>
  dplyr::bind_rows() |>
  dplyr::distinct() |>
  dplyr::select(dplyr::any_of(c("name", "type")))

usethis::use_data(ffiec_schema, overwrite = TRUE)
