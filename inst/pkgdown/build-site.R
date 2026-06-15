pkgdown_ns <- asNamespace("pkgdown")
unlockBinding("cran_link", pkgdown_ns)
assign("cran_link", function(pkg) NULL, envir = pkgdown_ns)
lockBinding("cran_link", pkgdown_ns)

pkgdown::build_site(
  pkg = ".",
  examples = FALSE,
  install = TRUE,
  preview = FALSE
)
