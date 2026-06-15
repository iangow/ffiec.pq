pkgdown_ns <- asNamespace("pkgdown")
unlockBinding("cran_link", pkgdown_ns)
assign("cran_link", function(pkg) NULL, envir = pkgdown_ns)
lockBinding("cran_link", pkgdown_ns)

pkgdown::deploy_to_branch(
  pkg = ".",
  branch = "gh-pages",
  examples = FALSE,
  install = TRUE
)
