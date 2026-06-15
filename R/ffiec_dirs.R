#' Resolve the FFIEC raw data repository directory
#'
#' Resolves the root directory used for raw source data from an explicit
#' argument or the `RAW_DATA_DIR` environment variable. In an interactive
#' session, when neither is available, the helper asks the user to choose or
#' enter a directory and offers to persist it in either project-level
#' `.Renviron` or user-level `~/.Renviron`.
#'
#' @param raw_data_dir Optional raw data repository root.
#' @param prompt If `TRUE`, prompt interactively when `raw_data_dir` and
#'   `RAW_DATA_DIR` are both missing.
#' @param fallback Directory used as the default when prompting.
#'
#' @return A directory path as a character string, or `NULL` when prompting is
#'   disabled and no directory can be resolved.
#' @export
ffiec_raw_data_dir <- function(raw_data_dir = NULL, prompt = interactive(),
                               fallback = ".") {
  ffiec_env_dir(
    value = raw_data_dir,
    envvar = "RAW_DATA_DIR",
    label = "raw FFIEC data",
    prompt = prompt,
    fallback = fallback
  )
}

#' Resolve the FFIEC Parquet data repository directory
#'
#' Resolves the root directory used for processed Parquet data from an explicit
#' argument or the `DATA_DIR` environment variable. In an interactive session,
#' when neither is available, the helper asks the user to choose or enter a
#' directory and offers to persist it in either project-level `.Renviron` or
#' user-level `~/.Renviron`.
#'
#' @param data_dir Optional Parquet data repository root.
#' @param prompt If `TRUE`, prompt interactively when `data_dir` and `DATA_DIR`
#'   are both missing.
#' @param fallback Directory used as the default when prompting.
#'
#' @return A directory path as a character string, or `NULL` when prompting is
#'   disabled and no directory can be resolved.
#' @export
ffiec_data_dir <- function(data_dir = NULL, prompt = interactive(),
                           fallback = ".") {
  ffiec_env_dir(
    value = data_dir,
    envvar = "DATA_DIR",
    label = "processed FFIEC Parquet data",
    prompt = prompt,
    fallback = fallback
  )
}

#' @keywords internal
#' @noRd
ffiec_env_dir <- function(value, envvar, label, prompt = interactive(),
                          fallback = ".") {
  if (!is.null(value) && nzchar(value)) {
    return(path.expand(value))
  }

  env_value <- Sys.getenv(envvar, unset = "")
  if (nzchar(env_value)) {
    return(path.expand(env_value))
  }

  if (!isTRUE(prompt)) {
    return(NULL)
  }

  ffiec_prompt_env_dir(envvar = envvar, label = label, fallback = fallback)
}

#' @keywords internal
#' @noRd
ffiec_prompt_env_dir <- function(envvar, label, fallback = ".") {
  message(envvar, " has not been set.")
  use_chooser <- ffiec_ask_yes_no(
    paste0("Use a directory chooser to select ", envvar, "? [Y/n] "),
    default = TRUE
  )

  dir <- if (use_chooser) {
    ffiec_choose_dir(
      default = path.expand(fallback),
      caption = paste("Select", label, "directory")
    )
  } else {
    NULL
  }

  if (is.null(dir) || !nzchar(dir)) {
    dir <- trimws(readline(
      paste0(envvar, " path (existing directory or new path): ")
    ))
  }
  if (!nzchar(dir)) {
    stop(envvar, " cannot be empty.", call. = FALSE)
  }

  dir <- path.expand(dir)
  if (!dir.exists(dir)) {
    create <- ffiec_ask_yes_no(
      paste0("Create ", envvar, " directory '", dir, "'? [Y/n] "),
      default = TRUE
    )
    if (!create) {
      stop(envvar, " directory does not exist: ", dir, call. = FALSE)
    }
    dir.create(dir, recursive = TRUE, showWarnings = FALSE)
    if (!dir.exists(dir)) {
      stop("Could not create ", envvar, " directory: ", dir, call. = FALSE)
    }
  }

  do.call(Sys.setenv, stats::setNames(list(dir), envvar))
  scope <- ffiec_prompt_env_scope(envvar)
  if (!identical(scope, "none")) {
    ffiec_set_renviron_value(
      envvar,
      dir,
      renviron = ffiec_renviron_path(scope)
    )
  }

  dir
}

#' @keywords internal
#' @noRd
ffiec_choose_dir <- function(default = getwd(), caption = "Select directory") {
  if (requireNamespace("rstudioapi", quietly = TRUE) &&
      isTRUE(tryCatch(rstudioapi::isAvailable(), error = function(e) FALSE))) {
    selected <- tryCatch(
      rstudioapi::selectDirectory(caption = caption, path = default),
      error = function(e) NULL
    )
    if (!is.null(selected) && nzchar(selected)) {
      return(selected)
    }
  }

  if (identical(.Platform$OS.type, "windows")) {
    selected <- tryCatch(
      utils::choose.dir(default = default, caption = caption),
      error = function(e) NULL
    )
    if (!is.null(selected) && !is.na(selected) && nzchar(selected)) {
      return(selected)
    }
  }

  selected <- tryCatch(
    tcltk::tk_choose.dir(default = default, caption = caption),
    error = function(e) NULL
  )
  if (!is.null(selected) && !is.na(selected) && nzchar(selected)) {
    return(selected)
  }

  NULL
}

#' @keywords internal
#' @noRd
ffiec_prompt_env_scope <- function(envvar) {
  repeat {
    answer <- tolower(trimws(readline(
      paste0(
        "Store ", envvar,
        " in project .Renviron, user ~/.Renviron, or not at all? ",
        "[project/user/no] "
      )
    )))
    if (answer %in% c("p", "project")) {
      return("project")
    }
    if (answer %in% c("u", "user")) {
      return("user")
    }
    if (answer %in% c("", "n", "no", "none")) {
      return("none")
    }
    message("Enter 'project', 'user', or 'no'.")
  }
}

#' @keywords internal
#' @noRd
ffiec_ask_yes_no <- function(prompt, default = FALSE) {
  answer <- tolower(trimws(readline(prompt)))
  if (!nzchar(answer)) {
    return(isTRUE(default))
  }
  answer %in% c("y", "yes")
}

#' @keywords internal
#' @noRd
ffiec_renviron_path <- function(scope = c("user", "project")) {
  scope <- match.arg(scope)
  if (identical(scope, "project")) {
    return(file.path(getwd(), ".Renviron"))
  }
  file.path(path.expand("~"), ".Renviron")
}

#' @keywords internal
#' @noRd
ffiec_set_renviron_value <- function(name, value, renviron) {
  if (!nzchar(name) || !nzchar(value)) {
    stop("`.Renviron` variable name and value must be non-empty.", call. = FALSE)
  }

  lines <- if (file.exists(renviron)) {
    readLines(renviron, warn = FALSE)
  } else {
    character()
  }

  pattern <- paste0("^\\s*", name, "\\s*=")
  value_lines <- grepl(pattern, lines)
  env_line <- paste0(name, "=", value)
  if (any(value_lines)) {
    lines[which(value_lines)[[1]]] <- env_line
    duplicate_lines <- which(value_lines)[-1L]
    if (length(duplicate_lines) > 0L) {
      lines <- lines[-duplicate_lines]
    }
  } else {
    lines <- c(lines, env_line)
  }

  dir.create(dirname(renviron), recursive = TRUE, showWarnings = FALSE)
  writeLines(lines, renviron)
  message("Stored ", name, " in ", renviron, ".")
}
