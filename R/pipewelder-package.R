#' pipeweldr: Pipelines for Economic Time Series
#'
#' This package provides a consistent, modular set of tools for retrieving,
#' cleaning, and processing both public economic data and health data from the
#' Bureau of Labor Statistics (BLS), Federal Reserve Economic Data (FRED), and
#' the Centers for Disease Control and Prevention (CDC).
#'
#' @section Pipelines:
#' - `get_bls()` retrieves BLS time series across large time ranges.
#' - `get_fred()` retrieves and tidies FRED data from the St. Louis Fed.
#' - `get_cdc()` retrieves and aggregates CDC data.
#'
#' Internal helper functions handle chunking, formatting, and error management,
#' allowing end-users to call a single function for each pipeline.
#'
"_PACKAGE"
NULL
