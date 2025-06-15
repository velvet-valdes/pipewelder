# FRED data pipeline functions


#' Sanitize a Date Input for FRED Queries
#'
#' Normalizes date input to `"YYYY-MM-DD"` format, allowing flexible entry formats
#' including `"YYYY"`, `"YYYY-MM"`, and `"YYYY-MM-DD"` with either slashes or dashes.
#' Ensures the input is a valid date and returns it as a properly formatted character string.
#'
#' @seealso [get_fred_series_raw()]
#'
#' @param date_input A character string (e.g., "2024", "2024-06", "2024/06/13")
#'   or a `Date` object to be cleaned and standardized.
#'
#' @return A character string in `"YYYY-MM-DD"` format.
#'
#' @examples
#' sanitize_date("2024")
#' sanitize_date("2024-06")
#' sanitize_date("2024/06/13")
#' sanitize_date(as.Date("2024-06-13"))
#'
#' @keywords internal
#' @noRd
sanitize_date <- function(date_input) {
  # If it's already a Date, return in correct format
  if (inherits(date_input, "Date")) {
    return(format(date_input, "%Y-%m-%d"))
  }
  
  if (!is.character(date_input)) {
    stop("date_input must be a character string or Date object")
  }
  
  # Normalize all separators to dashes
  date_input <- gsub("/", "-", date_input)
  
  # Add missing month/day defaults
  if (grepl("^\\d{4}$", date_input)) {
    date_input <- paste0(date_input, "-01-01")
  } else if (grepl("^\\d{4}-\\d{1,2}$", date_input)) {
    date_input <- paste0(date_input, "-01")
  }
  
  # Validate final format
  date_parsed <- as.Date(date_input)
  
  if (is.na(date_parsed)) {
    stop(
      "Invalid date format. Expected YYYY, YYYY-MM, or YYYY-MM-DD (slashes or dashes allowed)."
    )
  }
  
  return(format(date_parsed, "%Y-%m-%d"))
}


#' Retrieve Raw FRED Series Data
#'
#' Queries the Federal Reserve Economic Data (FRED) API for a specific series
#' between a start and end date. Returns the parsed JSON response as a list or data frame.
#' This retrieves a single series and is used in [get_fred()]
#'
#' @param series_id A character string representing the FRED series ID (e.g., `"UNRATE"`).
#' @param start_date The start date of the series in `"YYYY"`, `"YYYY-MM"`, `"YYYY-MM-DD"` format,
#'   or a `Date` object.
#' @param end_date The end date of the series in `"YYYY"`, `"YYYY-MM"`, `"YYYY-MM-DD"` format,
#'   or a `Date` object.
#'
#' @seealso [sanitize_date()], [get_fred()]
#'
#' @return A named list containing the parsed JSON response from the FRED API.
#'
#' @details This function depends on the `FRED_KEY` environment variable to authenticate with
#' the API. Dates are cleaned using [sanitize_date()] to ensure valid API query formatting.
#'
#' @examples
#' \dontrun{
#' Sys.setenv(FRED_KEY = "your_fred_api_key")
#' get_fred_series_raw("UNRATE", "2020", "2021-12")
#' }
#'
#' @importFrom httr GET content
#' @importFrom jsonlite fromJSON
#' @keywords internal
#' @noRd
get_fred_series_raw <- function(series_id, start_date, end_date) {
  api_key <- Sys.getenv("FRED_KEY")
  
  start_date_clean <- sanitize_date(start_date)
  end_date_clean <- sanitize_date(end_date)
  
  url <- paste0(
    "https://api.stlouisfed.org/fred/series/observations?",
    "series_id=",
    series_id,
    "&api_key=",
    api_key,
    "&file_type=json",
    "&observation_start=",
    start_date_clean,
    "&observation_end=",
    end_date_clean
  )
  
  res <- httr::GET(url)
  res_text <- httr::content(res, as = "text", encoding = "UTF-8")
  parsed <- jsonlite::fromJSON(res_text, flatten = TRUE)
  
  return(parsed)
}


#' Tidy FRED Series Output
#'
#' Converts the raw `observations` component from a FRED API response
#' (as returned by [get_fred_series_raw()]) into a clean tibble with properly typed columns.
#' This is used inside the [get_fred()] function.
#'
#' @param fred_result A list returned by [get_fred_series_raw()], containing a named element `observations`
#'   with date and value fields.
#'
#' @seealso [get_fred_series_raw()], [get_fred()]
#'
#' @return A tibble with two columns:
#' \describe{
#'   \item{date}{Date column parsed as `Date` class}
#'   \item{value}{Numeric value of the series observation}
#' }
#'
#' @examples
#' \dontrun{
#' raw <- get_fred_series_raw("UNRATE", "2020", "2021")
#' tidy_fred_series(raw)
#' }
#'
#' @importFrom tibble as_tibble
#' @importFrom dplyr transmute
#' @keywords internal
#' @noRd
tidy_fred_series <- function(fred_result) {
  df <- fred_result$observations
  
  df <- tibble::as_tibble(df) |>
    dplyr::transmute(date = as.Date(date), value = as.numeric(value))
  
  return(df)
}


#' Retrieve and Tidy FRED Series Data
#'
#' A convenience wrapper that retrieves a FRED time series using [get_fred_series_raw()]
#' and immediately formats it using [tidy_fred_series()]. Returns a clean tibble of
#' observations between the specified dates.
#'
#' @details Internally uses [get_fred_series_raw()] to fetch the raw JSON data and [tidy_fred_series()] to format the result.
#'
#' @param series_id A character string representing the FRED series ID (e.g., `"UNRATE"`).
#' @param start_date Start date of the time series in `"YYYY"`, `"YYYY-MM"`, `"YYYY-MM-DD"` format,
#'   or a `Date` object.
#' @param end_date Optional. End date in the same format as `start_date`. Defaults to today's date.
#'
#' @seealso [get_fred_series_raw()], [tidy_fred_series()]
#'
#' @return A tibble with `date` and `value` columns, one row per observation.
#'
#' @examples
#' \dontrun{
#' Sys.setenv(FRED_KEY = "your_fred_api_key")
#' get_fred("UNRATE", "2020-01-01")
#' }
#'
#' @export
get_fred <- function(series_id, start_date, end_date = Sys.Date()) {
  return(tidy_fred_series(get_fred_series_raw(series_id, start_date, end_date)))
}