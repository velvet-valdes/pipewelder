# BLS data pipeline functions


#' Split a Year Range into Fixed-Size Chunks
#'
#' Splits a range of years into chunks of up to a specified size (default 20 years).
#' Useful for APIs (like BLS) that restrict the maximum date range per request.
#' Not intended for direct use by end users.
#'
#' @param start_year An integer representing the starting year.
#' @param end_year An integer representing the ending year. Must be >= `start_year`.
#' @param chunk_size Maximum number of years per chunk. Defaults to 20.
#'
#' @return A list of lists, each containing `start` and `end` keys representing chunk boundaries.
#'
#' @examples
#'
#' \dontrun{
#' split_year_range(2000, 2023, chunk_size = 10)
#' }
#'
#' @seealso \code{get_bls_series_chunks()}
#'
#' @keywords internal

split_year_range <- function(start_year, end_year, chunk_size = 20) {
  if (start_year > end_year)
    stop("start_year must be <= end_year")

  years <- seq(start_year, end_year)
  chunks <- split(years, ceiling(seq_along(years) / chunk_size))

  chunk_ranges <- lapply(chunks, function(yr) {
    list(start = min(yr), end = max(yr))
  })

  return(chunk_ranges)
}


#' Retrieve a Single BLS Data Chunk
#'
#' Makes a single API request to the Bureau of Labor Statistics (BLS) public API
#' for one series over a limited year range (e.g., a 20-year chunk). Intended for internal use
#' inside [get_bls_series_chunks()], not called directly by users.
#'
#' @param series_id A character string representing the BLS series ID.
#' @param chunk A named list containing `start` and `end` years, typically produced by [split_year_range()].
#'
#' @return A parsed JSON object (as a list) containing the raw BLS API response.
#'
#' @details Requires the environment variable `BLS_KEY` to be set with a valid BLS API key.
#'
#' @examples
#' \dontrun{
#' chunk <- list(start = 2000, end = 2020)
#' Sys.setenv(BLS_KEY = "your_bls_api_key")
#' get_bls_chunk("LNS14000000", chunk)
#' }
#'
#' @importFrom httr POST content content_type_json
#' @importFrom jsonlite toJSON fromJSON
#' @seealso \code{get_bls_series_chunks()}
#' @keywords internal
get_bls_chunk <- function(series_id, chunk) {
  json_body <- jsonlite::toJSON(
    list(
      seriesid = list(series_id),
      startyear = chunk$start,
      endyear = chunk$end,
      registrationKey = Sys.getenv("BLS_KEY")
    ),
    auto_unbox = TRUE
  )

  res <- httr::POST(
    "https://api.bls.gov/publicAPI/v2/timeseries/data/",
    body = json_body,
    encode = "raw",
    httr::content_type_json()
  )

  res_json <- httr::content(res, as = "text", encoding = "UTF-8")
  parsed <- jsonlite::fromJSON(res_json, flatten = TRUE)

  return(parsed)
}


#' Tidy a Single BLS API Response
#'
#' Cleans and formats the data portion of a single BLS API response. Converts types,
#' extracts date components, and returns a tidy tibble. Intended for internal use inside
#' [merge_bls_series_chunks()], not called directly by users.
#'
#' @param res A list object returned by [get_bls_chunk()], containing the nested `Results$series$data` structure.
#'
#' @return A tibble with columns: `date`, `year`, `month`, `value`, and `seriesID`.
#'
#' @examples
#' \dontrun{
#' raw <- get_bls_chunk("LNS14000000", list(start = 2000, end = 2020))
#' tidy_bls_single_result(raw)
#' }
#'
#' @importFrom dplyr mutate select
#' @importFrom stringr str_remove
#' @importFrom tibble as_tibble
#' @seealso \code{merge_bls_series_chunks()}
#' @keywords internal
tidy_bls_single_result <- function(res) {
  series_df <- res[["Results"]][["series"]]
  data_df <- series_df$data[[1]]

  cleaned_df <- dplyr::mutate(
    data_df,
    year = as.integer(year),
    value = as.numeric(value),
    month = as.integer(stringr::str_remove(period, "M")),
    date = as.Date(sprintf("%d-%02d-01", year, month)),
    seriesID = series_df$seriesID
  )

  cleaned_df <- dplyr::select(cleaned_df, date, year, month, value, seriesID)
  return(tibble::as_tibble(cleaned_df))
}


#' Retrieve and Aggregate BLS Data Chunks for a Series
#'
#' Iterates over a list of year chunks (e.g., from [split_year_range()]) and makes repeated API calls
#' using [get_bls_chunk()] to collect BLS data for a single series. Used internally by
#' [merge_bls_series_chunks()] to fetch all segments before merging.
#'
#' @param series_id A character string representing the BLS series ID.
#' @param chunk_list A list of year ranges, each with `start` and `end` values (as produced by [split_year_range()]).
#'
#' @return A list of raw BLS API response objects, one per chunk.
#'
#' @examples
#' \dontrun{
#' chunks <- split_year_range(2000, 2023, chunk_size = 10)
#' Sys.setenv(BLS_KEY = "your_bls_api_key")
#' get_bls_series_chunks("LNS14000000", chunks)
#' }
#'
#' @seealso \code{get_bls_chunk()}, \code{split_year_range()}
#' @keywords internal
get_bls_series_chunks <- function(series_id, chunk_list) {
  results <- list()

  for (chunk in chunk_list) {
    chunk_result <- get_bls_chunk(series_id, chunk)
    results <- append(results, list(chunk_result))
  }

  return(results)
}


#' Merge and Tidy Multiple BLS API Chunk Results
#'
#' Applies [tidy_bls_single_result()] to each result in a list of BLS API responses and combines
#' them into a single ordered tibble. Intended for internal use in the BLS data pipeline.
#'
#' @param result_list A list of BLS API response objects, typically returned by [get_bls_series_chunks()].
#'
#' @return A tibble combining all observations, sorted by `date`.
#'
#' @examples
#' \dontrun{
#' chunks <- split_year_range(2000, 2020)
#' raw_results <- get_bls_series_chunks("LNS14000000", chunks)
#' merged <- merge_bls_series_chunks(raw_results)
#' }
#'
#' @importFrom dplyr bind_rows arrange
#' @seealso \code{tidy_bls_single_result()}, \code{get_bls_series_chunks()}
#' @keywords internal
merge_bls_series_chunks <- function(result_list) {
  results <- lapply(result_list, tidy_bls_single_result)
  return(dplyr::arrange(dplyr::bind_rows(results), date))
}


#' Retrieve and Tidy BLS Series Data
#'
#' Fetches data for a single BLS series across a multi-year range. Handles API chunking automatically
#' by splitting large ranges into smaller segments (e.g., 20-year chunks), retrieving each piece via the
#' BLS API, tidying the results, and combining them into a single tibble.
#'
#' @details Internally uses [split_year_range()] to break the date range into chunks,
#' [get_bls_series_chunks()] to fetch each chunk from the API, and [merge_bls_series_chunks()]
#' to clean and combine the results.
#'
#' @param series_id A character string representing the BLS series ID (e.g., `"LNS14000000"`).
#' @param start_year An integer representing the starting year of the desired time series.
#' @param end_year An optional integer for the ending year. Defaults to the current year (`format(Sys.Date(), "%Y")`).
#'
#' @return A tibble with columns: `date`, `year`, `month`, `value`, and `seriesID`, one row per observation.
#'
#' @details Requires the `BLS_KEY` environment variable to be set with a valid BLS API key.
#'
#' @examples
#' \dontrun{
#' Sys.setenv(BLS_KEY = "your_bls_api_key")
#' get_bls("LNS14000000", 2000, 2023)
#' }
#'
#' @seealso \code{split_year_range()}, \code{get_bls_series_chunks()}, \code{merge_bls_series_chunks()}
#' @export
get_bls <- function(series_id,
                    start_year,
                    end_year = format(Sys.Date(), "%Y")) {
  chunk_list <- split_year_range(start_year, end_year)
  result_list <- get_bls_series_chunks(series_id, chunk_list)
  return(merge_bls_series_chunks(result_list))
}
