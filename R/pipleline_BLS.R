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
  data_df <- series_df$data[[1]]

  cleaned_df <- dplyr::mutate(
    data_df,
    year = as.integer(year),
    value = as.numeric(value),
    month = as.integer(stringr::str_remove(period, "M")),
    date = as.Date(sprintf("%d-%02d-01", year, month)),
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


#' Construct BLS Data
#'
#' Iterates over all tabs in a Google Sheet with names matching the pattern
#' `"BLS-"`, retrieves BLS series for each ID listed, and returns a named list
#' of tibbles. Results are cached by series ID and date range to avoid redundant
#' API calls.
#'
#' @param sheet A list of tibbles (typically returned by `get_full_gsheet()`),
#'   with one or more elements named like `"BLS-XXX"`, each containing a `Series
#'   ID` column and optionally a `Description` column.
#' @param start_year The starting year of the time series (default is 2000).
#' @param end_year The ending year of the time series (default is the current
#'   year).
#' @param flush Logical. If `TRUE`, clears the existing cache for the relevant
#'   BLS series before downloading new data.
#'
#' @return A named list of tibbles, one for each BLS series. Each tibble
#'   includes an attribute `"description"` from the sheet if present.
#'
#' @details Cached files are stored in a `pipewelder_cache/` folder in the
#' working directory. File names are hashed using the series ID, start year, and
#' end year to uniquely identify each request.
#'
#' @export
construct_bls <- function(sheet,
                          start_year = 2000,
                          end_year = as.integer(format(Sys.Date(), "%Y")),
                          flush = FALSE) {
  # Filter only BLS-related tabs
  bls_keys <- grep("^BLS-", names(sheet), value = TRUE)

  # Ensure cache directory exists
  cache_dir <- file.path(getwd(), "pipewelder_cache")
  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir)
  }

  # Initialize result list
  results <- list()

  # Iterate over each BLS tab
  for (key in bls_keys) {
    tab <- sheet[[key]]

    # Sanity check: skip if no Series ID column
    if (!"Series ID" %in% names(tab))
      next

    for (i in seq_len(nrow(tab))) {
      series_id <- tab[["Series ID"]][i]
      description <- tab$Description[i]

      if (is.na(series_id) || series_id == "")
        next

      var_name <- paste0(gsub("-", "_", key), "_", series_id)

      # Generate cache key and path
      cache_key_input <- paste(series_id, start_year, end_year, sep = "_")
      cache_key <- digest::digest(cache_key_input, algo = "md5")
      cache_file <- file.path(cache_dir, paste0("bls_", cache_key, ".rds"))

      message(sprintf("Retrieving BLS series: %s", series_id))

      # Delete existing cache if flush is requested
      if (flush && file.exists(cache_file)) {
        file.remove(cache_file)
        message("↪ Flushed existing cache.")
      }

      # Load from cache or fetch fresh data
      result <- tryCatch({
        if (file.exists(cache_file)) {
          message("↪ Using cached series. Pass 'flush = TRUE' to override.")
          readRDS(cache_file)
        } else {
          data <- get_bls(series_id, start_year, end_year)
          saveRDS(data, cache_file)
          Sys.sleep(3)  # Delay to avoid hitting rate limits
          data
        }
      }, error = function(e) {
        warning(sprintf(
          "Failed to retrieve BLS series '%s': %s",
          series_id,
          e$message
        ))
        NULL
      })

      # Attach description if retrieval succeeded
      if (!is.null(result)) {
        attr(result, "description") <- description
        results[[var_name]] <- result
      }

    }
  }

  return(results)
}
