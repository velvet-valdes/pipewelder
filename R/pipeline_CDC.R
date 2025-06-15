#' Get a single batch (page) of CDC Socrata data
#'
#' @param series_id Socrata dataset ID (e.g., "489q-934x")
#' @param offset Row offset to begin from (default is 0)
#' @param limit Number of rows per page (default is 1000)
#' @param app_token Optional Socrata API token
#'
#' @return A tibble containing the batch of rows
#' @seealso \code{collect_cdc_batches()}
#' @keywords internal
get_cdc_batch <- function(series_id,
                          offset = 0,
                          limit = 1000,
                          app_token = NULL) {
  endpoint <- paste0("https://data.cdc.gov/resource/", series_id, ".json")

  res <- httr::GET(
    url = endpoint,
    query = list(`$limit` = limit, `$offset` = offset),
    httr::add_headers(`X-App-Token` = app_token)
  )

  stopifnot(httr::status_code(res) == 200)

  data <- jsonlite::fromJSON(httr::content(res, as = "text", encoding = "UTF-8"))
  tibble::as_tibble(data)
}


#' Collect paginated batches from CDC Socrata dataset
#'
#' @param series_id Dataset ID to pull from
#' @param app_token Optional API token
#' @param limit Page size (default 1000)
#' @param verbose Print progress
#'
#' @return A list of tibbles (each page of results)
#' @seealso \code{get_cdc_batch()}, \code{harmonize_flag_column()}
#' @keywords internal
collect_cdc_batches <- function(series_id,
                                app_token = NULL,
                                limit = 1000,
                                verbose = FALSE) {
  batches <- list()
  offset <- 0
  i <- 1

  repeat {
    if (verbose)
      message("Fetching batch ", i, " (offset = ", offset, ")")
    batch <- get_cdc_batch(
      series_id,
      offset = offset,
      limit = limit,
      app_token = app_token
    )
    if (nrow(batch) == 0)
      break
    batches[[i]] <- batch
    offset <- offset + limit
    i <- i + 1
  }

  batches
}


#' Harmonize the presence of the 'flag' column across CDC data batches
#'
#' Ensures that if any batch includes the 'flag' column, all batches have it
#' in the same position, filled with NA where missing.
#'
#' @param batches A list of tibbles returned by `collect_cdc_batches()`
#'
#' @return A new list of tibbles with harmonized 'flag' columns
#' @seealso \code{collect_cdc_batches()}, \code{bind_cdc_batches()}
#' @keywords internal
harmonize_flag_column <- function(batches) {
  # Check if any batch contains 'flag'
  any_has_flag <- any(purrr::map_lgl(batches, ~ "flag" %in% names(.x)))
  if (!any_has_flag)
    return(batches)

  # Get the reference column order from the first batch that contains 'flag'
  ref_batch <- purrr::detect(batches, ~ "flag" %in% names(.x))
  ref_names <- names(ref_batch)

  purrr::map(batches, function(df) {
    # Add 'flag' if it's missing
    if (!"flag" %in% names(df)) {
      df$flag <- NA_character_
    }

    # Reorder to match reference
    df <- df[ref_names]
    df
  })
}


#' Combine CDC batches into a single tibble
#'
#' @param batches A list of tibbles as returned by `collect_cdc_batches()`
#'
#' @return A single tibble with all rows
#' @seealso \code{harmonize_flag_column()}, \code{get_cdc()}
#' @keywords internal
bind_cdc_batches <- function(batches) {
  dplyr::bind_rows(batches)
}


#' Retrieve and combine CDC dataset records with schema harmonization
#'
#' Downloads all records from a CDC Socrata dataset using paginated API calls,
#' harmonizes the presence of the `flag` column across batches (if applicable),
#' and returns a single tibble.
#'
#' This function wraps the three-step process of:
#' 1. Collecting paginated data chunks
#' 2. Ensuring consistent schema (with respect to the `flag` column)
#' 3. Binding all records into a single tibble
#'
#' @param series_id A Socrata dataset ID string (e.g., "489q-934x")
#'
#' @return A tibble containing all records from the dataset with consistent columns
#' @examples
#' \dontrun{
#'   suicide_qtr <- get_cdc("489q-934x")
#'   suicide_final <- get_cdc("p7se-k3ix")
#' }
#'
#' @seealso \code{collect_cdc_batches()}, \code{harmonize_flag_column()}, \code{bind_cdc_batches()}
#' @export
get_cdc <- function(series_id) {
  return(bind_cdc_batches(harmonize_flag_column(collect_cdc_batches(series_id))))
}
