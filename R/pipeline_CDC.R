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


#' Coerce column types for finalized CDC dataset
#'
#' This function standardizes the column types of a CDC dataset with the
#' finalized structure (e.g., dataset ID 'p7se-k3ix'), ensuring proper
#' coercion to factors, integers, numerics, and date formats. It also
#' handles missing flags and reorders columns to prioritize `time_period`
#' and `time_period_id` for consistency.
#'
#' @param df A tibble returned from the CDC API with the finalized structure.
#' @return A tibble with coerced column types and reordered columns.
#' @keywords internal
clean_cdc_finalized <- function(df) {
  df$topic             <- as.factor(df$topic)
  df$classification    <- as.factor(df$classification)
  df$classification_id <- as.integer(df$classification_id)
  df$group             <- as.factor(df$group)
  df$group_id          <- as.integer(df$group_id)
  df$group_order       <- as.integer(df$group_order)
  df$subgroup          <- as.factor(df$subgroup)
  df$subgroup_id       <- as.integer(df$subgroup_id)
  df$subgroup_order    <- as.integer(df$subgroup_order)
  df$estimate_type     <- as.factor(df$estimate_type)
  df$estimate_type_id  <- as.integer(df$estimate_type_id)
  df$time_period       <- as.Date(paste0(df$time_period, "-01-01"))
  df$time_period_id    <- as.integer(df$time_period_id)
  df$estimate          <- as.numeric(df$estimate)
  df$standard_error    <- as.numeric(df$standard_error)
  df$estimate_lci      <- as.numeric(df$estimate_lci)
  df$estimate_uci      <- as.numeric(df$estimate_uci)
  df$footnote_id_list  <- as.factor(df$footnote_id_list)
  df$flag <- as.factor(ifelse(is.na(df$flag), "None", df$flag))

  # Move time_period and time_period_id to the front
  if (all(c("time_period", "time_period_id") %in% names(df))) {
    new_order <- c("time_period", "time_period_id", setdiff(names(df), c("time_period", "time_period_id")))
    df <- df[, new_order]
  }

  return(df)
}

#' Coerce column types for historical CDC dataset
#'
#' This function standardizes column types for historical-format CDC datasets
#' (e.g., dataset ID '9j2v-jamp'). It converts character fields into appropriate
#' factor, integer, numeric, or date types. It also handles missing flags and
#' reorders the output to place `year` and `year_num` as the first columns.
#'
#' @param df A tibble with the historical structure.
#' @return A tibble with coerced types and column reordering.
#' @keywords internal
clean_cdc_historical <- function(df) {
  df$indicator        <- as.factor(df$indicator)
  df$unit             <- as.factor(df$unit)
  df$unit_num         <- as.integer(df$unit_num)
  df$stub_name        <- as.factor(df$stub_name)
  df$stub_name_num    <- as.integer(df$stub_name_num)
  df$stub_label       <- as.factor(df$stub_label)
  df$stub_label_num   <- as.integer(df$stub_label_num)
  df$year             <- as.Date(paste0(df$year, "-01-01"))
  df$year_num         <- as.integer(df$year_num)
  df$age              <- as.factor(df$age)
  df$age_num          <- as.integer(df$age_num)
  df$estimate         <- as.numeric(df$estimate)
  df$flag <- as.factor(ifelse(is.na(df$flag), "None", df$flag))

  # Move 'year' and 'year_num' to the first two columns
  if (all(c("year", "year_num") %in% names(df))) {
    new_order <- c("year", "year_num", setdiff(names(df), c("year", "year_num")))
    df <- df[, new_order]
  }

  return(df)
}

#' Coerce column types for quarterly provisional CDC dataset
#'
#' This function prepares quarterly-structured CDC datasets (e.g., dataset ID
#' '489q-934x') by converting relevant fields to factors, dates, and numerics.
#' It parses `year_and_quarter` into separate `year` (as Date) and `quarter` (as
#' factor) columns, and reorders them to appear first.
#'
#' @param df A tibble with quarterly structure.
#' @return A tibble with coerced types and column reordering.
#' @keywords internal
clean_cdc_quarterly <- function(df) {
  df$year <- as.Date(paste0(substr(df$year_and_quarter, 1, 4), "-01-01"))
  df$quarter <- factor(substr(df$year_and_quarter, 6, 7),
                       levels = c("Q1", "Q2", "Q3", "Q4"))
  df$year_and_quarter <- NULL

  # Convert all columns starting with "rate_" EXCEPT "rate_type" to numeric
  rate_cols <- grep("^rate_", names(df), value = TRUE)
  rate_cols <- setdiff(rate_cols, "rate_type")

  for (col in rate_cols) {
    df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
  }

  df$cause_of_death <- as.factor(df$cause_of_death)
  df$rate_type <- as.factor(df$rate_type)
  df$unit <- as.factor(df$unit)

  if (all(c("year", "quarter") %in% names(df))) {
    new_order <- c("year", "quarter", setdiff(names(df), c("year", "quarter")))
    df <- df[, new_order]
  }

  return(df)
}

#' Detect the CDC dataset structure based on column names hash
#'
#' Given a tibble (typically returned from `bind_cdc_batches()`), this function
#' generates an MD5 hash of the column names and uses it to determine which data
#' cleaning function should be applied.
#'
#' @param df A tibble returned from CDC data batching.
#' @return A string indicating which cleaning function to apply, or `NULL` if no
#'   match is found.
#' @keywords internal
detect_cdc_structure <- function(df) {
  hash <- digest::digest(paste(names(df)), algo = "md5")

  switch(
    hash,
    "4c9cd083d29433984eb29d9451bada25" = "clean_cdc_finalized",
    "fcec76284a2e1e8701ceb4eabbf94827" = "clean_cdc_historical",
    "e0397317d335549d8244da964f07e598" = "clean_cdc_quarterly",
    NULL
  )
}


#' Retrieve and optionally clean CDC dataset records with schema harmonization
#'
#' Downloads all records from a CDC Socrata dataset using paginated API calls,
#' harmonizes the presence of the `flag` column across batches (if applicable),
#' and returns a single tibble. If the dataset structure is recognized, an
#' appropriate cleaning function is applied to coerce data types and re-order
#' columns.
#'
#' This function wraps the three-step process of: 1. Collecting paginated data
#' chunks 2. Ensuring consistent schema (with respect to the `flag` column) 3.
#' Binding all records into a single tibble
#'
#' If the dataset's column structure is not recognized, the raw tibble is
#' returned without coercion or reordering.
#'
#' @param series_id A Socrata dataset ID string (e.g., "489q-934x")
#'
#' @return A tibble containing all records from the dataset. If recognized, the
#'   tibble is cleaned and coerced to appropriate types; otherwise, it is
#'   returned unmodified.
#'
#' @examples
#' \dontrun{
#'   suicide_qtr <- get_cdc("489q-934x")
#'   suicide_final <- get_cdc("p7se-k3ix")
#' }
#'
#' @seealso \code{collect_cdc_batches()}, \code{harmonize_flag_column()},
#'   \code{bind_cdc_batches()}, \code{detect_cdc_structure()}
#' @export
get_cdc <- function(series_id) {
  tmp <- bind_cdc_batches(harmonize_flag_column(collect_cdc_batches(series_id)))
  cleaner <- detect_cdc_structure(tmp)
  if (!is.null(cleaner)) {
    tmp <- do.call(cleaner, list(tmp))
    return(tmp)
  }
}


#' Construct CDC Data
#'
#' Iterates over the CDC tab and retrieves all listed datasets using
#' \code{get_cdc()}. Each result is stored in a named list, with a description
#' attribute attached for later reference.
#'
#' @param sheet A named list from \code{get_full_gsheet()}, containing a "CDC"
#'   element. This tab must include at least two columns: "Series ID" and
#'   "Description".
#'
#' @return A named list of tibbles retrieved from CDC's Socrata endpoints. Each
#'   tibble will have a "description" attribute containing its metadata label.
#' @export
construct_cdc <- function(sheet, flush = FALSE) {
  if (!"CDC" %in% names(sheet)) {
    stop("Input must contain a 'CDC' tab with Series IDs.")
  }

  cdc_tab <- sheet$CDC
  series_ids <- cdc_tab[["Series ID"]]

  # Ensure cache directory exists
  cache_dir <- file.path(getwd(), "pipewelder_cache")
  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir)
  }

  results <- list()

  for (i in seq_along(series_ids)) {
    series_id <- series_ids[i]
    description <- cdc_tab$Description[i]

    # Generate MD5 hash for cache key
    cache_key_input <- paste(series_id, Sys.Date(), sep = "_")
    cache_key <- digest::digest(cache_key_input, algo = "md5")
    cache_file <- file.path(cache_dir, paste0("cdc_", cache_key, ".rds"))

    message(sprintf("Retrieving CDC dataset: %s", series_id))

    # Delete cache if flushing is requested
    if (flush && file.exists(cache_file)) {
      file.remove(cache_file)
      message("↪ Flushed existing cache.")
    }

    # Load from cache or fetch if needed
    result <- tryCatch({
      if (file.exists(cache_file)) {
        message("↪ Using cached series. Pass 'flush = TRUE' to overide")
        readRDS(cache_file)
      } else {
        data <- get_cdc(series_id)
        saveRDS(data, cache_file)
        data
      }
    }, error = function(e) {
      warning(sprintf(
        "Failed to retrieve CDC series '%s': %s",
        series_id,
        e$message
      ))
      NULL
    })

    if (!is.null(result)) {
      attr(result, "description") <- description
      results[[paste0("CDC_", series_id)]] <- result
    }
  }

  return(results)
}
