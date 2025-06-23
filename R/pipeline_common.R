#' Retrieve All Tabs from a Public Google Sheet
#'
#' Downloads and returns all visible tabs (worksheets) from a publicly shared
#' Google Sheets URL. Returns a named list of tibbles, one per tab, using
#' [googlesheets4::read_sheet()] internally. Automatically handles
#' authentication deauthorization for public sheets, if requested.
#'
#' @param url Character. A publicly shared Google Sheets URL.
#' @param deauth Logical. Whether to disable Google authentication for accessing
#'   public sheets. Defaults to `TRUE`. Set to `FALSE` if you want to use your
#'   own credentials (e.g., for private sheets).
#'
#' @return A named list of tibbles. Each element corresponds to a tab in the
#'   Google Sheet, with the tab name used as the list element name. If a tab
#'   fails to load, a warning is issued and that element will be `NULL`.
#'
#' @examples
#' \dontrun{
#'   url <- "https://docs.google.com/spreadsheets/d/your_public_sheet_id_here"
#'   sheet_data <- get_full_gsheet(url)
#'   names(sheet_data)  # See tab names
#'   sheet_data$Metadata  # Access the "Metadata" tab as a tibble
#' }
#'
#' @seealso [googlesheets4::read_sheet()], [googlesheets4::gs4_deauth()]
#' @export
get_full_gsheet <- function(url, deauth = TRUE) {
  if (deauth) {
    googlesheets4::gs4_deauth()
  }

  tab_names <- googlesheets4::sheet_names(url)

  sheet_list <- lapply(tab_names, function(tab) {
    tryCatch({
      googlesheets4::read_sheet(url, sheet = tab)
    }, error = function(e) {
      warning(paste("Could not read sheet:", tab, "-", e$message))
      NULL
    })
  })

  names(sheet_list) <- tab_names

  return(sheet_list)
}
