% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pipeline_BLS.R
\name{construct_bls}
\alias{construct_bls}
\title{Construct BLS Data}
\usage{
construct_bls(
  sheet,
  start_year = 2000,
  end_year = as.integer(format(Sys.Date(), "\%Y")),
  flush = FALSE
)
}
\arguments{
\item{sheet}{A list of tibbles (typically returned by \code{get_full_gsheet()}),
with one or more elements named like \code{"BLS-XXX"}, each containing a \verb{Series ID} column and optionally a \code{Description} column.}

\item{start_year}{The starting year of the time series (default is 2000).}

\item{end_year}{The ending year of the time series (default is the current
year).}

\item{flush}{Logical. If \code{TRUE}, clears the existing cache for the relevant
BLS series before downloading new data.}
}
\value{
A named list of tibbles, one for each BLS series. Each tibble
includes an attribute \code{"description"} from the sheet if present.
}
\description{
Iterates over all tabs in a Google Sheet with names matching the pattern
\code{"BLS-"}, retrieves BLS series for each ID listed, and returns a named list
of tibbles. Results are cached by series ID and date range to avoid redundant
API calls.
}
\details{
Cached files are stored in a \verb{pipewelder_cache/} folder in the
working directory. File names are hashed using the series ID, start year, and
end year to uniquely identify each request.
}
