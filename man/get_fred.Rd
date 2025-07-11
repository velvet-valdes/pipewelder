% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pipeline_FRED.R
\name{get_fred}
\alias{get_fred}
\title{Retrieve and Tidy FRED Series Data}
\usage{
get_fred(series_id, start_date, end_date = Sys.Date())
}
\arguments{
\item{series_id}{A character string representing the FRED series ID (e.g., \code{"UNRATE"}).}

\item{start_date}{Start date of the time series in \code{"YYYY"}, \code{"YYYY-MM"}, \code{"YYYY-MM-DD"} format,
or a \code{Date} object.}

\item{end_date}{Optional. End date in the same format as \code{start_date}. Defaults to today's date.}
}
\value{
A tibble with \code{date} and \code{value} columns, one row per observation.
}
\description{
A convenience wrapper that retrieves a FRED time series using \code{\link[=get_fred_series_raw]{get_fred_series_raw()}}
and immediately formats it using \code{\link[=tidy_fred_series]{tidy_fred_series()}}. Returns a clean tibble of
observations between the specified dates.
}
\details{
Internally uses \code{\link[=get_fred_series_raw]{get_fred_series_raw()}} to fetch the raw JSON data and \code{\link[=tidy_fred_series]{tidy_fred_series()}} to format the result.
}
\examples{
\dontrun{
Sys.setenv(FRED_KEY = "your_fred_api_key")
get_fred("UNRATE", "2020-01-01")
}

}
\seealso{
\code{get_fred_series_raw()}, \code{tidy_fred_series()}
}
