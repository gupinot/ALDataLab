\name{siteMap}
\alias{siteMap}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
TODO
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
}
\usage{
siteMap(FromSite = "IDM", Sector = "", VolumeUnit = "SumTraffic", SiteRemoved = c("MSO1", "MSO2"), SiteSelect = "", MaxMatrix = 50)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{FromSite}{
%%     ~~Describe \code{FromSite} here~~
}
  \item{Sector}{
%%     ~~Describe \code{Sector} here~~
}
  \item{VolumeUnit}{
%%     ~~Describe \code{VolumeUnit} here~~
}
  \item{SiteRemoved}{
%%     ~~Describe \code{SiteRemoved} here~~
}
  \item{SiteSelect}{
%%     ~~Describe \code{SiteSelect} here~~
}
  \item{MaxMatrix}{
%%     ~~Describe \code{MaxMatrix} here~~
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (FromSite = "IDM", Sector = "", VolumeUnit = "SumTraffic", 
    SiteRemoved = c("MSO1", "MSO2"), SiteSelect = "", MaxMatrix = 50) 
{
    if (!exists("StatByIDMSite")) 
        readNxStatsFiles()
    if (FromSite == "IDM") {
        Data <- StatByIDMSite
        ColForSiteFromResolution <- "IDM_SiteCode"
    }
    else {
        Data <- StatByDeviceSite
        ColForSiteFromResolution <- "REFIPRANGE_device_last_Site"
    }
    ColForVolume <- switch(VolumeUnit, SumConnect = "sum_con_cardinality", 
        SumTraffic = "sum_con_traffic", SumCountUniqueLogin = "sum_count_unique_login", 
        "ERR")
    output <- reformatStat(Stat = Data, ColForSiteFromResolution = ColForSiteFromResolution, 
        SectorFilter = Sector, ColForVolume = ColForVolume, SiteRemoved = SiteRemoved, 
        SiteSelect = SiteSelect, MaxMatrix = MaxMatrix, DEBUG = FALSE)
    CircosTable <- output[[1]]
    if (!is.null(CircosTable)) {
        unit <- output[2]
        ResBeforeSpread <- output[[3]]
        ResBeforeSpread[, `:=`(unit, paste("10^", unit, sep = ""))]
        Dir <- paste(ResultDir, "/circos/", FromSite, "-", Sector, 
            "-", VolumeUnit, "-", SiteSelect, sep = "")
        dir.create(Dir, showWarnings = FALSE, recursive = TRUE)
        circosfile <- paste(Dir, "/circos.csv", sep = "")
        write.table(CircosTable, file = circosfile, row.names = FALSE, 
            sep = "\t")
        statfile <- paste(Dir, "/stat.csv", sep = "")
        write.table(ResBeforeSpread, file = statfile, row.names = FALSE, 
            sep = ";")
        Title <- paste("\"Data lab innovation - Circos diagram - Mapping between sites\"", 
            sep = "")
        dat <- format(Sys.time(), "\%Y\%m\%d\%H\%M\%S")
        fileout <- paste(Dir, "/circos", FromSite, "-", Sector, 
            "-", VolumeUnit, "-", SiteSelect, ".png", sep = "")
        command <- paste(circosScript, " \"", circosfile, "\" ", 
            dat, " \"", fileout, "\" ", Title, sep = "")
        cat("command=", command)
        system(command, wait = TRUE)
        return(fileout)
    }
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
