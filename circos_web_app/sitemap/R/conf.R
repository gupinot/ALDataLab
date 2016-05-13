#SitemapSourceDir
SiteMapDir <- "/home/datalab/SiteMap"
MasterURL <- "//circos.datalab.gealstom.eu"
#SiteMapDir <- "/Users/guillaumepinot/Dev/Alstom/SiteMap"

HttpServerDir <- "data"

#Nexthink Data Input directory
NXDataDir <- paste(SiteMapDir, "/datainput", sep="")
NXDataDir3 <- paste(NXDataDir, "3-R-NXDataNoOverlap", sep="/")
NXDataDir4 <- paste(NXDataDir, "4-AppliData", sep="/")

#retro compatibility with readNxStatsFiles()
NXDataInputDir <- paste(NXDataDir, "/NXFiles", sep="")

NXStatCollectFile <- paste(NXDataDir3, "/Stats-collect.csv", sep="")  #File containing statistics of NX files collected and available
NXStatFileCsv <- paste(NXDataDir3, "/R-NXStat_connection.csv", sep="")
StatServer2ServerDetailFile <- paste(NXDataDir4, "/StatAppliDetailServer2Server.csv.gz", sep="")
StatServer2ServerDetailFile6Weeks <- paste(NXDataDir4, "/StatAppliDetailServer2Server6Weeks.csv.gz", sep="")


#Directory of repositories
RepositoryDir<-paste(SiteMapDir, "/Repository", sep="")

MDM_File <- paste(RepositoryDir, "/MDM_IP_Address-2015-03-29.csv", sep="")
IDMFile<-paste(RepositoryDir, "IDM-Extracts-2015-08-28-AccountRights.csv", sep="/")

#Output Directory
ResultDir <- paste(SiteMapDir, "/res/Work/SiteMap/circos/tmp", sep="")

circosResHttpServerLocalPath <- ResultDir
circosResHttpServerAddress<-paste("/", HttpServerDir, sep="")

#circos tool
circosScript <- paste(SiteMapDir, "/circos/circos.sh", sep="")

SitesSectorScenarioFile <- paste(NXDataDir, "Sites_sector_scenario.csv", sep="/")

SitesCodeFile <- paste(NXDataDir4, "SiteCode.csv.gz", sep="/")
SectorCodeFile <- paste(NXDataDir4, "SectorCode.csv.gz", sep="/")
ServerSectorCodeFile <- paste(NXDataDir4, "SectorCodeServer.csv.gz", sep="/")
DateRangeFile <- paste(NXDataDir4, "/DateRange.csv.gz", sep="")
DateRangeFile6Weeks <- paste(NXDataDir4, "/DateRange6Weeks.csv.gz", sep="")
DateRangeFileLastWeek <- paste(NXDataDir4, "/DateRangeLastWeek.csv.gz", sep="")
DateRangeFileDayOne <- paste(NXDataDir4, "/DateRangeDayOne.csv.gz", sep="")
DateRangeServerFile <- paste(NXDataDir4, "/DateRangeServer.csv.gz", sep="")
DateRangeServerFile6Weeks <- paste(NXDataDir4, "/DateRangeServer6Weeks.csv.gz", sep="")
DateRangeServerFileLastWeek <- paste(NXDataDir4, "/DateRangeServerLastWeek.csv.gz", sep="")
DateRangeServerFileDayOne <- paste(NXDataDir4, "/DateRangeServerDayOne.csv.gz", sep="")

SitesCodeFile <- paste(circosResHttpServerLocalPath, "SiteCode.csv.gz", sep="/")

SitesCodeFile <- paste(NXDataDir4, "SiteCode.csv.gz", sep="/")
SectorCodeFile <- paste(NXDataDir4, "SectorCode.csv.gz", sep="/")
