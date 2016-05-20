#Site map functions

require(data.table)
require(bit64)
require(tidyr)
require(utils)

source("./R/conf.R")

################################################################################################
#########################                 Functions             ################################
################################################################################################

################################################################################################
readNxStatsAppliFiles <- function(deviceOrServer = "Device", FromSite = "IDM", withDate = FALSE, ServerDetail = FALSE, DateRange = "") {
  
  if (deviceOrServer == "Device") {
    if (FromSite == "IDM") {
      Method="IDM"
    }
    else {
      Method="Device"
    }
    if (ServerDetail)
      Type="Server"
    else
      Type=""

    if (withDate)
      Day="Day"
    else
      Day=""

    filename=paste("Stat", Method, "AppliSite", Type, Day, DateRange, ".csv.gz", sep="")
  }
  else
  {
    filename=paste("StatAppliDetailServer2Server", DateRange, ".csv.gz", sep="")

  }


  print(paste("readNxStatsAppliFiles() : read file : ", filename, sep=""))
  filein=paste(NXDataDir4, filename, sep="/")
  Stat<-fread(paste("gunzip -c ", filein, sep=""), sep=";")
  
  #verrue : suppress special character in NX_bin* : '"', '\'
  Stat[, source_app_name:=gsub("\"","", source_app_name)]
  Stat[, source_app_name:=gsub("\\", "", source_app_name, fixed=TRUE)]
  return(Stat)
}



################################################################################################
################################################################################################
Read_SitesSectorScenario <- function(File = SitesSectorScenarioFile) {
  if (file.exists(File)) {
    SitesSectorScenarioData <- fread(File, sep=";")[, c(1, 4), with=FALSE]
    setnames(SitesSectorScenarioData, c("SiteCode", "SiteScenario"))
    SitesSectorScenarioData <- SitesSectorScenarioData[is.na(SiteScenario), SiteScenario:="NA"]
    return(SitesSectorScenarioData)
  }
  else return(NULL)
}


################################################################################################
################################################################################################
StatFiltering <- function(Data, deviceOrServer = "Device",
                          Sector = c(""), SectorNA = TRUE, SectorTo = c(""), SectorNATo = TRUE,
                          InterIntraSite = c(TRUE, FALSE),
                          SiteSelectIn = c(""), SiteSelectOut = c(""), SiteSelectOperand="OR",
                          CountrySiteFilterIn = c(""), CountrySiteFilterOut = c(""), CountrySelectOperand = "AND",
                          CountryExcludeSelectIn = c(""), CountryExcludeSelectOut = c(""), CountryExcludeSelectOperand = "AND",
                          RemovedSiteSelectIn = c(""), RemovedSiteSelectOut = c(""), RemovedSiteSelectOperand="AND",
                          SitesSectorScenarioFilterIn = c(""), SitesSectorScenarioFilterOut = c(""), SiteCategorySelectOperand = "AND",
                          AppliFilteringDeviceType = "none", AppFilterDeviceFileType, AppFilterDeviceFile, AppFilterDeviceTypeSelectLogicalOperand = "OR",
                          AppliFilteringServerType = "none", AppFilterServerFileType, AppFilterServerFile, AppFilterServerTypeSelectLogicalOperand = "OR",
                          AppFilterSourceDestOperand = "AND",
                          ForceAppFilter=FALSE, LstAppNameToFilter=c("Age of Empires", "Amazon Music", "Torrent", "Dropbox", "Super Mario", "age of empires"), 
                          LstAppExecToFiler = c("torrent"),
                          portsfilterIncludeOrExclude = "include", portsfiltering = "") {
  
  #Application Filtering
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  #Data <- AppliFilter(Data, deviceOrServer = deviceOrServer, SourceOrDestination="Source", FilteringType=AppliFilteringDeviceType, AppFilterFileName=AppFilterDeviceFile,
  #                    ForceAppFilter = ForceAppFilter, LstAppNameToFilter=LstAppNameToFilter, LstAppExecToFiler=LstAppExecToFiler)
  Data <- AppliFilter(Data, deviceOrServer = deviceOrServer, AppliFilteringDeviceType = AppliFilteringDeviceType, AppFilterDeviceFileType = AppFilterDeviceFileType, AppFilterDeviceFile=AppFilterDeviceFile, AppFilterDeviceTypeSelectLogicalOperand=AppFilterDeviceTypeSelectLogicalOperand,
          AppliFilteringServerType = AppliFilteringServerType, AppFilterServerFileType=AppFilterServerFileType, AppFilterServerFile=AppFilterServerFile, AppFilterServerTypeSelectLogicalOperand=AppFilterServerTypeSelectLogicalOperand,
          AppFilterSourceDestOperand=AppFilterSourceDestOperand)
  
  print("   StatFiltering() : prepare filtering...")
  if (is.null(Sector) | Sector[1] == "" | Sector[1] == "All" | is.null(Sector[1])) {
    Sector=c("")
  }
  if (is.null(SectorTo) | SectorTo[1] == "" | SectorTo[1] == "All" | is.null(SectorTo[1])) {
    SectorTo=c("")
  }

  if (is.null(RemovedSiteSelectIn) | RemovedSiteSelectIn[1] == "" | RemovedSiteSelectIn[1] == "All" | is.null(RemovedSiteSelectIn[1])) {
    RemovedSiteSelectIn=c("")
  }
  if (is.null(RemovedSiteSelectOut) | RemovedSiteSelectOut[1] == "" | RemovedSiteSelectOut[1] == "All" | is.null(RemovedSiteSelectOut[1])) {
    RemovedSiteSelectOut=c("")
  }
  if (is.null(CountrySiteFilterIn) | CountrySiteFilterIn[1] == "" | CountrySiteFilterIn[1] == "All" | is.null(CountrySiteFilterIn[1])) {
    CountrySiteFilterIn=c("")
  }
  if (is.null(CountrySiteFilterOut) | CountrySiteFilterOut[1] == "" | CountrySiteFilterOut[1] == "All" | is.null(CountrySiteFilterOut[1])) {
    CountrySiteFilterOut=c("")
  }
  if (is.null(CountryExcludeSelectIn) | CountryExcludeSelectIn[1] == "" | CountryExcludeSelectIn[1] == "All" | is.null(CountryExcludeSelectIn[1])) {
    CountryExcludeSelectIn=c("")
  }
  if (is.null(CountryExcludeSelectOut) | CountryExcludeSelectOut[1] == "" | CountryExcludeSelectOut[1] == "All" | is.null(CountryExcludeSelectOut[1])) {
    CountryExcludeSelectOut=c("")
  }
  
  if (is.null(SiteSelectIn) | SiteSelectIn[1] == "" | SiteSelectIn[1] == "All"  | is.null(SiteSelectIn[1])) {
    SiteSelectIn=c("")
  }
  if (is.null(SiteSelectOut) | SiteSelectOut[1] == "" | SiteSelectOut[1] == "All"  | is.null(SiteSelectOut[1])) {
    SiteSelectOut=c("")
  }
  
  print("   StatFiltering() : Correct NA Sites...")
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  #verrue : Correct NA SiteCode_Source
  Data[is.na(SiteCode_Source), SiteCode_Source:="nf"]
  

  
  Stat <- Data
  
  print("   StatFiltering() : keep only connection status closed...")
  setkey(Stat, con_status)
  Stat<-Stat["closed",]

  print("    StatFiltering() : Sector filtering...")
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  if (!SectorNA) {
    print(paste("StatFiltering() : SectorNA not true. Value : ", SectorNA, sep=""))
    if (deviceOrServer == "Device") {
      setkey(Stat, source_sector)
      Stat <- Stat[!is.na(source_sector), ]
    }
    else {
      setkey(Stat, source_aip_app_sector)
      Stat <- Stat[!is.na(source_aip_app_sector), ]
    }
  }


  tmpReformatStat <- copy(Stat)

  if (Sector[1] != "") {
    print(paste("StatFiltering() : Sector not empty", sep=""))

    if (deviceOrServer == "Device") {
      setkey(tmpReformatStat, source_sector)
      tmpReformatStat <- tmpReformatStat[Sector, nomatch=0]
    } else {
      SectorPattern <- paste(Sector, collapse="|")
      SectorPattern <- paste("(", SectorPattern, ")", sep="")
      tmpReformatStat <- tmpReformatStat[grepl(SectorPattern, source_aip_app_sector), ]
    }
  }

  if (!SectorNATo) {
  print("StatFiltering() : SectorNATo not true")
    setkey(tmpReformatStat, dest_aip_app_sector)
    tmpReformatStat <- tmpReformatStat[!is.na(dest_aip_app_sector), ]
  }


  if (SectorTo[1] != "") {
    SectorPattern <- paste(SectorTo, collapse="|")
    SectorPattern <- paste("(", SectorPattern, ")", sep="")
    tmpReformatStat <- tmpReformatStat[grepl(SectorPattern, dest_aip_app_sector), ]
  }


print("    StatFiltering() : Site filtering...")
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  if (SiteSelectIn[1] != "" & SiteSelectOut[1] != "") {
    if (SiteSelectOperand == "AND") {
      setkey(tmpReformatStat, SiteCode_Source)
      tmpReformatStat <- tmpReformatStat[SiteSelectIn, nomatch=0]
      setkey(tmpReformatStat, SiteCode_Destination)
      tmpReformatStat <- tmpReformatStat[SiteSelectOut, nomatch=0]
    }
    else {
      tmpReformatStat <- tmpReformatStat[SiteCode_Source %in% SiteSelectIn | SiteCode_Destination %in% SiteSelectOut, ]
    }
  }
  else {
    if (SiteSelectIn[1] != "") {
      setkey(tmpReformatStat, SiteCode_Source)
      tmpReformatStat <- tmpReformatStat[SiteSelectIn, nomatch=0]
    }
    else {
      if (SiteSelectOut[1] != "") {
        setkey(tmpReformatStat, SiteCode_Destination)
        tmpReformatStat <- tmpReformatStat[SiteSelectOut, nomatch=0]
      }
    }
  }
  
  print("    StatFiltering() : Exclude site filtering...")
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  if (RemovedSiteSelectIn[1] != "" & RemovedSiteSelectOut[1] != "") {
    if (RemovedSiteSelectOperand == "AND") {
      setkey(tmpReformatStat, SiteCode_Source)
      tmpReformatStat <- tmpReformatStat[!RemovedSiteSelectIn]
      setkey(tmpReformatStat, SiteCode_Destination)
      tmpReformatStat <- tmpReformatStat[!RemovedSiteSelectOut]
    }
    else {
      tmpReformatStat <- tmpReformatStat[!(SiteCode_Source %in% RemovedSiteSelectIn | SiteCode_Destination %in% RemovedSiteSelectOut), ]
    }
  }
  else {
    if (RemovedSiteSelectIn[1] != "") {
      setkey(tmpReformatStat, SiteCode_Source)
      tmpReformatStat <- tmpReformatStat[!RemovedSiteSelectIn]
    }
    else {
      if (RemovedSiteSelectOut[1] != "") {
        setkey(tmpReformatStat, SiteCode_Destination)
        tmpReformatStat <- tmpReformatStat[!RemovedSiteSelectOut]
      }
    }
  }
  
  ###########################
  #Add CountrySiteCode column 
  print("    StatFiltering() : Add CountrySiteCode column...")
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  RepoSite <- LstSite()
  
  setkey(RepoSite, SiteCode)
  setkey(tmpReformatStat, SiteCode_Source)
  tmpReformatStat <- RepoSite[tmpReformatStat, nomatch=NA]
  setnames(tmpReformatStat, 
           c("SiteCode", "CountryCode", "SiteName"), 
           c("SiteCode_Source", "CountryCode_Source", "SiteName_Source"))
  
  setkey(tmpReformatStat, SiteCode_Destination)
  setkey(RepoSite, SiteCode)
  tmpReformatStat <- RepoSite[tmpReformatStat, nomatch=NA]
  setnames(tmpReformatStat, 
           c("SiteCode", "CountryCode", "SiteName"), 
           c("SiteCode_Destination", "CountryCode_Destination", "SiteName_Destination"))
  ###########################
  
  print("    StatFiltering() : Country filtering...")
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  if (CountrySiteFilterIn[1] != "" & CountrySiteFilterOut[1] != "") {
    if (CountrySelectOperand == "AND") {
      setkey(tmpReformatStat, CountryCode_Source)
      tmpReformatStat <- tmpReformatStat[CountrySiteFilterIn, nomatch=0]
      setkey(tmpReformatStat, CountryCode_Destination)
      tmpReformatStat <- tmpReformatStat[CountrySiteFilterOut, nomatch=0]
    }
    else {
      tmpReformatStat <- tmpReformatStat[CountryCode_Source %in% CountrySiteFilterIn | CountryCode_Destination %in% CountrySiteFilterOut, ]
    }
  }
  else {
    if (CountrySiteFilterIn[1] != "") {
      setkey(tmpReformatStat, CountryCode_Source)
      tmpReformatStat <- tmpReformatStat[CountrySiteFilterIn, nomatch=0]
    }
    else {
      if (CountrySiteFilterOut[1] != "") {
        setkey(tmpReformatStat, CountryCode_Destination)
        tmpReformatStat <- tmpReformatStat[CountrySiteFilterOut, nomatch=0]
      }
    }
  }
  
  print("    StatFiltering() : Exclude site filtering...")
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  if (CountryExcludeSelectIn[1] != "" & CountryExcludeSelectOut[1] != "") {
    if (CountryExcludeSelectOperand == "AND") {
      setkey(tmpReformatStat, CountryCode_Source)
      tmpReformatStat <- tmpReformatStat[!CountryExcludeSelectIn]
      setkey(tmpReformatStat, CountryCode_Destination)
      tmpReformatStat <- tmpReformatStat[!CountryExcludeSelectOut]
    }
    else {
      tmpReformatStat <- tmpReformatStat[!(CountryCode_Source %in% CountryExcludeSelectIn | CountryCode_Destination %in% CountryExcludeSelectOut), ]
    }
  }
  else {
    if (CountryExcludeSelectIn[1] != "") {
      setkey(tmpReformatStat, CountryCode_Source)
      tmpReformatStat <- tmpReformatStat[!CountryExcludeSelectIn]
    }
    else {
      if (CountryExcludeSelectOut[1] != "") {
        setkey(tmpReformatStat, CountryCode_Destination)
        tmpReformatStat <- tmpReformatStat[!CountryExcludeSelectOut]
      }
    }
  }
  
  
  print("    StatFiltering() : InterIntra filtering...")
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  if (!InterIntraSite[1] | !InterIntraSite[2]) {
    if (InterIntraSite[1]) {
      #Exclude intra site traffic
      tmpReformatStat <- tmpReformatStat[SiteCode_Source != SiteCode_Destination]
    } else {
      #Keep only intra site traffic
      tmpReformatStat <- tmpReformatStat[SiteCode_Source == SiteCode_Destination]
    }
  }
  
  print("    StatFiltering() : Scenario filtering...")
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  if (SitesSectorScenarioFilterIn[1] != "" & SitesSectorScenarioFilterOut[1] != "") {
    SitesSectorScenarioList <- Read_SitesSectorScenario()[, list(SiteCode, SiteScenario)]
    setkey(SitesSectorScenarioList, SiteScenario)
    SitesFilterListIn <- SitesSectorScenarioList[SitesSectorScenarioFilterIn, SiteCode, nomatch=0]
    SitesFilterListOut <- SitesSectorScenarioList[SitesSectorScenarioFilterOut, SiteCode, nomatch=0]
    if (SiteCategorySelectOperand == "AND") {
        tmpReformatStat <- tmpReformatStat[SiteCode_Source %in% SitesFilterListIn & SiteCode_Destination %in% SitesFilterListOut, ]
    }
    else {
      tmpReformatStat <- tmpReformatStat[SiteCode_Source %in% SitesFilterListIn | SiteCode_Destination %in% SitesFilterListOut, ]
    }
  }
  else {
    if (SitesSectorScenarioFilterIn[1] != "") {
      SitesSectorScenarioList <- Read_SitesSectorScenario()[, list(SiteCode, SiteScenario)]
      setkey(SitesSectorScenarioList, SiteScenario)
      SitesFilterListIn <- SitesSectorScenarioList[SitesSectorScenarioFilterIn, SiteCode, nomatch=0]
      setkey(tmpReformatStat, SiteCode_Source)
      tmpReformatStat <- tmpReformatStat[SitesFilterListIn, nomatch=0]
    }
    else {
      if (SitesSectorScenarioFilterOut[1] != "") {
        SitesSectorScenarioList <- Read_SitesSectorScenario()[, list(SiteCode, SiteScenario)]
        setkey(SitesSectorScenarioList, SiteScenario)
        SitesFilterListOut <- SitesSectorScenarioList[SitesSectorScenarioFilterOut, SiteCode, nomatch=0]
        setkey(tmpReformatStat, SiteCode_Destination)
        tmpReformatStat <- tmpReformatStat[SitesFilterListOut, nomatch=0]
      }
    }
  }
  
  if (portsfiltering != "") {
    print("   StatFiltering() : port filtering...")
    ProtPort <- unlist(strsplit(portsfiltering, split="\\|"))
    
    #Port without range
    ProtPortWithoutRange <- ProtPort[grepl("^(tcp|udp)\\/[0-9]+$", ProtPort)]
    portfilteringDT <- data.table(con_protocol=gsub("\\/.*", "", ProtPortWithoutRange), dest_port=as.integer(gsub(".*\\/", "", ProtPortWithoutRange)))

    #Port with range
    ProtPortWithRange <- ProtPort[grepl("^(tcp|udp)\\/[0-9]+-[0-9]+$", ProtPort)]
    tempDT<-NULL
    for (d in ProtPortWithRange) {
      prot<-gsub("\\/.*", "", d)
      port1<-as.integer(gsub("-.*$", "", gsub(".*\\/", "", d)))
      port2<-as.integer(gsub("^.*-", "", gsub(".*\\/", "", d)))
      port<-c(port1:port2)
      if (is.null(tempDT)) {
        tempDT<-data.table(con_protocol=prot, dest_port=port)
      }
      else {
        tempDT<-rbindlist(list(tempDT, data.table(con_protocol=prot, dest_port=port)), use.names=TRUE)
      }
    }
    if (!is.null(tempDT)) {
      portfilteringDT <- rbindlist(list(portfilteringDT, tempDT), use.names = TRUE)
    }
    
    setkey(tmpReformatStat, con_protocol, dest_port)
    setkey(portfilteringDT, con_protocol, dest_port)
    portfilteringDT <- unique(portfilteringDT)
    if (portsfilterIncludeOrExclude == "include") {
      print("   StatFiltering() : port filtering : inner join")
      #inner join
      tmpReformatStat <- portfilteringDT[tmpReformatStat, nomatch=0]
    }
    else {
      #left outer join with exclusion
      tmpReformatStat <- tmpReformatStat[!portfilteringDT]
    }
  }
  
  print(paste("StatFiltering() : ", Sys.time(), sep=";"))
  return(tmpReformatStat)
}


################################################################################################
################################################################################################
#Function to generate the matrix in circos format.
reformatStat <- function(Stat, deviceOrServer = "Device",
                         MaxMatrix = 50, 
                         unit = 0,
                         DEBUG = FALSE, 
                         DetailledFlow = FALSE) {
  
  
  print("reformatStat() : begin")
  
  ColForVolume <- "Traffic"
    ColForSiteFromResolution="SiteCode_Source"
  
  tmpReformatStat <- Stat
  
  #Add Site category
  SitesSectorScenarioList <- Read_SitesSectorScenario()[, list(SiteCode, SiteScenario)]
  setkey(SitesSectorScenarioList, SiteCode)
  setkey(tmpReformatStat, SiteCode_Source)
  tmpReformatStat <- SitesSectorScenarioList[tmpReformatStat, nomatch=NA]
  setnames(tmpReformatStat, c("SiteCode", "SiteScenario"), c("SiteCode_Source", "SiteCategory_Source"))
  setkey(tmpReformatStat, SiteCode_Destination)
  tmpReformatStat <- SitesSectorScenarioList[tmpReformatStat, nomatch=NA]
  setnames(tmpReformatStat, c("SiteCode", "SiteScenario"), c("SiteCode_Destination", "SiteCategory_Destination"))
  
  print("    reformatStat() : Add country to site code...")
  #Add country to site code
  tmpReformatStat[, SiteCode_Source := paste(SiteCode_Source, "-", CountryCode_Source, sep="")]
  tmpReformatStat[, SiteCode_Destination := paste(SiteCode_Destination, "-", CountryCode_Destination, sep="")]
  
  #suppress rows with null traffic
  print("    reformatStat() : Agregate...")
  tmpReformatStat <- tmpReformatStat[Traffic!=0,]

  if (deviceOrServer == "Device" ) {
    if (!DetailledFlow) {
      tmpReformatStatAppli <- tmpReformatStat[, list(Traffic=round(sum(as.numeric(Traffic)), digits=0)),
                                              by=list(SiteCode_Source, SiteName_Source, SiteCategory_Source, source_sector, source_teranga,
                                                      SiteCode_Destination, SiteName_Destination, SiteCategory_Destination,
                                                      source_app_name, source_app_exec, url,
                                                      dest_aip_app_name, dest_aip_server_function, dest_aip_server_subfunction, dest_aip_app_criticality, dest_aip_app_type,
                                                      dest_aip_app_sector, dest_aip_app_shared_unique_id,
                                                      dest_aip_appinstance_type, dest_aip_server_adminby)]
    }
    else {
      tmpReformatStatAppli <- tmpReformatStat[, list(Traffic=round(sum(as.numeric(Traffic)), digits=0),
                                                     I_ID=paste(unique(unlist(strsplit(I_ID_U, ","))), collapse=",")),
                                              by=list(SiteCode_Source, SiteName_Source, SiteCategory_Source,source_sector, source_teranga,
                                                      SiteCode_Destination, SiteName_Destination, SiteCategory_Destination,
                                                      source_app_name, source_app_exec, url,
                                                      dest_ip, dest_aip_server_hostname, dest_port, con_protocol,
                                                      dest_aip_app_name, dest_aip_server_function, dest_aip_server_subfunction, dest_aip_app_criticality, dest_aip_app_type,
                                                      dest_aip_app_sector, dest_aip_app_shared_unique_id,
                                                      dest_aip_appinstance_type, dest_aip_server_adminby)]
    }
  } else {
    tmpReformatStatAppli <- tmpReformatStat[, list(Traffic=round(sum(as.numeric(Traffic)), digits=0)),
                                    by=list(
                                      source_ip, source_aip_server_hostname, source_app_name, source_app_exec, url,
                                      source_aip_app_name, source_aip_server_function, source_aip_server_subfunction, source_aip_app_criticality, source_aip_app_type,
                                      source_aip_app_sector, source_aip_app_shared_unique_id,
                                      source_aip_appinstance_type, source_aip_server_adminby,
                                      dest_ip, dest_aip_server_hostname, dest_port, con_protocol,
                                      dest_aip_app_name, dest_aip_server_function, dest_aip_server_subfunction, dest_aip_app_criticality, dest_aip_app_type,
                                      dest_aip_app_sector, dest_aip_app_shared_unique_id,
                                      dest_aip_appinstance_type, dest_aip_server_adminby,
                                      SiteCode_Source, SiteName_Source, SiteCategory_Source,
                                      SiteCode_Destination, SiteName_Destination, SiteCategory_Destination)]

  }
  tmpReformatStat <- tmpReformatStat[, list(Traffic=round(sum(as.numeric(Traffic)), digits=0)), 
                                     by=list(SiteCode_Source, SiteCode_Destination)]

  if (DEBUG == TRUE) {
    cat("reformatStat :\n\tdim(tmpReformatStat) : ", dim(tmpReformatStat), "\n") 
  }
  
  if (dim(tmpReformatStat)[1] == 0) return(list(NULL, 0, NULL))
  
  if (DEBUG == TRUE) {
    cat("reformatStat :\n\tdim(tmpReformatStat) : ", dim(tmpReformatStat), "\n")
  }
  
  #convert ColForVolume in units in such a way that it's not greater than 1 million
  print("    reformatStat() : convert ColForVolume in units in such a way that it's not greater than 1 million...")
  exprVolume <- parse(text = paste0(ColForVolume, ":=round(", ColForVolume, "/1000, digits=0)"))
  while (max(tmpReformatStat[, ColForVolume, with=FALSE]) >= 1000000) {
    tmpReformatStat[, eval(exprVolume)]
    unit=unit+3
    if (DEBUG == TRUE) {
      #cat("reformatStat : unit = 10^", unit, "\n")
    }
  }
  
  #Reduce nomber of rows+Columns to MaxMatrix (150)
  print("    reformatStat() : Reduce nomber of rows+Columns to MaxMatrix...")
  #Calculate ranked Sites and keep only top MaxMtrix
  exprby <- parse(text = paste0("list(", ColForSiteFromResolution, ")"))
  exprsum <- parse(text = paste0(ColForVolume, "=sum(", ColForVolume, ")"))
  FromSum <- tmpReformatStat[, eval(exprsum), by=eval(exprby)]
  ToSum <- tmpReformatStat[, eval(exprsum), by=SiteCode_Destination]
  setnames(FromSum, c("Site", "Sum"))
  setnames(ToSum, c("Site", "Sum"))
  tmp <- rbindlist(list(FromSum, ToSum), use.names=TRUE)
  tmp <- tmp[, list(Sum=sum(Sum)), by=Site]
  setorderv(tmp, c("Sum"), order=-1L)
  SiteToKeep <- tmp[1:MaxMatrix, ]$Site
  
  #Save 
  ReformatStatBeforeSpread <- copy(tmpReformatStatAppli)
  tmpReformatStat<-tmpReformatStat[, list(SiteCode_Source, SiteCode_Destination, Traffic)]
  
  #Keep top 
  print("    reformatStat() : Spread...")
  tmpReformatStat <- spread_(tmpReformatStat, "SiteCode_Destination", "Traffic", fill=0)
  
  if (DEBUG == TRUE) {
    cat("reformatStat :\n\tdim(tmpReformatStat) : ", dim(tmpReformatStat), "\n")
  }
  
  #Reduce nomber of rows+Columns to MaxMatrix (150)
  print("    reformatStat() : Reduce nomber of rows+Columns to MaxMatrix...")
  #Calculate ranked Sites  
  i <- MaxMatrix
  while(dim(tmpReformatStat)[1] + dim(tmpReformatStat)[2] > MaxMatrix)
  {
    setkeyv(tmpReformatStat, ColForSiteFromResolution)
    SiteToKeep <- tmp[1:i, ]$Site
    exprlstcol <- parse(text = paste("grepl(\"", 
                                     ColForSiteFromResolution, 
                                     "|", 
                                     paste(SiteToKeep, collapse="|"), 
                                     "\", colnames(tmpReformatStat))", 
                                     sep=""))
    #cat("exprlstcol=", as.character(exprlstcol), "\n")
    tmpReformatStat <- tmpReformatStat[SiteToKeep, eval(exprlstcol), with=FALSE, nomatch=0]
    i <- i - 1
  }
  if (DEBUG == TRUE) {
    cat("reformatStat :\n\tdim(tmpReformatStat) : ", dim(tmpReformatStat), "\n")
  }
  print("reformatStat() : End...")
  return(list(tmpReformatStat, unit, ReformatStatBeforeSpread))
}


################################################################################################
################################################################################################
LstCountry <- function() {
  Repo <- LstSite()[, CountryCode]
  return(sort(unique(Repo)))
}


################################################################################################
################################################################################################
LstSite <- function(file = SitesCodeFile) {
  #Based on following repositories : MDM, IDM
  if (file.exists(file))
  {
    return(fread(paste("gunzip -c ", file, sep=""), sep=";"))
  }
  else
  {
    #Error
    return(NULL)
  }
  
}

############################################################################
LstSector <- function(file = SectorCodeFile) {
  if (file.exists(file))
  {
    return(fread(paste("gunzip -c ", file, sep=""), sep="\n")$Sector)
  }
  else
  {
    #Error
    return(NULL)
  }
  
}

############################################################################
LstSectorServer <- function(file = ServerSectorCodeFile) {
  if (file.exists(file))
{
  return(fread(paste("gunzip -c ", file, sep=""), sep="\n")$Sector)
}
else
{
  #Error
return(NULL)
}

}

############################################################################
LstFromSite <- function() {
  return(c("IDM", "Last Device IP"))
}

############################################################################
LstVolumeUnit <- function() {
  
  return(data.table(Id = c("SumConnect", "SumData", "ConnectTimesData", "CountSumUniqueLogin"), 
                    Label = c("Number of tcp/udp connections", "tcp/udp traffic data size", "Number of connections times traffic size", "Approximate count of unique login")
  ))
}

############################################################################
LstSiteCategory <- function(File = SitesSectorScenarioFile) {
  SiteCategory <- Read_SitesSectorScenario(File)[, SiteScenario]
  SiteCategory <- unique(SiteCategory)
  return(SiteCategory)
}

############################################################################
LstDateRange <- function(DeviceOrServer = "Device", DateRange = "") {
  switch(paste(DeviceOrServer, DateRange, sep=""),
         Device = {
           File = DateRangeFile
         },
         Device6Weeks = {
           File = DateRangeFile6Weeks
         },
         DeviceLastWeek = {
           File = DateRangeFileLastWeek
         },
         DeviceDayOne = {
           File = DateRangeFileDayOne
         },
         Server = {
           File = DateRangeServerFile
         },
         Server6Weeks = {
           File = DateRangeServerFile6Weeks
         },
         ServerLastWeek = {
           File = DateRangeServerFileLastWeek
         },
         ServerDayOne = {
           File = DateRangeServerFileDayOne
         },
         File <- NULL
         )

  if (!file.exists(File)) {
    print(paste("Erreur, following file not found : ", File, sep=""))
    return(FALSE)
  }
  DateRangeData <- fread(paste("gunzip -c ", File, sep=""), sep=";")
  return(c(DateRangeData[1, c(1), with=FALSE], DateRangeData[1, c(2), with=FALSE]))
}

############################################################################
LstPannelSetting <- function() {
  return(list(LstDateRange(), 
              LstFromSite(), 
              LstVolumeUnit(), 
              LstSector(), 
              LstCountry(), 
              LstSite(), 
              LstSiteCategory(), 
              LstDateRange(DateRange="6Weeks"), 
              LstDateRange(DateRange="LastWeek"),
              LstDateRange(DateRange="DayOne"),
              LstDateRange(DeviceOrServer="Server"),
              LstDateRange(DeviceOrServer="Server", DateRange="6Weeks"),
              LstDateRange(DeviceOrServer="Server", DateRange="LastWeek"),
              LstDateRange(DeviceOrServer="Server", DateRange="DayOne"),
              LstSectorServer()
              ))
}

############################################################################
############################################################################
siteMap <- function(deviceOrServer = "Device", FromSite = "IDM",
                    SourceCollectSelect = "Windows",
                    Sector = c(""), SectorNA = TRUE, SectorTo = c(""), SectorNATo = TRUE,
                    VolumeUnit = "ConnectTimesData", MaxMatrix = 50,
                    InterIntraSite = c(TRUE, FALSE),
                    SiteSelectIn = c(""), SiteSelectOut = c(""), SiteSelectOperand="OR",
                    CountrySiteFilterIn = c(""), CountrySiteFilterOut = c(""), CountrySelectOperand = "AND",
                    CountryExcludeSelectIn = c(""), CountryExcludeSelectOut = c(""), CountryExcludeSelectOperand = "AND",
                    RemovedSiteSelectIn = c(""), RemovedSiteSelectOut = c(""), RemovedSiteSelectOperand="AND",
                    SitesSectorScenarioFilterIn = c(""), SitesSectorScenarioFilterOut = c(""), SiteCategorySelectOperand = "AND",
                    Clustering = FALSE, ClusteringAlgo = "mcl", DirectedGraphClustering = FALSE, mclClusterParam = c(1.5, 4), kmeanClusterParam = 100,
                    AppliFilteringDeviceType = "none", AppFilterDeviceFileType, AppFilterDeviceFile, AppFilterDeviceTypeSelectLogicalOperand= "OR",
                    AppliFilteringServerType = "none", AppFilterServerFileType, AppFilterServerFile, AppFilterServerTypeSelectLogicalOperand = "OR",
                    AppFilterSourceDestOperand = "AND",
                    DetailledFlow = FALSE, Server2Server = FALSE, portsfilterIncludeOrExclude = "include", portsfiltering = "",
                    DateRange = "") 
{
  
  print("siteMap() : Begin")
  print(paste("siteMap() : ", Sys.time(), sep=";"))
  print("   siteMap() : read data...")
  
  Data <-  readNxStatsAppliFiles(deviceOrServer, FromSite, ServerDetail = DetailledFlow, DateRange = DateRange)
  
  print(paste("siteMap() : ", Sys.time(), sep=";"))
  
  if (deviceOrServer == "Device" && FromSite == "IDM") {
    setnames(Data, "source_I_ID_site", "SiteCode_Source")
  }
  else {
    setnames(Data, "source_site", "SiteCode_Source")
  }
  setnames(Data, "dest_site", "SiteCode_Destination")
  
  if (!DetailledFlow) {
    portsfiltering=""
  }
  
  #Filter Data
  Data <- StatFiltering(Data, deviceOrServer = deviceOrServer,
                        Sector = Sector, SectorNA = SectorNA, SectorTo = SectorTo, SectorNATo = SectorNATo, InterIntraSite = InterIntraSite,
                        SiteSelectIn = SiteSelectIn, SiteSelectOut = SiteSelectOut, SiteSelectOperand=SiteSelectOperand,
                        CountrySiteFilterIn = CountrySiteFilterIn, CountrySiteFilterOut = CountrySiteFilterOut, CountrySelectOperand = CountrySelectOperand,
                        CountryExcludeSelectIn = CountryExcludeSelectIn, CountryExcludeSelectOut = CountryExcludeSelectOut, CountryExcludeSelectOperand = CountryExcludeSelectOperand,
                        RemovedSiteSelectIn = RemovedSiteSelectIn, RemovedSiteSelectOut = RemovedSiteSelectOut, RemovedSiteSelectOperand=RemovedSiteSelectOperand,
                        SitesSectorScenarioFilterIn = SitesSectorScenarioFilterIn, SitesSectorScenarioFilterOut = SitesSectorScenarioFilterOut, SiteCategorySelectOperand = SiteCategorySelectOperand,
                        AppliFilteringDeviceType = AppliFilteringDeviceType, AppFilterDeviceFileType=AppFilterDeviceFileType, AppFilterDeviceFile=AppFilterDeviceFile, AppFilterDeviceTypeSelectLogicalOperand=AppFilterDeviceTypeSelectLogicalOperand,
                        AppliFilteringServerType = AppliFilteringServerType, AppFilterServerFileType=AppFilterServerFileType, AppFilterServerFile=AppFilterServerFile, AppFilterServerTypeSelectLogicalOperand=AppFilterServerTypeSelectLogicalOperand,
                        AppFilterSourceDestOperand = AppFilterSourceDestOperand,
                        portsfilterIncludeOrExclude = portsfilterIncludeOrExclude, portsfiltering = portsfiltering)
  
  print(paste("siteMap() : ", Sys.time(), sep=";"))

  MaxMatrix <- as.integer(MaxMatrix)
  unit <- 0
  if ( deviceOrServer != "Device" ) {
    setnames(Data, "con_number", "Traffic")
  } else {
    switch(VolumeUnit,
         SumConnect = {
           setnames(Data, "con_number", "Traffic")
           },
         SumData = {
           setnames(Data, "con_traffic","Traffic")
           },
         ConnectTimesData = {
           setnames(Data, "con_times_traffic","Traffic")
           },
         CountSumUniqueLogin = {
           setnames(Data, "distinct_I_ID_U","Traffic")
           },
         stop)
  }

  print("   siteMap() : reformatStat() call...")
  print(paste("siteMap() : ", Sys.time(), sep=";"))
  output <- try(reformatStat(Stat = Data, deviceOrServer = deviceOrServer,
                             MaxMatrix = MaxMatrix, 
                             unit = unit,
                             DEBUG=FALSE,
                             DetailledFlow = DetailledFlow))
  if("try-error" %in% class(output)) return(NULL)
  print(paste("siteMap() : ", Sys.time(), sep=";"))
  CircosTable <- output[[1]]
  
  if (!is.null(CircosTable)) {
    print("   siteMap() : prepare circos call")
    unit_ResBeforeSpread <- unit;
    unit <- output[2]
    ResBeforeSpread <- output[[3]]
    ResBeforeSpread[, unit:=paste("10^", unit_ResBeforeSpread, sep="")]
    
    
    #Create a directory
    Dir <- ResultDir
    if (!file.exists(Dir))
      dir.create(Dir, showWarnings = FALSE, recursive=TRUE)
    
    sessionId <- ceiling(runif(1, 0, 10^10))
    
    print(paste("siteMap() : ", Sys.time(), sep=";"))
    if (Clustering == TRUE) {
      mclClusterParam <- as.numeric(mclClusterParam)
      print("   siteMap() : Compute clustering...")
      #Compute clustering...
      #clusters <- clustering(ResBeforeSpread, mclClusterParam, algo=ClusteringAlgo, directed=DirectedGraphClustering, NumberOfClusters=as.numeric(kmeanClusterParam))
      ClusteringInput <- gather(CircosTable, SiteCode_Destination, Traffic, -SiteCode_Source)
      clusters <- clustering(ClusteringInput, mclClusterParam, algo=ClusteringAlgo, directed=DirectedGraphClustering, NumberOfClusters=as.numeric(kmeanClusterParam))
      
      #prefix site name with cluster number of the site
      dt.cluster<-data.table(clusters)
      dt.cluster[, Site.newname:=paste(Cluster, Site, sep="-")]
      dt.cluster <- dt.cluster[, list(Site, Site.newname)]
      setkey(dt.cluster, Site)
      
      #prefix for circos table
      tmp_ <- gather(CircosTable, SiteCode_Destination, Traffic, -SiteCode_Source)  #convert matrix to table
      setkey(tmp_, SiteCode_Source)
      tmp_ <- dt.cluster[tmp_, nomatch=0]
      tmp_ <- tmp_[, Site:=NULL]
      setnames(tmp_, "Site.newname", "SiteCode_Source")
      setkey(tmp_, SiteCode_Destination)
      tmp_ <- dt.cluster[tmp_, nomatch=0]
      tmp_ <- tmp_[, Site:=NULL]
      setnames(tmp_, "Site.newname", "SiteCode_Destination")
      #spread = reverse of gather
      CircosTable <- spread_(tmp_, "SiteCode_Destination", "Traffic", fill=0)
      
      #prefix for ResBeforeSpread
      tmp_ <- copy(ResBeforeSpread)
      setkey(tmp_, SiteCode_Source)
      tmp_ <- dt.cluster[tmp_, nomatch=0]
      tmp_ <- tmp_[, Site:=NULL]
      setnames(tmp_, "Site.newname", "SiteCode_Source")
      setkey(tmp_, SiteCode_Destination)
      tmp_ <- dt.cluster[tmp_, nomatch=0]
      tmp_ <- tmp_[, Site:=NULL]
      setnames(tmp_, "Site.newname", "SiteCode_Destination")
      ResBeforeSpread<-tmp_              
    }
    else clusters <- NULL
    print(paste("siteMap() : ", Sys.time(), sep=";"))
    
    #Write the input table for circos
    print("   siteMap() : write files...")
    circosfileName <- paste("circos_", sessionId, ".csv", sep="")
    circosfile <- paste(Dir, "/", circosfileName, sep="")
    write.table(CircosTable, file=circosfile, row.names=FALSE, sep="\t")
    print(paste("siteMap() : ", Sys.time(), sep=";"))
    
    #rename column to be more user friendly
    ResBeforeSpread <- RenameColumns(ResBeforeSpread, Type="Device")
    setnames(ResBeforeSpread, c("Traffic"), c(VolumeUnit))
    
    statfileName <- paste("stat", deviceOrServer, "_", sessionId, ".csv", sep="")
    statfile <- paste(Dir, "/", statfileName, sep="")
    write.table(ResBeforeSpread, file=statfile, row.names=FALSE, sep=",")
    statfilesize <- format(structure(file.info(statfile)$size, class="object_size"), units="auto")
    
    print("   siteMap() : execute circos...")
    #execute circos
    print(paste("siteMap() : ", Sys.time(), sep=";"))
    Title <- paste("\"Data lab - Circos diagram - Mapping between sites\n",
                   "Traffic : Unit = 10^", unit, 
                   ";     Based on : ", VolumeUnit, ";     Device Resolution (IDM or Device) : ", FromSite, "\n",
                   "   Sector  : ", paste(Sector, collapse=", "), ";   Country : ", paste(CountrySiteFilterIn, collapse=", "), ";",
                   "   Zoomed sites  : ", paste(SiteSelectIn, collapse=", "), ";",
                   "\nExcluded Sites : ", paste(RemovedSiteSelectIn, collapse=", "), ";",
                   "   Max Sites limit : ", MaxMatrix, "\n",
                   "Inter/Intra filtering : ", paste(InterIntraSite, collapse="/"), "\n",
                   "\"", 
                   sep="")
    Title <- paste("\"Data lab - Circos diagram - Mapping between sites\n",
                   "Traffic : Unit = 10^", unit, 
                   "\"", 
                   sep="")
    #Title <- paste("\"Data lab - Circos diagram - Mapping between sites\n", "\"", sep="")
    dat <- format(Sys.time(), "%Y%m%d%H%M%S")
    filename <- paste("circos-", sessionId,".png", sep="")
    
    command <- paste(circosScript, " \"", circosfile, "\" ", dat, " \"", circosResHttpServerLocalPath, "/", filename, "\" ", Title, sep="")
    cat("command=", command)
    print("   siteMap() : execute circos...")
    system(command, wait=TRUE, invisible=FALSE)
    
    statServerfilesize <- "fake"
    statServerfileName <-"fake"
    if (Server2Server) {
      print("siteMap() : StatServer2ServerDetail...")
      print(paste("siteMap() : ", Sys.time(), sep=";"))
      if (DateRange == "6Weeks") {
        StatServer <- fread(paste("gunzip -c ", StatServer2ServerDetailFile6Weeks, sep=""), sep=";")
      } else {
        StatServer <- fread(paste("gunzip -c ", StatServer2ServerDetailFile, sep=""), sep=";")
      }
      
      ListServerToGet <- unique(ResBeforeSpread$Dest.IP)
      StatServer <- StatServer[source_ip %in% ListServerToGet, ]
      
      #Rename columns to be more user friendly
      StatServer <- RenameColumns(StatServer, Type = "Server")
      
      statServerfileName <- paste("statServer_", sessionId, ".csv", sep="")
      statServerfile <- paste(Dir, "/", statServerfileName, sep="")
      write.table(StatServer, file=statServerfile, row.names=FALSE, sep=",")
      statServerfilesize <- format(structure(file.info(statServerfile)$size, class="object_size"), units="auto")
    }
    
    print("   siteMap() : return links...")
    print(paste("siteMap() : ", Sys.time(), sep=";"))
    return(list(paste(circosResHttpServerAddress, filename, sep="/"), unit, 
                paste(circosResHttpServerAddress, circosfileName, sep="/"), 
                paste(circosResHttpServerAddress, statfileName,sep="/"), clusters, statfilesize, 
                paste(circosResHttpServerAddress, statServerfileName,sep="/"), statServerfilesize))
  }
  
  else {
    print("   siteMap() : no data return")
    return(c("img/nodata.png", "0"))
  }
}


################################################################################################
################################################################################################
ocpu_singleRun <- function() {
  require(opencpu)
  opencpu$browse("/library/sitemap/www") 
}

################################################################################################
################################################################################################
clustering <- function(stat, mclClusterParam = c(1.5, 4), directed = FALSE, algo = "mcl", NumberOfClusters = 100) {
  #Script to do clustering of sites
  
  library(graph)
  if (algo == "mcl")
    library(MCL)
  setkey(stat, SiteCode_Source)
  table_stat <- data.frame(stat[, list(Traffic=sum(as.numeric(Traffic))), by=list(SiteCode_Source, SiteCode_Destination)])  
  table_stat_adjM <- ftM2adjM(as.matrix(table_stat[,1:2]), W=table_stat$Traffic)
  
  #if not directed graph, convert table_stat_adjM to symetric matrix
  if (!directed) {
    for (col in 2:dim(table_stat_adjM)[2]) {
      for (row in 1:col-1) {
        table_stat_adjM[row, col] <- table_stat_adjM[row, col] + table_stat_adjM[col, row]
        table_stat_adjM[col, row] <- table_stat_adjM[row, col]
      }
    }
    addLoops=FALSE
  }
  else {
    addLoops=TRUE
  }
  
  if (algo == "mcl") {
    cluster <- mcl(table_stat_adjM, addLoops = addLoops, ESM = FALSE, inflation = mclClusterParam[1], expansion = mclClusterParam[2])
    result <- data.frame(Site=rownames(table_stat_adjM), Cluster=cluster$Cluster)
    result <- result[order(result$Cluster, result$Site),]
    result <- data.table(result)
    result[Cluster==0, Cluster:=999]
  }
  
  if (algo == "kmeans") {
    cluster <- kmeans(table_stat_adjM, NumberOfClusters)
    cluster$cluster[cluster$cluster %in% which(cluster$size %in% c(1))]<-999
    result <- cbind(read.table(text=names(cluster$cluster)), cluster$cluster)
    names(result) <- c("Site", "Cluster")
    result <- result[order(result$Cluster, result$Site),]
    result<-data.table(result)
  }
  
  #rename cluster num (expect 999)
  setkey(result, Cluster)
  tmp_<-data.table(Cluster=unique(result$Cluster))
  tmp_[, Cluster.new:=1:.N]
  setkey(tmp_, Cluster)
  result <- tmp_[result, nomatch=0]
  result[Cluster==999, Cluster.new:=999]
  result <- result[, list(Site, Cluster.new)]
  setnames(result, "Cluster.new", "Cluster")
  
  return(result)
}

############################################################################
############################################################################
AppliFilter <- function(Stat, deviceOrServer = "Device",   AppliFilteringDeviceType = "none",
    AppFilterDeviceFileType ="app", AppFilterDeviceFile, AppFilterDeviceTypeSelectLogicalOperand="OR",
    AppliFilteringServerType = "none", AppFilterServerFileType="app", AppFilterServerFile, AppFilterServerTypeSelectLogicalOperand="OR",
    AppFilterSourceDestOperand="AND")
{
  print("AppliFilter() : Begin...")
  print(paste("AppliFilter() : ", Sys.time(), sep=";"))

  if (AppliFilteringDeviceType == "none" && AppliFilteringServerType == "none") {
    print("AppliFilter() : no filtering")
    return(Stat)
  }

  if (AppliFilteringDeviceType == "none" || AppliFilteringServerType == "none") {
    AppFilterSourceDestOperand <- "AND"
  }

  Data <- copy(Stat)
  Data <- RenameColumns(Data)

  if (AppFilterSourceDestOperand == "AND") vecSourceDest <- Data[, vec:=TRUE]$vec
  else vecSourceDest <- Data[, vec:=FALSE]$vec
  Data[, vec:=NULL]

  #Source filtering
  if (AppliFilteringDeviceType != "none") {
    print("         AppliFilter() : Filtering source...")
    if (AppliFilteringDeviceType == "exclude" || AppFilterDeviceTypeSelectLogicalOperand == "AND") vecSource <- Data[, vec:=TRUE]$vec
    else vecSource <- Data[, vec:=FALSE]$vec
    Data[, vec:=NULL]

    file<-paste(ResultDir, AppFilterDeviceFile, sep="/")
    if (!file.exists(file)) {
      print(paste("AppFilterDeviceFile : following file not found : ", file, sep=""))
      return(Stat)
    }

    appfilterData <- tryCatch(
      {
        fread(file, sep=";")
      },
      error=function(cond) {
          return(fread(file, sep="\n"))},
      warning=function(cond) {
          print(cond)
      }
    )

    if (nrow(appfilterData) == 0) {
      vecDest = vecSourceDest
    }


    appfilterColumnName <- colnames(appfilterData)
    for(col in appfilterColumnName) {
      appfilter <- appfilterData[, col, with=FALSE][[1]]
      col <- switch(col,
        source_aip_app_shared_unique_id = "Source.AIP.Shared.Unique.ID",
        AIP.Shared.Unique.ID = "Source.AIP.Shared.Unique.ID",
        source_aip_app_name = "Source.AIP.App.Name",
        aip_app_name = "Source.AIP.App.Name",
        AIPSharedUniqueId = "Source.AIP.Shared.Unique.ID",
        AIPAppName = "Source.AIP.App.Name",
        AIPip = "Source.IP",
        NX_bin_exec_name = "Source.App.Exec.Name",
        col
      )

      if(! col %in% colnames(Data))
      {
        print(paste("         AppliFilter() : column ", col, " to filter not in data source", sep=""))
        next
      }
      appfilter <- appfilter[!is.na(appfilter)]
      if (length(appfilter) == 0) next
      pattern <- paste(appfilter, collapse="|")
      print(paste("col : ", col, "; pattern : ", pattern, sept=""))
      if (AppliFilteringDeviceType == "exclude" || AppFilterDeviceTypeSelectLogicalOperand == "AND") vecSource <- vecSource & grepl(pattern, Data[, col, with=FALSE][[1]])
      else vecSource <- vecSource | grepl(pattern, Data[, col, with=FALSE][[1]])
    }
    if (AppliFilteringDeviceType == "exclude") vecSource <- !vecSource
    print("         AppliFilter() : Filtering source : end")
  }
  else
  {
    vecSource = vecSourceDest
  }


  #Source filtering
  if (AppliFilteringServerType != "none") {
    print("         AppliFilter() : Filtering destinatino...")
    if (AppliFilteringServerType == "exclude" || AppFilterServerTypeSelectLogicalOperand == "AND") vecDest <- Data[, vec:=TRUE]$vec
    else vecDest <- Data[, vec:=FALSE]$vec
    Data[, vec:=NULL]

    file<-paste(ResultDir, AppFilterServerFile, sep="/")
    if (!file.exists(file)) {
      print(paste("AppFilterServerFile : following file not found : ", file, sep=""))
      return(Stat)
    }

    appfilterData <- tryCatch(
      {
        fread(file, sep=";")
      },
      error=function(cond) {
        return(fread(file, sep="\n"))},
      warning=function(cond) {
        print(cond)
      }
    )
    if (nrow(appfilterData) == 0) {
      vecDest = vecSourceDest
    }

    appfilterColumnName <- colnames(appfilterData)
    for(col in appfilterColumnName) {
      appfilter <- appfilterData[, col, with=FALSE][[1]]
      col <- switch(col,
          dest_aip_app_shared_unique_id = {
          "Dest.AIP.Shared.Unique.ID"
          },
            AIP.Shared.Unique.ID = {
            "Dest.AIP.Shared.Unique.ID"
          },
            dest_aip_app_name = {
              "Dest.AIP.App.Name"
          },
          aip_app_name = {
            "Dest.AIP.App.Name"
          },
          AIPSharedUniqueId = {
            "Dest.AIP.Shared.Unique.ID"
          },
          AIPAppName = {
            "Dest.AIP.App.Name"
          },
          AIPip = {
            "Dest.IP"
          },
          NX_bin_exec_name = {
            "Dest.App.Exec.Name"
          },
          {col}
      )

      if(! col %in% colnames(Data))
      {
        print(paste("         AppliFilter() : column ", col, " to filter not in data source", sep=""))
        next
      }
      appfilter <- appfilter[!is.na(appfilter)]
      if (length(appfilter) == 0) next
      pattern <- paste(appfilter, collapse="|")
      print(paste("col : ", col, "; pattern : ", pattern, sept=""))
      if (AppliFilteringServerType == "exclude" || AppFilterServerTypeSelectLogicalOperand == "AND") vecDest <- vecDest & grepl(pattern, Data[, col, with=FALSE][[1]])
      else vecDest <- vecDest | grepl(pattern, Data[, col, with=FALSE][[1]])
    }
    if (AppliFilteringServerType == "exclude") vecDest <- !vecDest
  }
  else
  {
    vecDest = vecSourceDest
  }



  if (AppFilterSourceDestOperand == "AND") vecRes <- vecSource & vecDest
  else vecRes <- vecSource | vecDest

  result <- Stat[vecRes]

  print(paste("      AppliFilter() : ", Sys.time(), sep=";"))
  return(result)
}


############################################################################
############################################################################
UploadAppFilterFile <- function(file) {
  #read and check format of file
  data <- tryCatch(
    {
      fread(file, sep=";")
    },
    error=function(cond) {
      return(fread(file, sep="\n"))},
    warning=function(cond) {
      print(cond)
    }
  )

  #create corresponding file
  sessionId <- ceiling(runif(1, 0, 10^10))
  filename=paste("AppliFilter-", sessionId, ".csv", sep="")
  write.table(data, file=paste(ResultDir, "/", filename, sep=""), row.names=FALSE, sep=";")
  return(filename);
}

############################################################################
############################################################################
RenameColumns <- function(DataTab, Type = "Device") {
  for (field in colnames(DataTab)) {
    switch(field,
           "SiteCode_Source" = {
             setnames(DataTab, c("SiteCode_Source"), c("Source.Site"))
           },
           "source_sector" = {
             setnames(DataTab, c("source_sector"), c("Source.Sector"))
           },
           "source_teranga" = {
             setnames(DataTab, c("source_teranga"), c("Source.Teranga"))
           },
           "SiteName_Source" = {
             setnames(DataTab, c("SiteName_Source"), c("Source.SiteName"))
           },
           "SiteCategory_Source" = {
             setnames(DataTab, c("SiteCategory_Source"), c("Source.SiteCategory"))
           },
           "SiteCode_Destination" = {
             setnames(DataTab, c("SiteCode_Destination"), c("Dest.Site"))
           },
           "SiteName_Destination" = {
             setnames(DataTab, c("SiteName_Destination"), c("Dest.SiteName"))
           },
           "SiteCategory_Destination" = {
             setnames(DataTab, c("SiteCategory_Destination"), c("Dest.SiteCategory"))
           },
           "source_app_name" = {
             setnames(DataTab, c("source_app_name"), c("Source.App.Name"))
           },
           "source_app_exec" = {
             setnames(DataTab, c("source_app_exec"), c("Source.App.Exec.Name"))
           },
           "url" = {
             setnames(DataTab, c("url"), c("Source.Wr.Url"))
           },
           "dest_ip" = {
             setnames(DataTab, c("dest_ip"), c("Dest.IP"))
           },
           "dest_aip_server_hostname" = {
             setnames(DataTab, c("dest_aip_server_hostname"), c("Dest.Hostname"))
           },
           "dest_port" = {
             setnames(DataTab, c("dest_port"), c("Dest.Port"))
           },
           "con_protocol" = {
             setnames(DataTab, c("con_protocol"), c("Dest.Protocol"))
           },
           "dest_aip_app_name" = {
             setnames(DataTab, c("dest_aip_app_name"), c("Dest.AIP.App.Name"))
           },
           "dest_aip_server_function" = {
             setnames(DataTab, c("dest_aip_server_function"), c("Dest.AIP.Function"))
           },
           "dest_aip_server_subfunction" = {
             setnames(DataTab, c("dest_aip_server_subfunction"), c("Dest.AIP.SubFunction"))
           },
           "dest_aip_app_criticality" = {
             setnames(DataTab, c("dest_aip_app_criticality"), c("Dest.AIP.Criticality"))
           },
           "dest_aip_app_type" = {
             setnames(DataTab, c("dest_aip_app_type"), c("Dest.AIP.App.Type"))
           },
           "AIP.Category.Label" = {
             setnames(DataTab, c("AIP.Category.Label"), c("Dest.AIP.Category.Label"))
           },
           "AIP.Category.Int" = {
             setnames(DataTab, c("AIP.Category.Int"), c("Dest.AIP.Category.Int"))
           },
           "dest_aip_app_sector" = {
             setnames(DataTab, c("dest_aip_app_sector"), c("Dest.AIP.Sector"))
           },
           "dest_aip_app_shared_unique_id" = {
             setnames(DataTab, c("dest_aip_app_shared_unique_id"), c("Dest.AIP.Shared.Unique.ID"))
           },
           "dest_aip_appinstance_type" = {
             setnames(DataTab, c("dest_aip_appinstance_type"), c("Dest.AIP.Type.Env"))
           },
           "source_ip" = {
             setnames(DataTab, c("source_ip"), c("Source.IP"))
           },
           "source_aip_server_hostname" = {
             setnames(DataTab, c("source_aip_server_hostname"), c("Source.Hostname"))
           },
           "source_app_category" = {
             setnames(DataTab, c("source_app_category"), c("Source.App.Category"))
           },
           "source_site" = {
             setnames(DataTab, c("source_site"), c("Source.Site"))
           },
           "source_app_company" = {
             setnames(DataTab, c("source_app_company"), c("Source.App.Company"))
           },
           "con_status" = {
             setnames(DataTab, c("con_status"), c("Dest.Con.Status"))
           },
           "dest_site" = {
             setnames(DataTab, c("dest_site"), c("Dest.Site"))
           }
    )
  }
  return(DataTab)
}

############################################################################
## take a dataframe as input with 3 columns : source, destination, size
## output : input dataframe converted as matrix and generated circos svg link
circosGraph <- function(input, nb_threshold = 100, volume_threshold = 1000000) {

      print("    circosGraph() : Begin...")

    print("circosGraph : input =")
    input
    str(input)
    DT = data.table(input)
    print(paste("dim(DT) : ", dim(DT), sep=""))

    setnames(DT, c("source", "destination", "traffic"))
    DT[, traffic:=as.integer(traffic)]
    unit=0
    DT <- DT[source != "", ][destination !="", ]

    if (dim(DT)[1] != 0) {
    #convert ColForVolume in units in such a way that it's not greater than volume_threshold
      while (max(DT[, traffic]) >= volume_threshold) {
        DT[, traffic:=round(traffic/1000, digits=0)]
        unit=unit+3
      }

      print("    circosGraph() : Reduce nomber of rows+Columns to MaxMatrix...")
      nblinemax <- nb_threshold*10
      if (dim(DT)[1] < nblinemax) nblinemax <- dim(DT)[1]
      DT <- DT[order(-rank(traffic))][1:nblinemax, ]
      while(length(unique(c(DT$source, DT$destination))) > nb_threshold)
      {
	nblinemax <- round(nblinemax - (nblinemax/10),0)
      	DT <- DT[order(-rank(traffic))][1:nblinemax, ]
      }

    #replace white space by '_'
    DT[, source:=gsub("\\(","-", gsub("\\)","", gsub("\\|", "&", gsub(" ","_", source))))]
    DT[, destination:=gsub("\\(","-", gsub("\\)","", gsub("\\|", "&", gsub(" ","_", destination))))]

    print("    reformatStat() : Spread...")
    DT <- spread_(DT, "destination", "traffic", fill=0)
    print(paste("dim(DT) 4 : ", dim(DT), sep=""))

    #Create a directory
    Dir <- ResultDir
    if (!file.exists(Dir))
      dir.create(Dir, showWarnings = FALSE, recursive=TRUE)

    sessionId <- ceiling(runif(1, 0, 10^10))
    circosfileName <- paste("circos_", sessionId, ".csv", sep="")
    circosfile <- paste(Dir, "/", circosfileName, sep="")
    write.table(DT, file=circosfile, row.names=FALSE, sep="\t")
    print(paste("dim(DT) : ", dim(DT), sep=""))


    Title <- paste("\"Data lab - Circos diagram\n",
                       "Traffic : Unit = 10^", unit,
                       "\"",
                       sep="")
    dat <- paste(format(Sys.time(), "%Y%m%d%H%M%S"), sample(1:1000,1)[1], sep="_")
    filename <- paste("circos-", sessionId,".png", sep="")

    command <- paste(circosScript, " \"", circosfile, "\" ", dat, " \"", circosResHttpServerLocalPath, "/", filename, "\" ", Title, sep="")
    cat("command=", command)
    print("   circosGraph() : execute circos...")
    system(command, wait=TRUE, invisible=FALSE)
    if (file.exists(paste(circosResHttpServerLocalPath,"/", filename, sep=""))) {
    	return(list(paste(MasterURL,circosResHttpServerAddress,"/", filename, sep=""),
                paste(circosResHttpServerAddress, circosfileName, sep="/")))
    }
    else {
    	print("   siteMap() : error : no image result")
    	return(c(paste(MasterURL,circosResHttpServerAddress,"/error.png",sep=""), "0"))
    }
  }
  else {
    print("   siteMap() : no data return")
    return(c(paste(MasterURL,circosResHttpServerAddress,"/nodata.png",sep=""), "0"))
  }
}
