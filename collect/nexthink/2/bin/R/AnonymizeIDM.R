#!/usr/bin/env Rscript

R_WD<-Sys.getenv("R_WD")
setwd(R_WD)

args <- commandArgs(TRUE)
DestFile <- args[1]

source("conf.R")

require(data.table)
require(bit64)
require(gsubfn)
require(uuid)

################################################################################################
################################################################################################
#Function to generate IDM file anonymized
IDMAnonymized <- function() {
  #Read IDM
  IDM<-fread(IDMFILE, sep=";")
  IDM<-IDM[, c(1, 6,7, 8, 9, 10, 11, 12, 13, 23, 22, 2, 18), with=FALSE]
  setnames(IDM, 
           c("ID", "Sector", "SubSector", "CountryCode", 
             "CountryName", "SiteCode", "SiteName", "TerangaCode", 
             "TerangaCountryCode", "ADLogin", "ADDomain", "FullName", "Status"))
  
  IDM<-IDM[ADLogin!="" & Status == "ACTIVE", ]
  
  IDM[, Login:=paste(tolower(ADLogin), "@", tolower(ADDomain), sep="")]
  
  #Concatenate Sector with SubSector
  IDM[SubSector!="", Sector:=paste(Sector, SubSector, sep="/")]
  IDM<-IDM[, 
            list(Sector, SiteCode, Login, SiteName, CountryCode)]
  
  setkey(IDM,Login)
  IDM<-unique(IDM)
  
  #Read Dictionnary
  Dico <- fread(DICTIONNARY, sep=";")
  
  #join Dico and IDM
  setkey(Dico, name)
  setkey(IDM, Login)
  
  IDM <- Dico[IDM, nomatch=0]
  
  
  return(IDM[, list(I_ID, Sector, SiteCode, SiteName, CountryCode)])
  
}

IDMAnonymized_Write <- function(Dest) {
  write.table(IDMAnonymized(), file=Dest, sep=";", row.names=FALSE)
}

IDMAnonymized_Write(DestFile)
