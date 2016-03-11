#!/usr/bin/env Rscript

R_WD<-Sys.getenv("R_WD")
setwd(R_WD)

args <- commandArgs(TRUE)
IDMFiles <- args[1]
DestFile <- args[2]
I_ID_REF <- args[3]

source("conf.R")

require(data.table)
require(bit64)
require(gsubfn)
require(uuid)

################################################################################################
################################################################################################
#Function to generate IDM file anonymized
IDMAnonymized <- function(IDMFILES, I_ID_REF) {

  #Read Dictionnary
  Dico <- fread(DICTIONNARY, sep=";")
  setkey(Dico, I_ID)
  print(paste("str(Dico) : ",str(Dico), sep=""))

  if (file.exists(I_ID_REF)) {
    I_ID <- fread(paste("gunzip -c ", I_ID_REF, sep=""), sep=";")
    setkey(I_ID, I_ID)
    I_ID <- unique(I_ID)
    setkey(I_ID, I_ID)
    I_ID_ok <- Dico[I_ID, nomatch=0][, list(I_ID, Sector, SiteCode, SiteName, CountryCode, TerangaCode)]
    I_ID_ko <- Dico[!I_ID][, list(I_ID, name)]
  }
  else {
    I_ID_ok <- data.table(I_ID=as.character(),
                Sector=as.character(),
                SiteCode=as.character(),
                SiteName=as.character(),
                CountryCode=as.character(),
                TerangaCode=as.character())
    I_ID_ko <- Dico
  }

  for (IDMFILE in strsplit(IDMFILES, " ")[[1]]) {

    print(paste("IDMFILE used : ",IDMFILE, sep=""))
    #Read IDM
    IDM<-fread(paste("gunzip -c ", IDMFILE, sep=""), sep=";")
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
              list(Sector, SiteCode, Login, SiteName, CountryCode, TerangaCode)]

    setkey(IDM,Login)
    IDM<-unique(IDM)

    #join Dico and IDM
    setkey(IDM, Login)
    setkey(I_ID_ko, I_ID)

    tmp <- I_ID_ko[IDM, nomatch=0]
    I_ID_ok <- rbindlist(list(I_ID_ok, tmp[, list(I_ID, Sector, SiteCode, SiteName, CountryCode, TerangaCode)]), use.names=TRUE)
    setkey(I_ID_ok, I_ID)
    I_ID_ok <- unique(I_ID_ok)
    I_ID_ko <- I_ID_ko[!IDM][, list(I_ID, name)]
  }

  return(I_ID_ok)
}

################################################################################################
IDMAnonymized_Write <- function(IDM_files, Dest_file, I_ID_REF) {
  Res <- IDMAnonymized(IDM_files, I_ID_REF)
  write.table(Res, gzfile(Dest_file), sep=";", row.names=FALSE)
  write.table(Res, gzfile(I_ID_REF), sep=";", row.names=FALSE)
}

################################################################################################
print("IDMFiles : ")
IDMFiles
IDMAnonymized_Write(IDMFiles, DestFile, I_ID_REF)
