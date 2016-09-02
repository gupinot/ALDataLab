source("conf.R")

require(data.table)
require(bit64)
require(gsubfn)
require(uuid)

#principe anonymisation :
# champs à anonymiser : NX_user_name et NX_device_name
# Une table dictionnaire est créée :
#   NX_name (pour chaque valeur vue NX_user_name ou NX_device_name) : 
#   I_ID : généré au hasard avec risque de collision très faible

#Les champs sont ensuite anonymisés via cette table dictionnaire
# IDM : I_ID, Sector, Site
# NXData : I_ID_D (pour le NX_device_name), I_ID_U (pour NX_user_name)

# problématiques à gérer : 
#   concurrence accès sur la table dictionnaire
#     - risque écrasement, risque plusieurs I_ID générés pour même valeur
#     - solution : poser un verrou
#   sécuriser accès au dictionnaire
#   sauvegarder dictionnaire

########################################################################################################################
AnonymizeFile <- function(FileIn, FileOut, FileType = "connection") {
  print("AnonymizeFile() : begin")

  print("AnonymizeFile() : read file...")
  NXData <- fread(paste("gunzip -c ", FileIn, sep=""), sep="\t")
  
  
  if (FileType == "connection") {
    setnames(NXData, c("NX_con_start_time", "NX_con_end_time", 
                       "NX_con_duration", "NX_con_cardinality", "NX_con_destination_ip", 
                       "NX_con_out_traffic", "NX_con_in_traffic", "NX_con_type", "NX_con_status", "NX_con_port", 
                       "NX_bin_app_category", "NX_bin_app_company", "NX_bin_app_name", "NX_bin_exec_name", "NX_bin_paths", 
                       "NX_bin_version", "NX_device_name", "NX_device_last_ip", "NX_device_last_logged_on_user", "NX_user_name", "NX_user_id",
                       "NX_user_department", "NX_user_sid", "NX_user_full_name", "NX_user_distinguished_name"))

    NXData <- NXData[, list(NX_con_start_time, NX_con_end_time, NX_con_duration, NX_con_cardinality, NX_con_destination_ip, 
                            NX_con_out_traffic, NX_con_in_traffic, NX_con_type, NX_con_status, NX_con_port, 
                            NX_bin_app_category, NX_bin_app_company, NX_bin_app_name, NX_bin_exec_name, NX_bin_paths, 
                            NX_bin_version, NX_device_name, NX_device_last_ip, NX_user_name)]
    
    #convert user_name to lower case and suppress .ad.sys suffix
    NXData[, NX_user_name:=tolower(NX_user_name)]
    NXData[, NX_user_name:=gsub(".ad.sys", "", NX_user_name)]
    
    print("AnonymizeFile() : anonymize NX_user_name")
    Res <- Anonymize(unique(NXData$NX_user_name))
    if (is.null(Res)) return(NULL)
    setkey(Res, name)
    setkey(NXData, NX_user_name)
    NXData <- Res[NXData, nomatch=NA]
    setnames(NXData, c("I_ID", "name"), c("I_ID_U", "NX_user_name"))

    print("AnonymizeFile() : anonymize NX_device_name")
    Res <- Anonymize(unique(NXData$NX_device_name))
    if (is.null(Res)) return(NULL)
    setkey(Res, name)
    setkey(NXData, NX_device_name)
    NXData <- Res[NXData, nomatch=NA]
    setnames(NXData, c("I_ID", "name"), c("I_ID_D", "NX_device_name"))
    
    NXData <- NXData[, !c("NX_device_name", "NX_user_name"), with=FALSE]

    #add engine and filedt columns
    reg<-regmatches(basename(FileIn), regexec("^([^_]+)_([^_]+)_(.*)(\\.tgz\\.csv)", basename(FileIn)))
    enginename<-reg[[1]][3]
    filedate<-reg[[1]][4]
    print(paste("AnonymizeFile() : enginename=", enginename, sep=""))
    print(paste("AnonymizeFile() : filedate=", filedate, sep=""))
    NXData[, engine:=enginename]
    NXData[, filedt:=filedate]
    
  }
  else if (FileType == "webrequest") {
    #webrequest
    NXData <- NXData[, c(1, 2, 3, 11, 15, 17, 18, 20, 21, 24), with=FALSE]
    setnames(NXData, c("wr_id", "wr_start_time", "wr_end_time", 
                         "wr_url", "wr_user_name", "wr_device_name", 
                         "wr_device_last_ip", "wr_destination_port", 
                         "wr_destination_ip", "wr_application_name"))

    NXData <- NXData[, list(wr_start_time, wr_end_time, 
                            wr_url, wr_device_name, 
                            wr_destination_port, wr_destination_ip, wr_application_name)]
    
    print("AnonymizeFile() : anonymize wr_device_name")
    Res <- Anonymize(unique(NXData$wr_device_name))
    if (is.null(Res)) return(NULL)
    setkey(Res, name)
    setkey(NXData, wr_device_name)
    NXData <- Res[NXData, nomatch=NA]
    setnames(NXData, c("I_ID", "name"), c("I_ID_D", "wr_device_name"))
    
    NXData <- NXData[, !c("wr_device_name"), with=FALSE]

    #add engine and filedt columns
    reg<-regmatches(basename(FileIn), regexec("^([^_]+)_([^_]+)_(.*)(\\.tgz\\.csv)", basename(FileIn)))
    enginename<-reg[[1]][3]
    filedate<-reg[[1]][4]
    print(paste("AnonymizeFile() : enginename=", enginename, sep=""))
    print(paste("AnonymizeFile() : filedate=", filedate, sep=""))
    NXData[, engine:=enginename]
    NXData[, filedt:=filedate]
  }
  else if (FileType == "execution") {
    #execution
    if (length(colnames(NXData)) == 16) {
    	setnames(NXData, c("ex_start_time", "ex_end_time",
    	"ex_bin_path", "ex_cardinality", "ex_duration",
    	"ex_status", "ex_app_category", "ex_app_company",
    	"ex_app_name", "ex_app_bin_exec_name", "ex_app_bin_paths",
    	"ex_app_bin_version", "ex_app_bin_hash", "ex_user_name", "ex_device_name", "ex_device_ip"))

    } else {
	if (length(colnames(NXData)) == 15) {
    		setnames(NXData, c("ex_start_time", "ex_end_time",
    		"ex_bin_path", "ex_cardinality", "ex_duration",
    		"ex_status", "ex_app_category", "ex_app_company",
    		"ex_app_name", "ex_app_bin_exec_name", "ex_app_bin_paths",
    		"ex_app_bin_version", "ex_user_name", "ex_device_name", "ex_device_ip"))

    	}
    	else {
    		setnames(NXData, c("ex_start_time", "ex_end_time",
    		"ex_bin_path", "ex_cardinality", "ex_duration",
    		"ex_status", "ex_app_category", "ex_app_company",
    		"ex_app_name", "ex_app_bin_exec_name", "ex_app_bin_paths",
    		"ex_app_bin_version", "ex_user_name", "ex_device_name"))
    	}
    }

    #convert user_name to lower case and suppress .ad.sys suffix
    NXData[, ex_user_name:=tolower(ex_user_name)]
    NXData[, ex_user_name:=gsub(".ad.sys", "", ex_user_name)]

    print("AnonymizeFile() : anonymize ex_user_name")
    Res <- Anonymize(unique(NXData$ex_user_name))
    if (is.null(Res)) return(NULL)
    setkey(Res, name)
    setkey(NXData, ex_user_name)
    NXData <- Res[NXData, nomatch=NA]
    setnames(NXData, c("I_ID", "name"), c("I_ID_U", "ex_user_name"))

    print("AnonymizeFile() : anonymize ex_device_name")
    Res <- Anonymize(unique(NXData$ex_device_name))
    if (is.null(Res)) return(NULL)
    setkey(Res, name)
    setkey(NXData, ex_device_name)
    NXData <- Res[NXData, nomatch=NA]
    setnames(NXData, c("I_ID", "name"), c("I_ID_D", "ex_device_name"))

    NXData <- NXData[, !c("ex_device_name", "ex_user_name"), with=FALSE]

    #add engine and filedt columns
    reg<-regmatches(basename(FileIn), regexec("^([^_]+)_([^_]+)_(.*)(\\.tgz\\.csv)", basename(FileIn)))
    enginename<-reg[[1]][3]
    filedate<-reg[[1]][4]
    print(paste("AnonymizeFile() : enginename=", enginename, sep=""))
    print(paste("AnonymizeFile() : filedate=", filedate, sep=""))
    NXData[, engine:=enginename]
    NXData[, filedt:=filedate]
  
  }
  else if (FileType == "listener") {
    
    #convert user_name to lower case and suppress .ad.sys suffix
    NXData[, source_user:=tolower(source_user)]
    NXData[, source_user:=gsub(".ad.sys", "", source_user)]
    
    #when source_user has not @domX value included, find this value in IDM if exists
    NXData_ok <- NXData[grepl('@', source_user]
    NXData_ko <- NXData[!grepl('@', source_user]
    for (IDMFILE in strsplit(IDMFILES, " ")[[1]]) {

      print(paste("IDMFILE used : ",IDMFILE, sep=""))
      #Read IDM
      IDM<-fread(paste("gunzip -c ", IDMFILE, sep=""), sep=";")
      IDM<-IDM[, c(1, 23, 22, 18), with=FALSE]
      setnames(IDM,
             c("ID", "ADLogin", "ADDomain", "Status"))

      IDM<-IDM[ADLogin!="" & Status == "ACTIVE" & ADDomain!="", ]
  
      IDM[, Login:=paste(tolower(ADLogin), "@", tolower(ADDomain), sep="")]

      IDM<-IDM[, list(ID, Login)]

      setkey(IDM,ID)
      IDM<-unique(IDM)

      #join Dico and IDM
      setkey(IDM, ID)
      setkey(NXData_ko, source_user)

      tmp <- IDM[NXData_ko, nomatch=0]
      tmp[, source_user:=Login]
      
      NXData_ok <- rbindlist(list(NXData_ok, tmp[, list(I_ID, Sector, SiteCode, SiteName, CountryCode, TerangaCode)]), use.names=TRUE)
      setkey(I_ID_ok, I_ID)
      I_ID_ok <- unique(I_ID_ok)
      print(paste("dim(I_ID_ok) : ",dim(I_ID_ok), sep=""))
      I_ID_ko <- I_ID_ko[!IDM][, list(I_ID, name)]
      print(paste("dim(I_ID_ko) : ",dim(I_ID_ko), sep=""))
    }
    
    
    NXData[, source_host_name:=toupper(source_host_name)]

    print("AnonymizeFile() : anonymize source_user")
    Res <- Anonymize(unique(NXData$source_user))
    if (is.null(Res)) return(NULL)
    setkey(Res, name)
    setkey(NXData, source_user)
    NXData <- Res[NXData, nomatch=NA]
    setnames(NXData, c("I_ID", "name"), c("I_ID_U", "source_user"))

    print("AnonymizeFile() : anonymize source_host_name")
    Res <- Anonymize(unique(NXData$source_host_name))
    if (is.null(Res)) return(NULL)
    setkey(Res, name)
    setkey(NXData, source_host_name)
    NXData <- Res[NXData, nomatch=NA]
    setnames(NXData, c("I_ID", "name"), c("I_ID_D", "source_host_name"))
    
    NXData <- NXData[, !c("source_host_name", "source_user"), with=FALSE]
  }
  else {
	print(paste("AnonymizeFile() : ERR : FileType not supported (", FileType, ")", sep=""))
	return(NULL)
  }
  
  write.table(NXData, gzfile(FileOut), sep=";", row.names=FALSE)

  print("AnonymizeFile() : end")
}

########################################################################################################################
Anonymize <- function(namelist) {
  
  print("Anonymize() : begin")

  lockfile=paste(DICTIONNARY, ".lck", sep="")
  while(TRUE) {
    if (file.exists(lockfile)) 
      Sys.sleep(0.5)
    else {
      file.create(lockfile)
      break
    }
  }

  print("Anonymize() : read DICTIONNARY")
  if (!file.exists(DICTIONNARY)) 
    Dico <- data.table(name=as.character(NULL), I_ID=as.character(NULL))
  else
    Dico <- fread(DICTIONNARY, sep=";")

  print("Anonymize() : join ")
  Res <- data.table(name=namelist)
  setkey(Dico, name)
  setkey(Res, name)
  Res <- Dico[Res, nomatch=NA]

  ValueKo <- Res[is.na(I_ID), ]
  
  if (dim(ValueKo)[1] != 0) {
    print("Anonymize() : create new anonymized values")
    ValueKo[, I_ID:=mapply(uid, name)]
    Res <- rbindlist(list(Res[!is.na(I_ID)], ValueKo), use.names=TRUE)
  
    Dico <- rbindlist(list(Dico, ValueKo), use.names=TRUE)
    setkey(Dico, name)
    Dico <- unique(Dico)
    print("Anonymize() : store new anonymized values")
    if (file.exists(DICTIONNARY)) 
      file.copy(DICTIONNARY, paste(DICTIONNARY_HIST, "/", basename(DICTIONNARY), ".", format(Sys.time(), "%Y%m%d%H%M%S"), sep=""))
    write.table(Dico, file=DICTIONNARY, sep=";", row.names = FALSE)
  }
  print("Anonymize() : remove lock")
  file.remove(lockfile)
  
  print("Anonymize() : end")
  return(Res)
}


uid <- function(name) {
  return(UUIDgenerate())
}
