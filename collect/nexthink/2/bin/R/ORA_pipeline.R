source("conf.R")
source("Anonymize.R")


####################################################################################
####################################################################################
ORA_pipe_connection <- function(FileType = "listener", patternin = "^listener.*.csv.gz$", MoveScannedFileToArchive = TRUE) {
  
  lstFileToPipe <- list.files(INORAFILES, pattern=patternin, full.names = TRUE)
  
  i<-1
  for (filein in lstFileToPipe) {
    filein_path <- dirname(filein)
    filein_file <- basename(filein)
    print(paste("ORA_pipe_connection() : ", i, "/", length(lstFileToPipe), " : ", filein_file, sep=""))
    
    print(paste("ORA_pipe_connection() : try AnonymizedOraFile for ", filein_file, sep=""))
    res <- try(AnonymizeFile(paste(INORAFILES, filein_file, sep="/"), paste(ORAANONYMIZED, filein_file, sep="/"), FileType = FileType))
    print(paste("ORA_pipe_connection() : try AnonymizedFile for ", filein_file, " ended with class(res)=", class(res), sep=""))

    if (MoveScannedFileToArchive & class(res) != "try-error") {
      print("   Move FileIn to done directory...")
      DestFile<-paste(DONEORAFILESIN, "/", basename(filein), sep="")
      print(paste("command : file.rename(", filein, ", ", DestFile, ")", sep=""))
      file.rename(filein, DestFile)
      print("   Move FileIn to done directory : done")
    }
    i <- i + 1
  }
  print("ORA_pipe_connection() : End")
  
}


