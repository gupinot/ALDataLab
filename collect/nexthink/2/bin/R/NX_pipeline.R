source("conf.R")
source("Anonymize.R")


####################################################################################
####################################################################################
NX_pipe_connection <- function(FileType = "connection", patternin = "^connection.*.gz$", MoveScannedFileToArchive = TRUE) {
  
  lstFileToPipe <- list.files(INNXFILES, pattern=patternin, full.names = TRUE)
  
  i<-1
  for (filein in lstFileToPipe) {
    filein_path <- dirname(filein)
    filein_file <- basename(filein)
    print(paste("NX_pipe_connection() : ", i, "/", length(lstFileToPipe), " : ", filein_file, sep=""))
    
    print(paste("NX_pipe_connection() : try AnonymizeFile for ", filein_file, sep=""))
    res <- try(AnonymizeFile(paste(INNXFILES, filein_file, sep="/"), paste(ANONYMIZED, filein_file, sep="/"), FileType = FileType))
    print(paste("NX_pipe_connection() : try AnonymizeFile for ", filein_file, " ended with class(res)=", class(res), sep=""))

    if (MoveScannedFileToArchive & class(res) != "try-error") {
      print("   Move FileIn to done directory...")
      DestFile<-paste(DONENXFILESIN, "/", basename(filein), sep="")
      file.rename(filein, DestFile)
      print("   Move FileIn to done directory : done")
    }
    i <- i + 1
  }
  print("NX_pipe_connection() : End")
  
}


