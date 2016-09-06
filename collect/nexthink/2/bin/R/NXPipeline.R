#!/usr/bin/env Rscript
args <- commandArgs(TRUE)

ServeurID <- args[1]
File <- args[2]
FileType=args[3]

R_WD<-Sys.getenv("R_WD")
setwd(R_WD)

File <- basename(File)

print(paste("Rscript : ", ServeurID, " : ", File, sep=""))
try(source("NX_pipeline.R")) 
NX_pipe_connection(FileType = FileType, patternin=File, MoveScannedFileToArchive = TRUE)
print("NXPipeline.R : End")
