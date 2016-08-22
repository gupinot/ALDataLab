#Read dictionary
require(data.table)
DicoPath <- "/home/datalab/Repo/Dictionnary.csv"
BigFixPath <- "/home/datalab/Repo/BigFix.csv"
BigFixAnoPath <- "/home/datalab/Repo/BigFixAno.csv.gz"

DicoPath <- "/Users/guillaumepinot/Dev/Alstom/V2/dictionnary_20160616.csv"
BigFixPath <- "/Users/guillaumepinot/Dev/Alstom/Repository/BigFix/BigFix_20160616.csv"
BigFixAnoPath <- "/Users/guillaumepinot/Dev/Alstom/Repository/BigFix/BigFix_20160616_ano.csv.gz"


Dico <- fread(DicoPath, sep=";")

#is pb caractère null, sous bash :
#export LC_CTYPE=C; cat BF.csv | tr -d "\0"  > BF_correct.csv

BigFix <- fread(BigFixPath, sep=",")

Dico[, name:=tolower(name)]

setnames(BigFix, c("bf_device_name", "LastReportTime", "ComputerCountry",
"ComputerLocation", "PrimaryUser", "PrimaryUserAlstomIdFromIDM", "PrimaryUserEmailFromIDM",
"PrimaryUserCountryFromIDM", "PrimaryUserLocationFromIDM",
"PrimaryUserSectorFromIDM", "tem_server", "Program", "ProductCode",
"Version", "UserInstallation", "Count", "ClassName", "SuperClassName"))

BigFix[, bf_device_name:=tolower(bf_device_name)]
setkey(Dico,name)
setkey(BigFix, bf_device_name)

BigFixRes <- Dico[BigFix, nomatch=NA]
BigFixRes <- BigFixRes[is.na(I_ID), I_ID:=paste("unknown device -", name, sep="")]


BigFixRes <- BigFixRes[, list(I_ID, LastReportTime, ComputerCountry,
    ComputerLocation,
    PrimaryUserCountryFromIDM, PrimaryUserLocationFromIDM,
    PrimaryUserSectorFromIDM, tem_server, Program, ProductCode,
    Version, UserInstallation, Count, ClassName, SuperClassName)]
setnames(BigFixRes, c("I_ID"), c("I_ID_D"))

write.table(BigFixRes, gzfile(BigFixAnoPath), sep=";", row.names=FALSE)

