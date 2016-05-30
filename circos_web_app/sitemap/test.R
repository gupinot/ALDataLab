source("R/conf.R")
source("R/SiteMap.R")
siteMap(VolumeUnit="SumConnect", SiteSelectIn = c("LPT1"), SiteSelectOut = c("LPT1"), InterIntraSite = c(TRUE, TRUE), DetailledFlow = TRUE, SiteSelectOperand="AND", DateRange = "6Weeks")
#LstPannelSetting()
#siteMap(DetailledFlow=TRUE)

