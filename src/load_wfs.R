# script to download wfs site information

source("src/helpers.R")
source("src/Hilltop.R")

# Load csv with WFS endpoints
wfs_list <- fread("input/wfs_list.csv")
wfs_request_fields_ms <- fread("input/wfs_request_fields_monitoringsite.csv")

# download WFS feed contents
for (i in 1:nrow(wfs_list)){
    tryCatch({download_wfs(wfs_list[i, wfs], wfs_list[i, region])},
             error=function(e){
                 cat("ERROR downloading from", wfs_list[i, region],":\n",
                     conditionMessage(e), "\n")})
}

# read WFS feed contents
for (i in 1:nrow(wfs_list)){
    tryCatch({read_wfs(wfs_list[i, region])},
             error=function(e){
                 cat("ERROR reading wfs.csv from", wfs_list[i, region],":\n",
                     conditionMessage(e), "\n")})
}

# check wfs fields against LAWA request
# create mandatory fields field
wfs_request_fields_ms[
    grepl("mandatory", data_to_be_provided, ignore.case = TRUE), 
    mandatory := TRUE]

wfs_request_fields_ms[, field_name := tolower(field_name)]

for(i in 1:nrow(region_list)){
    tryCatch({wfs_request_fields_ms[, c(paste0(region_list[i, region],"_has")) :=
                              lapply(region_list[i, wfs_file], function(x) field_name %in% colnames(get(x)))]},
             error=function(e){
                 cat("ERROR reading wfs.csv from", wfs_list[i, region],":\n",
                     conditionMessage(e), "\n")})
}

wfs_request_fields_ms <- as.data.table(wfs_request_fields_ms)
cols <- c("data_to_be_provided", "details", "type", "mandatory")
GW_list <- wfs_request_fields_ms[field_name == "gwqualitystatus",
                                 !(names(wfs_request_fields_ms) %in% cols), with = FALSE]
colnames(GW_list) <- gsub("_has", "", colnames(GW_list))

GW_list <- melt(GW_list, 
                id.vars = "field_name",
                variable.name = "region",
                value.name = "gw_wfs")
GW_list[gw_wfs == FALSE,]

#horizons has "gwqualitys"
# TODO: 
# 1) assess if translation tables are required for wfs field names
# 2) include other WFS information (consents drinking water and bores)
# 3) build automatic report for each council on provided fields