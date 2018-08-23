# build site tables for each region
source("src/load_wfs.R")

# define minimum required fields
fields <- c("siteid", "councilsiteid","lawasiteid", "gwqualitystatus")

# find which regions have minimum required cols
wfs_request_fields_ms[field_name %in% fields,]

site_list <- list()
for(i in 1:nrow(region_list)){
    tryCatch({
        df <- as.data.table(get(region_list[i, wfs_file]))
        df <- df[, fields, with = FALSE]
        df[, region := region_list[i, region]]
        # df[, siteid]
        if(region_list[i, region] == "Canterbury"){
            df <- df[gwqualitystatus == "YES",]
        } else {
            df <- df[gwqualitystatus == TRUE |
                         gwqualitystatus == "Yes" |
                         gwqualitystatus == "Y",]}
        site_list[[region_list[i, region]]] <- df
        df <- NULL
        },
             error=function(e){
                 cat("ERROR reading wfs.csv from", wfs_list[i, region],":\n",
                     conditionMessage(e), "\n")})
}

site_list <- rbindlist(site_list)
write.csv(site_list, "build/site_list.csv", row.names = FALSE)

# check Which regions are missing
region_list[!(region %in% unique(site_list[,region])), region]

# test against time-series server

site_list_ts <- list()
for(i in 1:nrow(region_list)){
    tryCatch({
        cat("Getting list of sites from: ", 
            region_list[i, region], " time-series server.\n") 
        df <- build_site_list(region_name = region_list[i, region], 
                                     endpoint = paste0(endpoint_list[region == region_list[i, region], endpoint],"KiWIS/KiWIS/"),
                                     server = endpoint_list[region == region_list[i, region], server_system])
        df[,region := region_list[i, region]]
        site_list_ts[[i]] <- df
        df <- NULL
        cat("Success!! \n\n")
    }, error=function(e){
        cat("ERROR reading sites from", region_list[i, region],": \n",
            conditionMessage(e), "\n\n")})
}

site_list_ts <- rbindlist(site_list_ts, fill = TRUE)
measurement_list <- measurement_list[!is.na(MeasurementName),]
