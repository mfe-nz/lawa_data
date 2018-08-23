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

# check Which regions are missing
region_list[!(region %in% unique(site_list[,region])), region]

# Get time-series server site names in order to debug any measurement list request issues

site_list_ts <- list()
for(i in 1:nrow(region_list)){
    tryCatch({
        cat("Getting list of sites from: ", 
            region_list[i, region], " time-series server.\n") 
        df <- build_site_list(region_name = region_list[i, region], 
                                     endpoint = endpoint_list[region == region_list[i, region], endpoint],
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
colnames(site_list_ts)[colnames(site_list_ts) %in% "site"] <- "councilsiteid"
site_list_ts[region == "Waikato" |
                 region == "Auckland" |
                 region == "Bay of Plenty", councilsiteid := NULL.station_id]
site_list_ts[, ts_site := TRUE]

site_list_merged <- merge(site_list, site_list_ts, all.x = TRUE, by = c("councilsiteid", "region"))
num_sites_ts <- site_list_merged[, sum(ts_site, na.rm = TRUE), by = region]
num_sites_wfs <- site_list[, .N, by = region]

colnames(num_sites_ts)[colnames(num_sites_ts) %in% "V1"] <- "ts_count"
colnames(num_sites_wfs)[colnames(num_sites_wfs) %in% "N"] <- "wfs_count"

site_count <- merge(num_sites_ts, num_sites_wfs, by = "region")
site_count[!(region %in% df_count[(ts_count == wfs_count),region]),]

write.csv(site_list_merged, "build/site_list.csv", row.names = FALSE)
