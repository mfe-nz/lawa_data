# build measurement list

source("src/build_site_tables.R")

site_list <- fread("build/site_list.csv")
endpoint_list <- fread("input/endpoint_list.csv")
gw_quality_variables <- fread("input/gw_quality_variables.csv")

# Comment this out if you want to work with more than one council
site_list <- site_list[region == "West Coast",]

# # switch siteid and councilciteid for Taranaki
df <- site_list[region == "Taranaki", councilsiteid_temp := councilsiteid]
 df[region == "Taranaki", councilsiteid := siteid]
 df[region == "Taranaki", siteid := councilsiteid_temp]
 df[, councilsiteid_temp := NULL]
site_list <- df


measurement_list <- list()
for(i in 1:nrow(site_list)){
    tryCatch({
        cat("Getting list of measurements from: ", 
            site_list[i, region], 
            site_list[i, councilsiteid], "\n\n")
        df <- build_measurement_list(region_name = site_list[i, region], 
                             site = site_list[i, councilsiteid],
                             endpoint = endpoint_list[region == site_list[i, region], endpoint],
                             server = endpoint_list[region == site_list[i, region], server_system])
        df[,site := site_list[i, councilsiteid]]
        df[,region := site_list[i, region]]
        measurement_list[[i]] <- df
        df <- NULL
        cat("Success!! \n\n")
        }, error=function(e){
            cat("ERROR reading measurements from", site_list[i, region], 
                site_list[i, councilsiteid], ":\n",
                conditionMessage(e), "\n")})
}

measurement_list <- rbindlist(measurement_list, fill = TRUE)
measurement_list <- measurement_list[!is.na(MeasurementName),]

# check Which regions are missing
region_list[!(region %in% unique(measurement_list[,region])), region]

# View(unique(measurement_list[,MeasurementName]))


# measurement_list[MeasurementName %in% gw_quality_variables[keep == TRUE,measurement], extract := TRUE]
#extract measurements
#identify full range of possible measurements for each site
#select GW specific measuremets
#Ensure correct name for measurements makes sense (common name)
write.csv(measurement_list, "build/measurement_list.csv", row.names = FALSE)
write.csv(unique(measurement_list[, MeasurementName, by = region]), "build/measurement_names.csv", row.names = FALSE)


put_object(file = "build/measurement_names.csv", 
           object = "Measurement names", 
           bucket = "lawa.data")

