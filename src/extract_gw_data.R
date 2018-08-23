# extract timeseries data
source("src/build_measurement_tables.R")

#New script to run query for measurements at each site
# bind with WQ Sample metadata

measurement_list <- fread("build/measurement_list.csv")
region_list[(region %in% unique(measurement_list[,region])), region]

# configure time period for query
startDate <- "1997-01-01"
endDate <- "2017-12-31"

start_time <- Sys.time()
for(i in 1:nrow(region_list)){
    region_it <- region_list[i, region]
    cat("Extracting data from ", region_it,": \n\n")
    
    measurement_list_it <- measurement_list[region == region_it,]
    
    gq_data <- pbmclapply(X=1:nrow(measurement_list_it),
                    FUN = extract_data,
                    mc.cores = (detectCores()-1),
                    mc.style = "ETA")
    
    gq_data <- rbindlist(gq_data, fill = TRUE)
    gq_data[, region := region_it]
    
    cat("Saving data from ", region_it,": \n\n")
    saveRDS(gq_data, paste0("downloads/",region_it, "_gw_data.RDS"), compress = TRUE)
    
    put_object(file = paste0("downloads/",region_it, "_gw_data.RDS"), 
           object = paste0(region_it, "_gw_data"), 
           bucket = "lawa.data")
    
    region_it <- NULL
    measurement_list_it <- NULL
    gq_data <- NULL
    cat("Success!! \n\n\n")
}
end_time <- Sys.time()

elapsed_time <- end_time - start_time