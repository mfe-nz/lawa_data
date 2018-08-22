# extract timeseries data
source("src/build_measurement_tables.R")

#New script to run query for measurements at each site
# bind with WQ Sample metadata

measurement_list <- fread("build/measurement_list.csv")
region_list[(region %in% unique(measurement_list[,region])), region]

region_it <- "Tasman"
#measurement_list <- measurement_list[extract == TRUE, ]
measurement_list <- measurement_list[region == region_it,]

# configure time period for query
startDate <- "1997-01-01"
endDate <- "2017-12-31"

# un-comment out these lines to sub-set the query for trouble-shooting
#measurement_list <- measurement_list[1:10, ]
#tss_url <- endpoint_list[region == "Hawke's Bay", endpoint]

extract_data <- function(i){
    site <- measurement_list[i,site] #select the site
    
    measurement <- measurement_list[i, MeasurementName] #select the measurement
    message(paste("Requesting", site, measurement))
    
    #build the request
    testrequest <- paste("service=Hilltop&request=GetData&Site=",site,"&Measurement=",measurement,"&From=",startDate,"&To=",endDate,sep="")
    #get the xml data from the server
    tss_url <- endpoint_list[region == measurement_list[i, region], endpoint]
    url<-paste(tss_url, testrequest, sep="?")
    dataxml<-anyXmlParse(url)
    #convert the xml into a dataframe of measurement results
    #with basic error handling
    wqdata<-tryCatch({
        as.data.table(hilltopMeasurement(dataxml))
    }, error=function(err){message(paste("Error retrieving", site, measurement))})  
    wqdata

    ## WQ sample metadata (Hilltop servers only)
    message(paste("Requesting", site, "WQ Sample metadata"))
    
    #get the WQ Sample parameters for the site
    #build the request
    WQSampleRequest <- paste("service=Hilltop&request=GetData&Site=",site,"&Measurement=WQ Sample&From=",startDate,"&To=",endDate,sep="")
    
    #get the xml data from the server
    
    url<-paste(tss_url, WQSampleRequest, sep="?")
    wqdataxml<-anyXmlParse(url)
    # wqdataxml<-xmlParse_fw(url)
    
    ##convert the xml to a dataframe of WQ Sample results
    #with basic error handling added
    wqSampleData<-tryCatch({
        hilltopMeasurementToDF(wqdataxml)
    }, error=function(err){message(paste("Error retrieving", site, "WQ Sample Information"))})
    wqSampleData
    #merge the WQ Sample data with the measurement data with basic error handling.
    if(length(wqSampleData)>0){
        output<-tryCatch({
        merge(wqdata,wqSampleData,by="Time",all.x = TRUE)
    }, error=function(err){message(paste("No WQ Sample information, leaving blank"))})
    return(output)
    } else {
        return(wqdata)
    }
}
    

gq_data <- pbmclapply(X=1:nrow(measurement_list),
                    FUN = extract_data,
                    mc.cores = (detectCores()-1),
                    mc.style = "ETA")

gq_data <- rbindlist(gq_data, fill = TRUE)
gq_data[, region := region_it]

saveRDS(gq_data, paste0("downloads/",region_it, "_gw_data.RDS"), compress = TRUE)

put_object(file = paste0("downloads/",region_it, "_gw_data.RDS"), 
           object = paste0(region_it, "_gw_data"), 
           bucket = "lawa.data")

gq_data <- NULL
