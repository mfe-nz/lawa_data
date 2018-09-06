# helpers.R

#Install the required packages
pkgs <- c('XML',
          'reshape2',
          'plyr',
          'RCurl',
          'data.table',
          'readxl',
          'aws.s3',
          'pbmcapply',
          'ggplot2',
          'scales')

if(!all(pkgs %in% installed.packages()[, 'Package']))
    install.packages(pkgs[!(pkgs %in% installed.packages()[, 'Package'])], dep = T)

lapply(pkgs, require, character.only = TRUE)

source("src/Hilltop.R")

download_wfs <- function(url, region){
    if(!file.exists(paste0("downloads/", region, "_wfs.csv"))){
        cat("Downloading ", region, " WFS from ", url,"\n\n")
    
        # Dealing with https:
        if(substr(url,start = 1,stop = 5)=="http:"){
            str<- tempfile(pattern = "file", tmpdir = tempdir())
            #(download.file(url,destfile = str,method="wininet",mode = "wb"))
            (download.file(url,destfile = str))
            getSites.xml <- try(xmlInternalTreeParse(file = str),silent=TRUE)
            unlink(str)
        } else {
            tf <- getURL(url, ssl.verifypeer = FALSE)
            getSites.xml <- xmlParse(tf)
        }
        ds <- as.data.table(
                xmlToDataFrame(
                    getNodeSet(
                        getSites.xml, 
                        "//MonitoringSiteReferenceData|
                        //emar:MonitoringSiteReferenceData")))
        
        # add region information
        ds[, region_name := region]
    
        colnames(ds) <- tolower(colnames(ds))
        
        # format Shape field
        # Assumption is that gml:pos has coordinates recorded in lat,lon order
        ds[grepl(" ", shape), c("lat", "long") := tstrsplit(shape, " ", fixed = TRUE)]
        ds[grepl(",", shape), c("lat", "long") := tstrsplit(shape, ",", fixed = TRUE)]
    
        write.csv(ds, 
                  file = paste0("downloads/", region, "_wfs.csv"), 
                  row.names = FALSE)
    
        cat("Success! created: ", paste0("downloads/", region, "_wfs.csv"), "\n\n")
    } else {
        cat("File: ", paste0("downloads/", region, "_wfs.csv already downloaded"), "\n\n")
    }
}

read_wfs <- function(region){
    if(!file.exists(paste0("downloads/", region, "_wfs.csv"))){
        cat("WFS.csv from ", region,"not in downloads directory.\n\n")}
    df <- fread(file = paste0("downloads/", region, "_wfs.csv"))
    assign(
        paste0(gsub(" |'", "_", tolower(region)),"_wfs"), value = df,envir = .GlobalEnv)
    df <- NULL
}

build_site_list <- function(region_name, endpoint, server){
    if(server == "hilltop"){
        siteListrequest <- "service=Hilltop&request=SiteList"
        #get the xml data from the server
        url<-paste(endpoint, siteListrequest, sep="?")
        dataxml<-anyXmlParse(url)
        
        df<-tryCatch({
            as.data.table(hilltopSiteList(dataxml))
        }, error=function(err){message(paste("Error retrieving sites for: ", region_name))})
        return(df)
    }
    if(server == "kisters"){
        siteListrequest <- "service=kisters&type=queryServices&request=getStationList&datasource=0&format=html"
        #get the xml data from the server
        url<-paste(endpoint, siteListrequest, sep="?")
        datahtml <- readHTMLTable(url)
        
        df<-tryCatch({
            as.data.table(datahtml)
        }, error=function(err){message(paste("Error retrieving sites for: ", region_name))})
        return(df)
        }
}

build_measurement_list <- function(region_name, site, endpoint, server){
    if(server == "hilltop"){
        measurementListrequest <- paste0("service=Hilltop&request=MeasurementList&Site=",site)
        #get the xml data from the server
        url<-paste(endpoint, measurementListrequest, sep="?")
        dataxml<-anyXmlParse(url)
        
        df<-tryCatch({
            as.data.table(hilltopDsMeasList(dataxml))
            }, error=function(err){message(paste("Error retrieving measurements for: ", site))})
        return(df)
        }
    if(server == "kisters"){
        measurementListrequest <- paste0("service=kisters&type=queryServices&request=getParameterList&datasource=0&format=html&station_id=",site)
        #get the xml data from the server
        url<-paste(endpoint, measurementListrequest, sep="?")
        datahtml <- readHTMLTable(url)

        df<-tryCatch({
            as.data.table(datahtml)
        }, error=function(err){message(paste("Error retrieving measurements for: ", site))})
        return(df)
    }
}      

build_getData_list <- function(site, measurement, server){
    if(server == "hilltop"){
        #build the request
        testrequest <- paste("service=Hilltop&request=GetData&Site=",site,"&Measurement=",measurement,"&From=",startDate,"&To=",endDate,sep="")
        #get the xml data from the server
        tss_url <- endpoint_list[region == measurement_list_it[i, region], endpoint]
        url<-paste(tss_url, testrequest, sep="?")
        dataxml<-anyXmlParse(url)
        #convert the xml into a dataframe of measurement results
        #with basic error handling
        wqdata<-tryCatch({
            as.data.table(hilltopMeasurement(dataxml))
            }, error=function(err){message(paste("Error retrieving", site, measurement))})  
        
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
    if(server == "kisters"){
        #build the request
        testrequest <- paste("datasource=0&service=SOS&version=2.0&request=GetObservation&featureOfInterest=",site,"&procedure=GWLawa.Sample.Results.P&observedProperty=",measurement,"&From=",startDate,"&To=",endDate,sep="")
        #get the xml data from the server
        tss_url <- endpoint_list[region == measurement_list_it[i, region], endpoint]
        url<-paste(tss_url, testrequest, sep="?")
        
        wqdataxml <- anyXmlParse(url)
        
        ##convert the xml to a dataframe of WQ Sample results
        #with basic error handling added
        wqSampleData<-tryCatch({
            df[2] <- xml2::read_xml(url)
        }, error=function(err){message(paste("Error retrieving", site, "WQ Sample Information"))})
        
        df<-tryCatch({
            as.data.table(datahtml)
        }, error=function(err){message(paste("Error retrieving measurements for: ", site))})
        return(df)
    }
}

#xmlParse function that works behind MFE Firewall
xmlParse_fw <- function(url){
    str<- tempfile(pattern = "file", tmpdir = tempdir())
    (download.file(url,destfile=str,method="wininet"))
    xmlfile <- try(xmlParse(file = str),silent=TRUE)
    if(attr(xmlfile,"class")[1]=="try-error"){
        xmlfile <- FALSE
    }
    unlink(str)
    return(xmlfile)
}

extract_measurements <- function(i){
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
        cat("Success!! \n\n")
        return(df)
    }, error=function(e){
        cat("ERROR reading measurements from", site_list[i, region], 
            site_list[i, councilsiteid], ":\n",
            conditionMessage(e), "\n")})
}

extract_data <- function(i){
    tryCatch({
        cat("Getting data from: ",
            measurement_list_it[i, region], "\n\n")
        
        df <- build_getData_list(site = measurement_list_it[i,site],
                             measurement = measurement_list_it[i,MeasurementName],
                             server = endpoint_list[region == measurement_list_it[i,region], server_system])
        df[,region := measurement_list_it[i, region]]
        cat("Success!! \n\n")
        return(df)
        }, error=function(e){
            cat("ERROR extracting data from", 
                measurement_list_it[i, region], ":\n",
                conditionMessage(e), "\n")})
}

download_data <- function(item){
    save_object(object = item,
                bucket = "lawa.data",
                file = paste0("data/", item, ".RDS"))
}