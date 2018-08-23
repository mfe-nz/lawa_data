# helpers.R

#Install the required packages
pkgs <- c('XML',
          'reshape2',
          'plyr',
          'RCurl',
          'data.table',
          'readxl',
          'aws.s3',
          'pbmcapply')

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

# ?
# ?

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
