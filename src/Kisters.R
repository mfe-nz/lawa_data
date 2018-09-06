# Kisters.R

kistersMeasurementToDF <- function(dataxml) {
    #Helper function that reads the nodes within a the Measurement node of a Kisters XML response
    #from a anyXmlParse(url) request such as dataxml<-anyXmlParse(url).
    #Returns a dataframe of the data for each timestamp.
    #Handles missing results and doen't require prior knowledge of parameter names.
    #Handles true measurements and WQ Sample requests
    # idNodes <- getNodeSet(dataxml, "//wml2:point/wml2:MeasurementTVP")
    times <- sapply(getNodeSet(doc=dataxml, "//wml2:time"), xmlValue)
    values <- sapply(getNodeSet(doc=dataxml, "//wml2:value"), xmlValue)
    idNodes <- getNodeSet(dataxml, "//wml2:MeasurementTVP")
    attributes <- lapply(idNodes, xpathApply, path = "./*", kistersAttributeHelper)
    data <- do.call(rbind.data.frame, Reduce(function(x,y) Map(cbind, x, y), list(times, attributes, values)))
    names(data) <- c("Time", "Attribute", "Content")
    data <- data[!(data$Attribute == "time"), ]
    data <- data.frame(lapply(data, as.character), stringsAsFactors = FALSE)
    cdata <- dcast(data, Time ~ Attribute, value.var = "Content")
    cdata$Time <- as.POSIXct(strptime(cdata$Time, format = "%Y-%m-%dT%H:%M:%S"))
    colnames(cdata)[colnames(cdata)=="I1"] <- "Value"
    return(cdata)
}

kistersAttributeHelper <- function(x) {
    #Helper function to return the appropriate xml attribute name depending whether the attribute of interest is from a named node,
    #or is a named parameter.
    if(xmlName(x) != "wml2:time") {
        if(xmlName(x) == "Parameter") {
            return(xmlGetAttr(x, "Name"))
        } else {return(xmlName(x)) }
    }
}

