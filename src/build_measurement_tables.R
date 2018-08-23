# build measurement list

source("src/build_site_tables.R")

site_list <- fread("build/site_list.csv")

# Comment this out if you want to work with more than one council
# site_list <- site_list[region == "Auckland",]

# # switch siteid and councilciteid for Taranaki
# df <- site_list[region == "Taranaki", councilsiteid_temp := councilsiteid]
#  df[region == "Taranaki", councilsiteid := siteid]
#  df[region == "Taranaki", siteid := councilsiteid_temp]
#  df[, councilsiteid_temp := NULL]
# site_list <- df

measurement_list <- pbmclapply(X=1:nrow(site_list),
                      FUN = extract_measurements,
                      mc.cores = (detectCores()-1),
                      mc.style = "ETA")

measurement_list <- rbindlist(measurement_list, fill = TRUE)

measurement_list[is.na(MeasurementName) &
                     region == "Waikato", MeasurementName := NULL.parametertype_name]

measurement_list[is.na(MeasurementName) &
                     !(region == "Waikato"), MeasurementName := datasource]

# check Which regions are missing
region_list[!(region %in% unique(measurement_list[,region])), region]

# View(unique(measurement_list[,MeasurementName]))


# measurement_list[MeasurementName %in% gw_quality_variables[keep == TRUE,measurement], extract := TRUE]
#extract measurements
#identify full range of possible measurements for each site
#select GW specific measuremets
#Ensure correct name for measurements makes sense (common name)
write.csv(measurement_list, "build/measurement_list.csv", row.names = FALSE)
table(unique(measurement_list[, MeasurementName, by = region])$region)

write.csv(unique(measurement_list[, MeasurementName, by = region]), "build/measurement_names.csv", row.names = FALSE)


# put_object(file = "build/measurement_names.csv", 
#            object = "Measurement names", 
#            bucket = "lawa.data")

