# download input data from AWS object store
# (to keep urls out of repo)
source("src/helpers.R")

object_list <- get_bucket_df(bucket = "lawa.input")

download_input <- function(item){
    save_object(object = item,
               bucket = "lawa.input",
               file = paste0("input/", item))
}

lapply(object_list$Key, download_input)

list.files("input/")

# read in input tables
region_list <- fread("input/agency_list.csv")
region_list[, downloaded := FALSE]
region_list[ , wfs_file := paste0(gsub(" |'", "_", tolower(region)),"_wfs")]

endpoint_list <- fread("input/endpoint_list.csv")
gw_quality_variables <- fread("input/gw_quality_variables.csv")

parameter_list <- fread("input/parameter_list.csv", na.strings = "")
