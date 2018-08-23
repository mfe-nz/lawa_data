# Reporting
source("src/helpers.R")

object_list <- get_bucket_df(bucket = "lawa.data")

download_data <- function(item){
    save_object(object = item,
                bucket = "lawa.data",
                file = paste0("data/", item, ".RDS"))
}

lapply(object_list$Key, download_input)

list.files("data/")
