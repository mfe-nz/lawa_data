# Reporting
source("src/helpers.R")

# read_data <- function(item, bucketName){
#     s3readRDS(object = item,
#                 bucket = bucketName)
# }
# 
# lapply(object_list$Key, read_data, bucketName = "lawa.tidy")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

summary_list <- list()
for (i in 1:nrow(object_list)){
    cat("getting", object_list[i,Key],"\n\n")
    
    df <- s3readRDS(object_list[i,Key],bucket = "lawa.data")
    
    if(nrow(df) < 1){
        cat("no data for", object_list[i,Key],"\n\n")
    } else {
        sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")
        
        df <- df[,sub_cols, with = FALSE]
        df[,Measurement_w_units := paste0(Measurement, "_", Units)]
        df[, c("Measurement", "Units") := NULL]
        
        df <- df[!is.na(Value),]
        df <- df[!duplicated(df),]
        
        df <- df[, .(measurement = gsub("_.*", "", Measurement_w_units),
                 units = gsub(".*_", "", Measurement_w_units),
                 records = .N,
                 start_date = min(as.Date(Time)),
                 end_date = max(as.Date(Time))),
             by = list(Measurement_w_units)]
        
        df[, region := gsub("_.*", "", object_list[i,Key])]
        
        summary_list[[i]] <- df
        
        df <- NULL
        cat(object_list[i,Key], "summarised \n\n")
    }
}

gw_data_summary <- rbindlist(summary_list)

write.csv(x = gw_data_summary,
          file = "gw_data_summary.csv",
          row.names = FALSE)

put_object(file = "gw_data_summary.csv",
           bucket = "lawa.data")