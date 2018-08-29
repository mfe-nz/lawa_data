# Reporting
source("src/helpers.R")

# read_data <- function(item, bucketName){
#     s3readRDS(object = item,
#                 bucket = bucketName)
# }
# 
# lapply(object_list$Key, read_data, bucketName = "lawa.tidy")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

gw_data_summary <- list()
gw_site_summary <- list()

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
        
        df_data_summary <- df[, .(measurement = gsub("_.*", "", Measurement_w_units),
                 units = gsub(".*_", "", Measurement_w_units),
                 records = .N,
                 start_date = min(as.Date(Time)),
                 end_date = max(as.Date(Time))),
             by = list(Measurement_w_units)]
        
        df_data_summary[, region := gsub("_.*", "", object_list[i,Key])]
        
        gw_data_summary[[i]] <- df_data_summary
        
        df_site_summary <- df[, .(records = .N,
                                  start_date = min(as.Date(Time)),
                                  end_date = max(as.Date(Time))),
                              by = list(Site)]
        
        df_site_summary[, region := gsub("_.*", "", object_list[i,Key])]
        
        gw_site_summary[[i]] <- df_site_summary
        
        df <- NULL
        cat(object_list[i,Key], "summarised \n\n")
    }
}

gw_data_summary <- rbindlist(gw_data_summary)
gw_site_summary <- rbindlist(gw_site_summary)

write.csv(x = gw_data_summary,
          file = "gw_data_summary.csv",
          row.names = FALSE)

write.csv(x = gw_site_summary,
          file = "gw_site_summary.csv",
          row.names = FALSE)

put_object(file = "gw_data_summary.csv",
           bucket = "lawa.data")

put_object(file = "gw_site_summary.csv",
           bucket = "lawa.data")
