# Reporting
source("src/get_input_data.R")

# read_data <- function(item, bucketName){
#     s3readRDS(object = item,
#                 bucket = bucketName)
# }
# 
# lapply(object_list$Key, read_data, bucketName = "lawa.tidy")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.tidy"))


for (i in 1:nrow(object_list)){
    cat("getting", object_list[i,Key],"\n\n")
    
    df <- s3readRDS(object_list[i,Key],bucket = "lawa.tidy")
    
    if(nrow(df) < 1){
        cat("no data for", object_list[i,Key],"\n\n")
    } else {
        sub_cols <- c("Time",
                      "Site",
                      parameter_list[region == gsub("_.*", "", object_list[i,Key]) &
                                       !is.na(`For ER`), Measurement_w_units])
        
        df <- df[,sub_cols, with = FALSE]
        df <- unique(df)
        df[, region := gsub("_.*", "", object_list[i,Key])]
        
        write.csv(x = df,
                  file = paste0(gsub("_.*", "", object_list[i,Key], "_tidy.csv")),
                  row.names = FALSE)
        
        put_object(file = paste0(gsub("_.*", "", object_list[i,Key], "_tidy.csv")),
                   bucket = "lawa.eyeball")
        
        df <- NULL
        
        file.remove(paste0(gsub("_.*", "", object_list[i,Key], "_tidy.csv")))
                    
        cat(object_list[i,Key], "summarised \n\n")
    }
}


