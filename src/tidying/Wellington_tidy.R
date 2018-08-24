# Wellington tidying

region <- "Wellington"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

Wellington_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(Wellington_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

Wellington_gw_data <- Wellington_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

Wellington_gw_data <- Wellington_gw_data[,sub_cols, with = FALSE]

Wellington_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
Wellington_gw_data[, c("Measurement", "Units") := NULL]

Wellington_gw_data <- Wellington_gw_data[!is.na(Value),]


Wellington_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
Wellington_gw_data <- Wellington_gw_data[!duplicated(Wellington_gw_data),]

Wellington_gw_data[duplicated(Wellington_gw_data[,grp]), Measurement_w_units := paste0(Measurement_w_units, "_dups")]

Wellington_gw_data[grp == "2585625",]

Wellington_gw_wide <- dcast.data.table(Wellington_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(Wellington_gw_wide,
          bucket = "lawa.tidy",
          compress = TRUE)

Wellington_gw_data <- NULL
