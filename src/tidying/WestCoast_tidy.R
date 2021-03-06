# West Coast tidying

region <- "West Coast"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

WestCoast_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(WestCoast_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

WestCoast_gw_data <- WestCoast_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

WestCoast_gw_data <- WestCoast_gw_data[,sub_cols, with = FALSE]

WestCoast_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
WestCoast_gw_data[, c("Measurement", "Units") := NULL]

WestCoast_gw_data <- WestCoast_gw_data[!is.na(Value),]


WestCoast_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
# WestCoast_gw_data <- WestCoast_gw_data[!duplicated(WestCoast_gw_data),]
# 
# WestCoast_gw_data[duplicated(WestCoast_gw_data[,grp]), Measurement_w_units := paste0(Measurement_w_units, "_dups")]
# 
# WestCoast_gw_data[duplicated(WestCoast_gw_data[,grp]),][duplicated(WestCoast_gw_data[duplicated(WestCoast_gw_data[,grp]),grp])]
# 
# WestCoast_gw_data[duplicated(WestCoast_gw_data[,grp]),][duplicated(WestCoast_gw_data[duplicated(WestCoast_gw_data[,grp]),grp])]$Measurement_w_units <- "2_2_dups_dups"

`West Coast_gw_wide` <- dcast.data.table(WestCoast_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(`West Coast_gw_wide`,
          bucket = "lawa.tidy",
          compress = TRUE)

`West Coast_gw_data` <- NULL
