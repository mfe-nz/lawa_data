# Hawke's Bay tidying

region <- "Hawke's Bay"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

HawkesBay_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(HawkesBay_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

HawkesBay_gw_data <- HawkesBay_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

HawkesBay_gw_data <- HawkesBay_gw_data[,sub_cols, with = FALSE]

HawkesBay_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
HawkesBay_gw_data[, c("Measurement", "Units") := NULL]

HawkesBay_gw_data <- HawkesBay_gw_data[!is.na(Value),]


HawkesBay_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
HawkesBay_gw_data <- HawkesBay_gw_data[!duplicated(HawkesBay_gw_data),]

# HawkesBay_gw_data[duplicated(HawkesBay_gw_data[,grp]), Measurement_w_units := paste0(Measurement_w_units, "_dups")]

# HawkesBay_gw_data[grp == "2585625",]

`Hawke's Bay_gw_wide` <- dcast.data.table(HawkesBay_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(`Hawke's Bay_gw_wide`,
          bucket = "lawa.tidy",
          compress = TRUE)

`HawkesBay_gw_data` <- NULL

          