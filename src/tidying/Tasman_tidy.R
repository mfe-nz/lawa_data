# Tasman tidying

region <- "Tasman"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

Tasman_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(Tasman_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

Tasman_gw_data <- Tasman_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

Tasman_gw_data <- Tasman_gw_data[,sub_cols, with = FALSE]

Tasman_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
Tasman_gw_data[, c("Measurement", "Units") := NULL]

Tasman_gw_data <- Tasman_gw_data[!is.na(Value),]


Tasman_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
Tasman_gw_data <- Tasman_gw_data[!duplicated(Tasman_gw_data),]

Tasman_gw_data[duplicated(Tasman_gw_data[,grp]), Measurement_w_units := paste0(Measurement_w_units, "_dups")]

Tasman_gw_data[duplicated(Tasman_gw_data[,grp]),][duplicated(Tasman_gw_data[duplicated(Tasman_gw_data[,grp]),grp])]
 
Tasman_gw_data <- Tasman_gw_data[!duplicated(Tasman_gw_data[,grp]),]

Tasman_gw_wide <- dcast.data.table(Tasman_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(Tasman_gw_wide,
          bucket = "lawa.tidy",
          compress = TRUE)

Tasman_gw_data <- NULL
