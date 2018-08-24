# Southland tidying

region <- "Southland"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

Southland_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(Southland_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

Southland_gw_data <- Southland_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

Southland_gw_data <- Southland_gw_data[,sub_cols, with = FALSE]

Southland_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
Southland_gw_data[, c("Measurement", "Units") := NULL]

Southland_gw_data <- Southland_gw_data[!is.na(Value),]


Southland_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
Southland_gw_data[duplicated(Southland_gw_data[,grp]), Measurement_w_units := paste(Measurement_w_units, "_dup")]

Southland_gw_data[grp == "9548531",]

Southland_gw_wide <- dcast.data.table(Southland_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(Southland_gw_wide,
          bucket = "lawa.tidy",
          compress = TRUE)
