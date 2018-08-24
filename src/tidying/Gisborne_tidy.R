# Gisborne tidying

region <- "Gisborne"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

Gisborne_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(Gisborne_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

Gisborne_gw_data <- Gisborne_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

Gisborne_gw_data <- Gisborne_gw_data[,sub_cols, with = FALSE]

Gisborne_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
Gisborne_gw_data[, c("Measurement", "Units") := NULL]

Gisborne_gw_data <- Gisborne_gw_data[!is.na(Value),]


Gisborne_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
Gisborne_gw_data <- Gisborne_gw_data[!duplicated(Gisborne_gw_data),]

# Gisborne_gw_data[duplicated(Gisborne_gw_data[,grp]), Measurement_w_units := paste0(Measurement_w_units, "_dups")]

# Gisborne_gw_data[grp == "2585625",]

Gisborne_gw_wide <- dcast.data.table(Gisborne_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(Gisborne_gw_wide,
          bucket = "lawa.tidy",
          compress = TRUE)

Gisborne_gw_data <- NULL
