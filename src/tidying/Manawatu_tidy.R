# Manawatu tidying

region <- "Manawatu"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

Manawatu_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(Manawatu_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

Manawatu_gw_data <- Manawatu_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

Manawatu_gw_data <- Manawatu_gw_data[,sub_cols, with = FALSE]

Manawatu_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
Manawatu_gw_data[, c("Measurement", "Units") := NULL]

Manawatu_gw_data <- Manawatu_gw_data[!is.na(Value),]


Manawatu_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
# Manawatu_gw_data <- Manawatu_gw_data[!duplicated(Manawatu_gw_data),]

# Manawatu_gw_data[duplicated(Manawatu_gw_data[,grp]), Measurement_w_units := paste0(Measurement_w_units, "_dups")]

# Manawatu_gw_data[grp == "2585625",]

Manawatu_gw_wide <- dcast.data.table(Manawatu_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(Manawatu_gw_wide,
          bucket = "lawa.tidy",
          compress = TRUE)

Manawatu_gw_data <- NULL

          