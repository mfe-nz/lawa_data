# Northland tidying

region <- "Northland"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

Northland_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(Northland_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

Northland_gw_data <- Northland_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

Northland_gw_data <- Northland_gw_data[,sub_cols, with = FALSE]

Northland_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
Northland_gw_data[, c("Measurement", "Units") := NULL]

Northland_gw_data <- Northland_gw_data[!is.na(Value),]


Northland_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
Northland_gw_data <- Northland_gw_data[!duplicated(Northland_gw_data),]

Northland_gw_data[duplicated(Northland_gw_data[,grp]), Measurement_w_units := paste0(Measurement_w_units, "_dups")]

# Northland_gw_data[duplicated(Northland_gw_data[,grp]),][duplicated(Northland_gw_data[duplicated(Northland_gw_data[,grp]),grp])]
# 
# Northland_gw_data[duplicated(Northland_gw_data[,grp]),][duplicated(Northland_gw_data[duplicated(Northland_gw_data[,grp]),grp])]$Measurement_w_units <- "2_2_dups_dups"

Northland_gw_wide <- dcast.data.table(Northland_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(Northland_gw_wide,
          bucket = "lawa.tidy",
          compress = TRUE)

Northland_gw_data <- NULL
