# Marlborough tidying

region <- "Marlborough"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

Marlborough_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(Marlborough_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

Marlborough_gw_data <- Marlborough_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

Marlborough_gw_data <- Marlborough_gw_data[,sub_cols, with = FALSE]

Marlborough_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
Marlborough_gw_data[, c("Measurement", "Units") := NULL]

Marlborough_gw_data <- Marlborough_gw_data[!is.na(Value),]


Marlborough_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
Marlborough_gw_data <- Marlborough_gw_data[!duplicated(Marlborough_gw_data),]

Marlborough_gw_data[duplicated(Marlborough_gw_data[,grp]), Measurement_w_units := paste0(Measurement_w_units, "_dups")]

Marlborough_gw_data[duplicated(Marlborough_gw_data[,grp]),][duplicated(Marlborough_gw_data[duplicated(Marlborough_gw_data[,grp]),grp])]

Marlborough_gw_data[duplicated(Marlborough_gw_data[,grp]),][duplicated(Marlborough_gw_data[duplicated(Marlborough_gw_data[,grp]),grp])]$Measurement_w_units <- "2_2_dups_dups"

Marlborough_gw_wide <- dcast.data.table(Marlborough_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(Marlborough_gw_wide,
          bucket = "lawa.tidy",
          compress = TRUE)

Marlborough_gw_data <- NULL