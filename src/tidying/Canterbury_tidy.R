# Canterbury tidying

region <- "Canterbury"
source("src/helpers.R")

object_list <- as.data.table(get_bucket_df(bucket = "lawa.data"))

Canterbury_gw_data <- s3readRDS(object_list[grepl(region, Key),Key],
          bucket = "lawa.data")

full_cols <- colnames(Canterbury_gw_data)
full_cols <- full_cols[!grepl("\\.x|\\.y", full_cols)]

Canterbury_gw_data <- Canterbury_gw_data[,full_cols, with = FALSE]

sub_cols <- c("Time",
              "Value",
              "Site",
              "Measurement",
              "Units")

Canterbury_gw_data <- Canterbury_gw_data[,sub_cols, with = FALSE]

Canterbury_gw_data[,Measurement_w_units := paste0(Measurement, "_", Units)]
Canterbury_gw_data[, c("Measurement", "Units") := NULL]

Canterbury_gw_data <- Canterbury_gw_data[!is.na(Value),]


Canterbury_gw_data[, grp := .GRP, by = .(Time, Site, Measurement_w_units)]
Canterbury_gw_data <- Canterbury_gw_data[!duplicated(Canterbury_gw_data),]

Canterbury_gw_data[duplicated(Canterbury_gw_data[,grp]), Measurement_w_units := paste0(Measurement_w_units, "_dups")]

# Canterbury_gw_data[grp == "2585625",]

Canterbury_gw_wide <- dcast.data.table(Canterbury_gw_data, 
                                      Time + Site ~ Measurement_w_units,
                                      fun.aggregate = NULL,
                                      value.var = "Value")

s3saveRDS(Canterbury_gw_wide,
          bucket = "lawa.tidy",
          compress = TRUE)

Canterbury_gw_data <- NULL
