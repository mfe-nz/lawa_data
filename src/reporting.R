# Reporting
source("src/helpers.R")

object_list <- get_bucket_df(bucket = "lawa.data")

download_data <- function(item){
    save_object(object = item,
                bucket = "lawa.data",
                file = paste0("data/", item, ".RDS"))
}

lapply(object_list$Key, download_data)

data_files <- list.files("data/")

# variable “day” that is in as.Date type)

df <- df[, .(max_day = max(day), min_day = min(day)), by =site]
df[, duration := as.numeric(max_day - min_day)]

dff_plot <- ggplot() +
    geom_segment(data = df, aes(x = min_day, y = site, xend = max_day, yend = site, col= site), size = 2)+
    scale_x_date( breaks=pretty_breaks()) + 
    xlab('Time') + 
    theme(axis.text.x = element_text(angle=90, vjust = 0.5),
          legend.position = NULL)


