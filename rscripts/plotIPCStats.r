library(reshape2)
library(ggplot2)
library(sqldf)
library(scales)

plotDistributionPerSection <- function() {
  loadDistDataByLang <- function(lang) {
    df <- sqldf(paste0('select "section", count from ipc_distribution
                       where class is null order by "section"'),
                dbname=paste0("ipcStats-", lang, ".sqlite"))
    for (i in 1:nrow(df)) {
      df[i, "lang"] <- lang
    }

    df$perc <- prop.table(df$count) # percentage
    df
  }

  df <- data.frame()
  df <- rbind(df, loadDistDataByLang("de"))
  df <- rbind(df, loadDistDataByLang("en"))
  #df <- rbind(df, loadDistDataByLang("fr"))

  df.m <- melt(df, id.vars=c("section", "lang", "perc"))

  p <- ggplot(df.m, aes(x=section, y=perc, fill=lang))
  p <- p + geom_bar(stat="identity", position='dodge')
  p <- p + scale_y_continuous(labels = percent)
  p <- p + xlab("IPC-Sektion") + ylab("Häufigkeit") +
    scale_fill_discrete(name="Sprache",
                        labels=c("deutsch", "englisch", "französisch"))
  p
}