library(reshape2)
library(ggplot2)
library(sqldf)
library(scales)
library(optparse)

cmdOpts <- list(
  make_option(c("-s", "--source"), action="store", metavar="<PATH>",
              type="character", default=".",
              help="Source path where databases are stored."),
  make_option(c("-t", "--target"), action="store", metavar="<PATH>",
              type="character", default=".",
              help="Target path to store plots.")
)
opt <- parse_args(OptionParser(option_list=cmdOpts))

# Distribution of IPC-Sections across all documents
plotDistributionPerSection <- function() {
  loadDataByLang <- function(lang) {
    df <- sqldf(paste0('select "section", "count" ',
                       'from "ipc_distribution" ',
                       'where "class" is null order by "section"'),
                dbname=paste0(opt$source, "/ipcStats-", lang, ".sqlite"))
    for (i in 1:nrow(df)) {
      df[i, "lang"] <- lang
    }

    df$perc <- prop.table(df$count) # percentage
    df
  }

  df <- data.frame()
  df <- rbind(df, loadDataByLang("de"))
  df <- rbind(df, loadDataByLang("en"))
  df <- rbind(df, loadDataByLang("fr"))

  df.m <- melt(df, id.vars=c("section", "lang", "perc"))

  p <- ggplot(df.m, aes(x=section, y=perc, fill=lang))
  p <- p + geom_bar(stat="identity", position='dodge')
  p <- p + scale_y_continuous(labels = percent)
  p <- p + xlab("IPC-Sektion") + ylab("Häufigkeit") +
    scale_fill_discrete(name="Sprache",
                        labels=c("deutsch", "englisch", "französisch"))
  ggsave(p, file=paste0(opt$target, "/ipcStats-section_dist.pdf"))
}

# Number of IPC-Sections assigned per document
plotNumberOfSections <- function() {
  loadDataByLang <- function(lang) {
    df <- sqldf(paste0('select "sections", "count" ',
                       'from "ipc_sections" ',
                       'order by "sections"'),
                dbname=paste0(opt$source, "/ipcStats-", lang, ".sqlite"))
    for (i in 1:nrow(df)) {
      df[i, "lang"] <- lang
    }
    
    df$perc <- prop.table(df$count) # percentage
    df
  }
  
  df <- data.frame()
  df <- rbind(df, loadDataByLang("de"))
  df <- rbind(df, loadDataByLang("en"))
  df <- rbind(df, loadDataByLang("fr"))
  
  df.m <- melt(df, id.vars=c("sections", "lang", "perc"))
  
  p <- ggplot(df.m, aes(x=sections, y=perc, fill=lang))
  p <- p + geom_bar(stat="identity", position='dodge')
  #p <- p + scale_y_continuous(labels = percent)
  p <- p + scale_y_sqrt(labels = percent)
  p <- p + xlab("Anzahl diverser IPC-Sektionen") + ylab("Häufigkeit") +
    scale_x_continuous(breaks=0:7) +
    scale_fill_discrete(name="Sprache",
                        labels=c("deutsch", "englisch", "französisch"))
  ggsave(p, file=paste0(opt$target, "/ipcStats-section_divcount.pdf"))
}

main <- function() {
  print("Plotting distribution per IPC-Section..")
  plotDistributionPerSection()
  
  print("Plotting number of IPC-Sections per document..")
  plotNumberOfSections()
}

main()