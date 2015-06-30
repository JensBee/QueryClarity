library(ggplot2)
library(grid)
library(sqldf)
library(reshape2)
library(optparse)
library(scales)

# configuration
cfg = new.env()
# fields indexed
cfg$fields <- list("claims", "detd")
# languages indexed
cfg$lang <- list("de", "en", "fr")
# IPC section names
cfg$ipc <- list("A", "B", "C", "D", "E", "F", "G", "H")
# known scorer names
cfg$scorer <- list("DCS", "ICS", "SCS")

# commandline parser
cmdOpts <- list(
  make_option(c("-s", "--source"), action="store", metavar="<PATH>",
              type="character", default=".",
              help="Root path to scoring databases."),
  make_option(c("-t", "--target"), action="store", metavar="<PATH>",
              type="character", default=".",
              help="Target path to store plots.")
)
opt <- parse_args(OptionParser(option_list=cmdOpts))

plot_lang <- function(lang) {
  filePattern <- paste0("^termDumpSample-", lang, "-.*\\.sqlite$")
  fileTerms <- paste0("termDump-", lang, ".sqlite")
  
  scale_count <- function(df) {
    # add scaled score column
    df <- transform(df, scount=rescale(count, to=c(0, 1)))
    df
  }
  
  load_lang_data <- function(dbFile, field, ipc) {
    query <- "select field, docfreq_rel, count(*) as count, "
    if (missing(ipc)) {
      query <- paste0(query, "\"*\" as ipc ")
    } else {
      query <- paste0(query, "ipc ")
    }
    query <- paste0(query, "from terms ")
    if (missing(ipc)) {
      query <- paste0(query, "where ipc is null ")
    } else {
      query <- paste0(query, "where ipc=\"", ipc, "\" ")
    }
    query <- paste0(query, "and field=\"", field, "\" group by docfreq_rel order by docfreq_rel asc")
    sqldf(query, dbname=dbFile)
  }
  
  load_segment_data <- function() {
    query <- "select *, section as ipc from term_segments"
    
    df <- data.frame()
    for (dbFile in list.files(
      path=paste0(opt$source, "/termDumpSamples"),
      pattern=filePattern)) {
      dbFile <- paste0(opt$source, "/termDumpSamples/", dbFile)
      # get the plain file name
      dbFileName <- basename(dbFile)
      filePrefix <- gsub("\\.sqlite$", "", dbFileName)
      print(paste0("Open '", dbFile, "'.."))
      df <- rbind(sqldf(query, dbname=dbFile))
    }
    
    df[is.na(df)] <- "*"
    df
  }
  
  plot_base <- function(df, dfseg) {
    df.m <- melt(df, id.vars=c("docfreq_rel", "field", "ipc"), measure.vars=c("scount"))
    #dfseg.m <- melt(df, id.vars=c("field", "ipc"), measure.vars=c("docfreq_rel_min", "docfreq_rel_max"))

    p <- ggplot(df.m, aes(x=docfreq_rel, y=value))
    p <- p + geom_step()
    p <- p + scale_x_log10() + scale_y_sqrt()
    p <- p + theme_bw() + theme(
      panel.grid.major=element_line(colour="#cccccc"),
      panel.grid.minor=element_line(colour="#bbbbbb", linetype="dotted"),
      strip.background=element_rect(fill="#eeeeee"),
      plot.margin = unit(c(0, 0, 0, 0), "lines")
    )
    #p <- p + geom_rect(data=dfseg.m, aes(
    #  xmin=docfreq_rel_min, xmax=docfreq_rel_max,
    #  ymin=-Inf, ymax=Inf))
    p
  }
  
  if (file.exists(fileTerms)) {
    # -- START base data
    print(paste0("Open '", fileTerms, "'.."))
    
    df <- data.frame()
    df.seg <- load_segment_data()
    
    for (field in cfg$fields) {
      df <- rbind(df, scale_count(load_lang_data(fileTerms, field)))
      for (ipc in cfg$ipc) {
        df <- rbind(df, scale_count(load_lang_data(fileTerms, field, ipc)))
      }
    }
    View(df)
    View(df.seg)
    print("Plot..")
    p <- plot_base(df, df.seg)
    p <- p + facet_grid(ipc ~ field)
    print(p)
  } else {
    print(paste0("Missing termDump for lang=",lang,"."))
  }
}

plot_lang("fr")
#for (lang in cfg$lang) {
#  plot_lang(lang)
#}