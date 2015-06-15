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
# regex pattern to find source databases
cfg$srcFilePattern <- "^termDumpSample-.*\\.sqlite$"

# commandline parser
cmdOpts <- list(
  make_option(c("-s", "--source"), action="store", metavar="<PATH>",
              type="character", default=".",
              help="Path to scoring databases."),
  make_option(c("-t", "--target"), action="store", metavar="<PATH>",
              type="character", default=".",
              help="Target path to store plots.")
)
opt <- parse_args(OptionParser(option_list=cmdOpts))

# scale scoring result values to 0-1
score_scale <- function(df) {
  # add scaled score column
  df <- transform(df, sscore=rescale(score, to=c(0, 1)))
  df
}

# add additional columns to loaded data which are required for plotting
load_finalize <- function(df, # loaded data
                          sField, # source field (term source)
                          tField, # target field (query target)
                          ipc # required ipc code (may be empty)
                          ) {
  # add source & target field
  for (i in 1:nrow(df)) {
    df[i, "src"] <- sField
    df[i, "tgt"] <- tField
    if (missing(ipc)) {
      df[i, "ipc"] <- "*"
    }
  }
  df
}

# load sentence scoring results
sentences_load <- function(dbFile, # database file name
                           scorer, # scorer name
                           sField, # source field (term source)
                           tField, # target field (query target)
                           ipc # required ipc code (may be empty)
                           ) {
  # build query
  query <- "select ssr.impl, ssr.score, "
  if (!missing(ipc)) {
    query <- paste0(query, "ssr.q_ipc as ipc, ")
  }
  query <- paste0(query, "st.bin ",
                  "from scoring_sentence_results ssr ",
                  "inner join scoring_sentences ss on (ssr.sent_ref = ss.id) ",
                  "inner join scoring_terms st on (ss.term_ref = st.id) ",
                  "where ssr.impl=\"", scorer, "\" ",
                  "and ssr.q_fields=\"", tField, "\" ",
                  "and st.field=\"", sField, "\" ")
  if (missing(ipc)) {
    query <- paste0(query, "and ssr.q_ipc is null")
  } else {
    query <- paste0(query, "and ssr.q_ipc=\"", ipc, "\"")
  }
  
  # load data
  df <- load_finalize(sqldf(query, dbname=dbFile),
                      sField, tField, ipc)
  df
}

# load all sentence scoring results for every source- & target-field combination
sentences_load_all <- function(dbFile, # database file name
                               scorer, # scorer name
                               ipc # required ipc code (may be empty)
) {
  df <- data.frame()
  for (f1 in cfg$fields) {
    for (f2 in cfg$fields) {
      df <- rbind(df, sentences_load(dbFile, scorer, f1, f2, ipc))
    }
  }
  df
}


# load term scoring results
terms_load <- function(dbFile, # database file name
                       scorer, # scorer name
                       sField, # source field (term source)
                       tField, # target field (query target)
                       ipc # required ipc code (may be empty)
                       ) {
  # build query
  query <- "select str.impl, str.score, "
  if (!missing(ipc)) {
    query <- paste0(query, "str.q_ipc as ipc, ")
  }
  query <- paste0(query, "st.bin ",
                  "from scoring_terms_results str ",
                  "inner join scoring_terms st on (str.term_ref=st.id) ",
                  "where str.impl = \"", scorer, "\" ",
                  "and str.q_fields=\"", tField, "\" and st.field=\"", sField, "\" ")
  if (missing(ipc)) {
    query <- paste0(query, "and str.q_ipc is null")
  } else {
    query <- paste0(query, "and str.q_ipc=\"", ipc, "\"")
  }
  
  # load data
  df <- load_finalize(sqldf(query, dbname=dbFile),
                      sField, tField, ipc)
  df
}

# load all term scoring results for every source- & target-field combination
terms_load_all <- function(dbFile, # database file name
                           scorer, # scorer name
                           ipc # required ipc code (may be empty)
                           ) {
  df <- data.frame()
  for (f1 in cfg$fields) {
    for (f2 in cfg$fields) {
      df <- rbind(df, terms_load(dbFile, scorer, f1, f2, ipc))
    }
  }
  df
}

plot_by_ipc_box <- function(df # data.frame with scaled score
                           ) {
  # prepare for plotting
  df.m <- melt(df, id.vars=c("impl", "bin", "src", "tgt", "ipc"), measure.vars=c("sscore"))
  
  # plot
  p <- ggplot(df.m, aes(x=factor(bin), y=value))
  p <- p + geom_boxplot()
  # grid layout
  p <- p + facet_grid(tgt + src ~ ipc) # source over target (right to left)
  # reduce score scale & set titles
  p <- p + 
    scale_y_continuous(breaks=seq(0, 1, 0.5), name="Score (normalisiert)") +
    scale_x_discrete(name="Quellsegment")
  
  # theme customizing
  p <- p + theme_bw() +
    theme(
      panel.grid.major=element_line(colour="#cccccc"),
      panel.grid.minor=element_line(colour="#bbbbbb", linetype="dotted"),
      strip.background=element_rect(fill="#eeeeee"),
      plot.margin = unit(c(0, 0, 0, 0), "lines")
      )
  p
}

plot_by_scorer <- function() {
  print("-- Result by scorer plot --")
  # iterate through all database files
  for (dbFile in list.files(path=opt$source, pattern=cfg$srcFilePattern)) {
    dbFile <- paste0(opt$source, "/", dbFile)
    # get the plain file name
    dbFileName <- basename(dbFile)
    filePrefix <- gsub("\\.sqlite$", "", dbFileName)
    
    print(paste0("Open '", dbFile, "'.."))
    
    # assume that every scorer is present in db
    for (scorer in cfg$scorer) {
      print(paste0("[", dbFileName, "] scorer=", scorer, " ipc=all"))
            
      ### TERMS
      # gather scoring results for all ipc-sections
      df <- score_scale(terms_load_all(dbFile, scorer))
      # gather scoring result for every single ipc-section
      for (ipc in cfg$ipc) {
        print(paste0("[", dbFileName, "] type=terms scorer=", scorer, " ipc=", ipc))
        df <- rbind(df, score_scale(terms_load_all(dbFile, scorer, ipc)))
      }
      
      p <- plot_by_ipc_box(df)
      outFileName <- paste0("/", filePrefix, "-plot-terms-", scorer,".pdf")
      print(paste0("[", dbFileName, "] scorer=", scorer, " -> ", outFileName))
      print(p)
      ggsave(p, scale=1.5,
             file=paste0(opt$target, outFileName))
      
      ### SENTENCES
      # gather scoring results for all ipc-sections
      df <- score_scale(sentences_load_all(dbFile, scorer))
      # gather scoring result for every single ipc-section
      for (ipc in cfg$ipc) {
        print(paste0("[", dbFileName, "] type=sentences scorer=", scorer, " ipc=", ipc))
        df <- rbind(df, score_scale(sentences_load_all(dbFile, scorer, ipc)))
      }
      
      p <- plot_by_ipc_box(df)
      outFileName <- paste0("/", filePrefix, "-plot-sentences-", scorer,".pdf")
      print(paste0("[", dbFileName, "] scorer=", scorer, " -> ", outFileName))
      ggsave(p, scale=1.5,
             file=paste0(opt$target, outFileName))
    }
  }
}

plot_by_scorer()