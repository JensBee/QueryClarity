library(XML)
library(sqldf)

# load results in dataframe
loadScoreResults <- function(name, xmlFile) {
  passages <- getNodeSet(xmlParse(xmlFile),
                         '/topicPassages/passages/passages/p')
  dfRows <- length(passages)
  df <- data.frame(set=rep("", dfRows), # dataset identifier
                   score=rep(NA, dfRows), # scoring result value
                   scorer=rep("", dfRows), # scorer id
                   q=rep("", dfRows), # query string
                   lang=rep("", dfRows), # query language
                   rank=rep(0, dfRows), # rank of source document
                   stringsAsFactors=FALSE)

  # go through all results
  for (i in 1:length(passages)) {
    df[i,1] <- name
    df[i,2] <- as.numeric(xmlGetAttr(passages[[i]][['score']], 'score'))
    df[i,3] <- xmlGetAttr(passages[[i]][['score']], 'impl')
    df[i,4] <- xmlValue(passages[[i]][['content']])
    df[i,5] <- xmlGetAttr(passages[[i]], 'lang')
    rank <- xmlGetAttr(passages[[i]][['score']], 'source-rank')
    if (!is.null(rank)) {
      print(paste0("rank ",rank))
      df[i,5] <- as.numeric(rank)
    }
  }
  df
}

loadTermDump <- function(dbFile) {
  df <- sqldf('select * from claims limit 100', dbname=dbFile)
  #df <- read.csv.sql(file=csvFile, header=TRUE,
  #                   field.types=c(),
  #                   colClasses(c(character, double))
  #               stringsAsFactors=FALSE)
  #df
}
loadTermDumpFreqs <- function(dbFile, table) {
  df <- sqldf(paste0('select reldf from ', table, ' order by reldf desc'), dbname=dbFile)
}