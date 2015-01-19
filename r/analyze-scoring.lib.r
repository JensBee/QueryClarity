library(XML)

# load results in dataframe
loadScoreResults <- function(name, xmlFile, existingDf) {
  passages <- getNodeSet(xmlParse(xmlFile),
                         '/topicPassages/passages/passages/p')
  dfRows <- length(passages)
  df <- data.frame(set=rep("", dfRows), # dataset identifier
                   score=rep(NA, dfRows), # scoring result value
                   scorer=rep("", dfRows), # scorer id
                   q=rep("", dfRows), # query string
                   lang=rep("", dfRows), # query language
                   stringsAsFactors=FALSE)

  # go through all results
  for (i in 1:length(passages)) {
    df[i,1] <- name
    df[i,2] <- as.numeric(xmlGetAttr(passages[[i]][['score']], 'score'))
    df[i,3] <- xmlGetAttr(passages[[i]][['score']], 'impl')
    df[i,4] <- xmlValue(passages[[i]][['content']])
    df[i,5] <- xmlGetAttr(passages[[i]], 'lang')
  }

  df
}