library(XML)
library(ggplot2)

# load results in dataframe
lib.loadScoreResults <- function(name, xmlFile) {
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

# load document
ctDf <-
  loadScoreResults("cterms",
    "../data/clef-topics/topics-clef-test-commonterms-dcs-de.xml")
View(ctDf)
min(ctDf$score); max(ctDf$score)
max(ctDf$score) - min(ctDf$score)

blDf <-
  loadScoreResults("bline",
    "../data/clef-topics/topics-clef-test-baseline-de.xml")
View(blDf)
min(blDf$score); max(blDf$score)
max(blDf$score) - min(blDf$score)

plot <- ggplot() +
  # ct
  geom_point(data=ctDf, colour="green",
             aes(row.names(ctDf), score)) +
  geom_point(data=ctDf, colour="green", shape=17,
             aes(row.names(ctDf), rank)) +
  geom_line(data=ctDf, colour="green",
            aes(row.names(ctDf), score, group=scorer)) +
  geom_hline(colour="darkgreen",
    yintercept=median(ctDf$score)) +
  # bl
  geom_point(data=blDf, colour="red",
             aes(row.names(blDf), score)) +
  geom_point(data=ctDf, colour="red", shape=15,
             aes(row.names(blDf), rank)) +
  geom_line(data=blDf, colour="red",
            aes(row.names(blDf), score, group=scorer)) +
  geom_hline(colour="darkred",
             yintercept=median(blDf$score)) +
  # completely remove x axis
  theme(axis.text.x=element_blank(),
        axis.title.x=element_blank(),
        axis.ticks=element_blank())
plot