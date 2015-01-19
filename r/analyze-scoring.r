setwd("~/IntelliJProjects/Clarity/QueryClarityFIZ/r")

source("analyze-scoring.lib.r")
library(ggplot2)

# load document
ctDf <-
  loadScoreResults("cterms",
    "../data/clef-topics/topics-clef-test-commonterms-dcs-de.xml")
View(ctDf)

blDf <-
  loadScoreResults("bline",
    "../data/clef-topics/topics-clef-test-baseline-de.xml")
View(blDf)

plot <- ggplot() +
  # ct
  geom_point(data=ctDf,
             aes(row.names(ctDf), score), colour="green") +
  geom_line(data=ctDf,
            aes(row.names(ctDf), score, group=scorer), colour="green") +
  # bl
  geom_point(data=blDf,
             aes(row.names(blDf), score), colour="red") +
  geom_line(data=blDf,
            aes(row.names(blDf), score, group=scorer), colour="red") +
  # completely remove x axis
  theme(axis.text.x=element_blank(),
        axis.title.x=element_blank(),
        axis.ticks=element_blank())
plot