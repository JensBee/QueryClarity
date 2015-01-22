source("analyze-scoring.lib.r")
library(ggplot2)

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