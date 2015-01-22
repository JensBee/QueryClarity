library(ggplot2)

source("analyze-scoring.lib.r")

dbFile <- "../data/clef-topics/common-terms-de-all.sqlite"
# claims
termDf.claims <-
  loadTermDumpFreqs(dbFile, "claims")
termDf.claims.f <- aggregate.data.frame(rep(1, nrow(termDf.claims)), termDf.claims, sum)
rm(termDf.claims)

# descriptions
termDf.detd <-
  loadTermDumpFreqs(dbFile, "detd")
termDf.detd.f <- aggregate.data.frame(rep(1, nrow(termDf.detd)), termDf.detd, sum)
rm(termDf.detd)

plot <- ggplot() + scale_x_log10() +
  geom_point(data=termDf.claims.f, method=loess, se=FALSE,
             aes(x=reldf, y=x, colour="claims")) +
  geom_line(data=termDf.claims.f,
            aes(x=reldf, y=x, colour="claims")) +
  geom_point(data=termDf.detd.f, method=loess, se=FALSE,
             aes(x=reldf, y=x, colour="detd")) +
  geom_line(data=termDf.detd.f, 
            aes(x=reldf, y=x, colour="detd")) +
  
  scale_colour_manual(name="Feld", values=c(claims="red", detd="blue")) +
  labs(list(title="Rel. Dokumentfrequenz: Term t in Feld f", 
              x="relDF(t, f)", y="Häufigkeit"))
plot
