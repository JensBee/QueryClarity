library(ggplot2)
library(sqldf)

dfData.dbFile <- "../data/clef-topics/common-terms-de-all.sqlite"
dfData.outFile <- "../data/df-term-bins.csv"
dfData.stopAt = 0.01 # stopwords threshold
dfData.bins <- 7 # segments to generate
dfData.pick <- c(1, 3, 5, 7) # segments to select

# -- load dataset
# loadTermData()
# -- plot data
# plotTermData()

# -- extract terms (prints number of terms per bin)
# termDf.claims.samples <- extractTerms(termDf.claims.bins, "claims")
# termDf.detd.samples <- extractTerms(termDf.claims.bins, "detd")
# -- sample terms (number of terms should at least match previous output)
dfData.claims.sampleSize <- 50 # how many terms to sample
dfData.detd.sampleSize <- 50
# termDf.claims.samples.data <- sampleTerms(termDf.claims.samples, dfData.claims.sampleSize, "claims")
# termDf.detd.samples.data <- sampleTerms(termDf.detd.samples, dfData.detd.sampleSize, "detd")
# -- write samples
# write.csv(file=dfData.outFile, x=rbind(termDf.claims.samples.data, termDf.detd.samples.data))

loadTermData <- function() {
  # load data from sqlite db
  loadTermDumpFreqs <- function(table) {
    df <- sqldf(paste0('select reldf from ', table, ' order by reldf desc'), 
                dbname=dfData.dbFile)
  }
  
  # create a number of segments based on values of a column
  getDfBins <- function(df, col, segs, stopAt) {
    df.range <- range(df[[col]])
    df.segSize <- (min(df.range[2], stopAt) - df.range[1]) / segs
    df.splitPoints = list(0)
    for (i in 0:(segs -1)) {
      df.splitPoints = c(df.splitPoints, (df.range[1] + df.segSize * i) + df.segSize)
    }
    as.list(split(df, cut(df[[col]], df.splitPoints, include.lowest=TRUE)))
  }
  
  # -- claims
  termDf.claims <<- loadTermDumpFreqs("claims")
  termDf.claims.f <<- aggregate.data.frame(rep(1, nrow(termDf.claims)), termDf.claims, sum)
  # create segments based on relDF values
  termDf.claims.bins <<- getDfBins(termDf.claims.f, "reldf", dfData.bins, dfData.stopAt)
  
  # -- descriptions
  termDf.detd <<- loadTermDumpFreqs("detd")
  termDf.detd.f <<- aggregate.data.frame(rep(1, nrow(termDf.detd)), termDf.detd, sum)
  # create segments based on relDF values
  termDf.detd.bins <<- getDfBins(termDf.detd.f, "reldf",  dfData.bins, dfData.stopAt)
}

sampleTerms <- function(sampleDf, sampleSize, field) {
  rowList <- NULL
  
  for (i in 1:length(dfData.pick)) {
    sampledRows <- sample(which(sampleDf$bin==i), sampleSize)
    if (is.null(rowList)) {
      rowList <- sampledRows
    } else {
      rowList <- c(rowList, sampledRows)
    }
  }
  
  df <- sampleDf[rowList,]
  df[["field"]] <- apply(df, 1, function(row) field)
  df
}

extractTerms <- function(bins, table) {
  extract <- function(binList, dbTable, field) {
    resultDf <- NULL
    
    for (i in 1:length(dfData.pick)) {
      binRange <- range(binList[[dfData.pick[[i]]]][[field]])
      df <- sqldf(paste0('select term, reldf from ', dbTable, ' where ',
                         'reldf between ', binRange[1], ' and ', binRange[2]),
                  dbname=dfData.dbFile)
      df[["bin"]] <- apply(df, 1, function(row) i)
      
      print(paste0("Bin ", i, " range ", binRange[1], " - ", binRange[2], 
                   " elements: ", nrow (df)))
      if (is.null(resultDf)) {
        resultDf <- df
      } else {
        resultDf <- rbind(resultDf, df)
      }
    }
    resultDf
  }
  
  extract(bins, table, "reldf")
}

createTermBins <- function(binList, picks, field) {
  bins <- length(picks)
  df <- data.frame(xmin=rep(0, bins),
                   xmax=rep(0, bins),
                   ymin=rep(-Inf, bins),
                   ymax=rep(Inf, bins))
  for (i in 1:bins) {
    df[i,] <- c(range(binList[[picks[[i]]]][[field]]), -Inf, Inf)
  }
  df
}

plotTermData <- function() {
  getPlot <- function(df.f, df.bins) {
    plot <- ggplot() + scale_x_log10() +
      geom_line(data=df.f, aes(x=reldf, y=x)) +
      geom_rect(data=createTermBins(df.bins, dfData.pick, "reldf"), 
                color="green", fill="green", alpha=0.15,
                aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax)) +
      # mark stopwords threshold
      geom_rect(color="red", 
                aes(xmin=dfData.stopAt, xmax=dfData.stopAt, ymin=-Inf, ymax=Inf))
    plot
  }
  
  # claims
  plot.claims <- getPlot(termDf.claims.f, termDf.claims.bins) +
    labs(list(title=paste0("Rel. Dokumentfrequenz: Term t in Feld f\n Grenzwert: ", dfData.stopAt, '\n'), 
              x="relDF(t, f); Feld 'claims'", y="Häufigkeit"))
  
  # detd
  plot.detd <- getPlot(termDf.detd.f, termDf.detd.bins) +
    labs(list(x="relDF(t, f); Feld 'detd'", y="Häufigkeit"))
  
  grid.arrange(plot.claims, plot.detd)
}
    