/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.unihildesheim.lucene.scoring.clarity.impl;

import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.util.TimeMeasure;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.slf4j.LoggerFactory;

/**
 * Simplified Clarity Score implementation as described by He, Ben, and Iadh
 * Ounis.
 * <p>
 * Reference:
 * <p>
 * He, Ben, and Iadh Ounis. “Inferring Query Performance Using Pre-Retrieval
 * Predictors.” In String Processing and Information Retrieval, edited by
 * Alberto Apostolico and Massimo Melucci, 43–54. Lecture Notes in Computer
 * Science 3246. Springer Berlin Heidelberg, 2004.
 * http://link.springer.com/chapter/10.1007/978-3-540-30213-1_5.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class SimplifiedClarityScore implements ClarityScoreCalculation {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          SimplifiedClarityScore.class);

  /**
   * Provider for statistical index related informations. Accessed from nested
   * thread class.
   */
  private final IndexDataProvider dataProv;

  /**
   * Index reader used by this instance. Accessed from nested thread class.
   */
  private final IndexReader reader;

  /**
   * Default constructor using the {@link IndexDataProvider} for statistical
   * index data.
   *
   * @param indexReader {@link IndexReader} to use by this instance
   * @param dataProvider Provider for statistical index data
   */
  public SimplifiedClarityScore(final IndexReader indexReader,
          final IndexDataProvider dataProvider) {
    super();
    this.reader = indexReader;
    this.dataProv = dataProvider;
  }

  /**
   * Calculate the Simplified Clarity Score for the given query terms.
   *
   * @param queryTerms Query terms to use for calculation
   * @return The calculated score
   */
  private double calculateScore(final Collection<BytesWrap> queryTerms) {
    // create a unique set of query terms
    final Collection<BytesWrap> uniqueQueryTerms = new HashSet<>(queryTerms);
    // length of the (rewritten) query
    final int queryLength = queryTerms.size();
    // number of unique terms in collection
    final long collTermCount = this.dataProv.getUniqueTermsCount();

    double result = 0d;

    // calculate max likelyhood of the query model for each term in the query
    for (BytesWrap queryTerm : uniqueQueryTerms) {
      int termCount = 0;
      for (BytesWrap aTerm : queryTerms) {
        if (aTerm.equals(queryTerm)) {
          termCount++;
        }
      }
      double pMl = (double) termCount / (double) queryLength;
      double pColl = (double) dataProv.getTermFrequency(queryTerm)
              / (double) collTermCount;
      double log = (Math.log(pMl) / Math.log(2)) - (Math.log(pColl) / Math.
              log(2));
      result += pMl * log;
    }

    return result;
  }

  @Override
  public ClarityScoreResult calculateClarity(final Query query) {
    if (query == null) {
      throw new IllegalArgumentException("Query was null.");
    }

    // pre-check query terms
    final Collection<BytesWrap> queryTerms;
    try {
      queryTerms = QueryUtils.getQueryTerms(this.reader, query);
    } catch (IOException ex) {
      LOG.error("Caught exception while preparing calculation.", ex);
      return null;
    }
    if (queryTerms == null || queryTerms.isEmpty()) {
      throw new IllegalStateException("No query terms.");
    }

    // convert query to readable string for logging output
    StringBuilder queryStr = new StringBuilder(100);
    for (BytesWrap bw : queryTerms) {
      queryStr.append(BytesWrapUtil.bytesWrapToString(bw)).append(' ');
    }
    LOG.info("Calculating clarity score. query={}", queryStr);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final double score = calculateScore(queryTerms);

    LOG.debug("Calculation results: query={} score={}.",
            queryStr, score);

    LOG.debug("Calculating simplified clarity score for query {} "
            + "took {}.", queryStr, timeMeasure.getTimeString());

    return new ClarityScoreResult(this.getClass(), score);
  }
}
