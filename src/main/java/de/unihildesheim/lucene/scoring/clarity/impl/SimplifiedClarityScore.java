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

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.TimeMeasure;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import org.apache.lucene.queryparser.classic.ParseException;
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
   * Default constructor using the {@link IndexDataProvider} for statistical
   * index data.
   */
  public SimplifiedClarityScore() {
    super();
  }

  /**
   * Calculate the Simplified Clarity Score for the given query terms.
   *
   * @param queryTerms Query terms to use for calculation
   * @return The calculated score
   */
  private double calculateScore(final Collection<BytesWrap> queryTerms) {
    // length of the (rewritten) query
    final int queryLength = queryTerms.size();
    // number of unique terms in collection
    final double collTermCount = Long.valueOf(Environment.getDataProvider().
            getUniqueTermsCount()).doubleValue();

    double result = 0d;

    // calculate max likelyhood of the query model for each term in the query
    // iterate over all query terms
    for (BytesWrap queryTerm : queryTerms) {
      // number of times a query term appears in the query
      int termCount = 0;
      // count the number of occurences
      for (BytesWrap aTerm : queryTerms) {
        if (aTerm.equals(queryTerm)) {
          termCount++;
        }
      }
      double pMl = Integer.valueOf(termCount).doubleValue() / Integer.valueOf(
              queryLength).doubleValue();
      double pColl = CollectionMetrics.tf(queryTerm).doubleValue()
              / collTermCount;
      double log = (Math.log(pMl) / Math.log(2)) - (Math.log(pColl) / Math.
              log(2));
      result += pMl * log;
    }

    return result;
  }

  @Override
  public ClarityScoreResult calculateClarity(final String query) throws
          ParseException {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // pre-check query terms
    final Collection<BytesWrap> queryTerms;
    try {
      // get all query terms - list must NOT be unique!
      queryTerms = QueryUtils.getAllQueryTerms(query);
    } catch (UnsupportedEncodingException ex) {
      LOG.error("Caught exception while preparing calculation.", ex);
      return null;
    }
    if (queryTerms == null || queryTerms.isEmpty()) {
      throw new IllegalStateException("No query terms.");
    }

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final double score = calculateScore(queryTerms);

    LOG.debug("Calculation results: query={} score={}.", query, score);

    LOG.debug("Calculating simplified clarity score for query {} "
            + "took {}.", query, timeMeasure.getTimeString());

    return new ClarityScoreResult(this.getClass(), score);
  }
}
