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
package de.unihildesheim.iw.lucene.scoring.clarity;

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;

/**
 * Simplified Clarity Score implementation as described by He, Ben, and Iadh
 * Ounis.
 * <p/>
 * Reference:
 * <p/>
 * He, Ben, and Iadh Ounis. “Inferring Query Performance Using Pre-Retrieval
 * Predictors.” In String Processing and Information Retrieval, edited by
 * Alberto Apostolico and Massimo Melucci, 43–54. Lecture Notes in Computer
 * Science 3246. Springer Berlin Heidelberg, 2004. http://link.springer
 * .com/chapter/10.1007/978-3-540-30213-1_5.
 *
 * @author Jens Bertram
 */
public final class SimplifiedClarityScore
    implements ClarityScoreCalculation {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      SimplifiedClarityScore.class);

  /**
   * {@link IndexDataProvider} to use.
   */
  private IndexDataProvider dataProv;

  /**
   * Provider for general index metrics.
   */
  protected Metrics metrics;

  /**
   * Default constructor. Called from builder.
   */
  private SimplifiedClarityScore() {
    super();
  }

  /**
   * Builder method to create a new instance.
   *
   * @param builder Builder to use for constructing the instance
   * @return New instance
   */
  protected static SimplifiedClarityScore build(final Builder
      builder) {
    final SimplifiedClarityScore instance = new SimplifiedClarityScore();

    // set configuration
    instance.setIndexDataProvider(builder.idxDataProvider);
    instance.metrics = Metrics.getInstance(builder.idxDataProvider);

    return instance;
  }

  /**
   * Set the {@link IndexDataProvider} to use.
   *
   * @param newDataProv Data provider
   */
  private void setIndexDataProvider(final IndexDataProvider newDataProv) {
    this.dataProv = newDataProv;
  }

  /**
   * Calculate the Simplified Clarity Score for the given query terms.
   *
   * @param queryTerms Query terms to use for calculation
   * @return The calculated score
   */
  private double calculateScore(final Collection<ByteArray> queryTerms) {
    // length of the (rewritten) query
    final int queryLength = queryTerms.size();
    // number of unique terms in collection
    final double collTermCount = this.metrics.collection.numberOfUniqueTerms().
        doubleValue();

    double result = 0d;

    // calculate max likelihood of the query model for each term in the
    // query
    // iterate over all query terms
    for (ByteArray queryTerm : queryTerms) {
      // number of times a query term appears in the query
      int termCount = 0;
      // count the number of occurrences
      for (ByteArray aTerm : queryTerms) {
        if (aTerm.equals(queryTerm)) {
          termCount++;
        }
      }
      double pMl =
          Integer.valueOf(termCount).doubleValue() / Integer.valueOf(
              queryLength).doubleValue();
      double pColl = this.metrics.collection.tf(queryTerm).doubleValue()
          / collTermCount;
      double log = (Math.log(pMl) / Math.log(2)) - (Math.log(pColl) /
          Math.log(2));
      result += pMl * log;
    }

    return result;
  }

  @Override
  public ClarityScoreResult calculateClarity(final String query)
      throws ParseException, IOException {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // pre-check query terms
    final Collection<ByteArray> queryTerms;
    try {
      // get all query terms - list must NOT be unique!
      final QueryUtils queryUtils =
          new QueryUtils(this.dataProv.getIndexReader(),
              this.dataProv.getDocumentFields());
      queryTerms = queryUtils.getAllQueryTerms(query);
    } catch (UnsupportedEncodingException e) {
      LOG.error("Caught exception while preparing calculation.", e);
      return null;
    } catch (Buildable.BuilderConfigurationException e) {
      LOG.error("Caught exception while building query.", e);
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
        + "took {}. {}", query, timeMeasure.getTimeString(), score);

    return new ClarityScoreResult(this.getClass(), score);
  }

  /**
   * Builder to create a new {@link SimplifiedClarityScore} instance.
   */
  public static final class Builder
      implements Buildable<SimplifiedClarityScore> {
    /**
     * {@link IndexDataProvider} to use.
     */
    protected IndexDataProvider idxDataProvider = null;

    public Builder indexDataProvider(
        final IndexDataProvider dataProv) {
      this.idxDataProvider = dataProv;
      return this;
    }

    @Override
    public SimplifiedClarityScore build()
        throws BuilderConfigurationException {
      validate();
      return SimplifiedClarityScore.build(this);
    }

    @Override
    public void validate()
        throws BuilderConfigurationException {
    }
  }
}
