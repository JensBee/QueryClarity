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

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Simplified Clarity Score implementation as described by He, Ben, and Iadh
 * Ounis. <br> Reference: <br> He, Ben, and Iadh Ounis. “Inferring Query
 * Performance Using Pre-Retrieval Predictors.” In String Processing and
 * Information Retrieval, edited by Alberto Apostolico and Massimo Melucci,
 * 43–54. Lecture Notes in Computer Science 3246. Springer Berlin Heidelberg,
 * 2004. http://link.springer .com/chapter/10.1007/978-3-540-30213-1_5.
 *
 * @author Jens Bertram
 */
public final class SimplifiedClarityScore
    implements ClarityScoreCalculation {

  /**
   * Prefix used to identify externally stored data.
   */
  static final String IDENTIFIER = "SCS";

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      SimplifiedClarityScore.class);

  /**
   * Provider for general index metrics.
   */
  @SuppressWarnings("PackageVisibleField")
  final Metrics metrics;

  /**
   * Lucene query analyzer.
   */
  private final Analyzer analyzer;

  /**
   * Default constructor. Called from builder.
   *
   * @param builder Builder to use for constructing the instance
   */
  SimplifiedClarityScore(final Builder builder) {
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    this.metrics = new Metrics(builder.idxDataProvider);
    this.analyzer = builder.analyzer;
  }

  @Override
  public Result calculateClarity(final String query)
      throws ClarityScoreCalculationException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }
    final Result result = new Result();

    // pre-check query terms
    final Collection<ByteArray> queryTerms;
    try {
      // get all query terms - list must NOT be unique!
      final List<String> qTerms =
          QueryUtils.tokenizeQueryString(query, this.analyzer);
      queryTerms = new ArrayList<>(qTerms.size());
      for (final String term : qTerms) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final ByteArray termBa = new ByteArray(term.getBytes("UTF-8"));
        queryTerms.add(termBa);
      }
    } catch (final UnsupportedEncodingException e) {
      throw new ClarityScoreCalculationException(
          "Caught exception while preparing calculation.", e);
    }
    if (queryTerms.isEmpty()) {
      result.setEmpty("No query terms.");
      return result;
    }
    result.setQueryTerms(queryTerms);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final double score;
    score = calculateScore(queryTerms);

    LOG.debug("Calculation results: query={} score={}.", query, score);

    LOG.debug("Calculating simplified clarity score for query {} "
        + "took {}. {}", query, timeMeasure.getTimeString(), score);

    result.setScore(score);
    return result;
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  /**
   * Calculate the Simplified Clarity Score for the given query terms.
   *
   * @param queryTerms Query terms to use for calculation
   * @return The calculated score
   */
  double calculateScore(final Collection<ByteArray> queryTerms) {
    // length of the (rewritten) query
    final int queryLength = queryTerms.size();
    // number of terms in collection
    final long collTermCount = this.metrics.collection().tf();
    // create a unique list of query terms
    final Iterable<ByteArray> uniqueQueryTerms = new HashSet<>(queryTerms);

    double result = 0d;

    // calculate max likelihood of the query model for each term in the
    // query
    // iterate over all unique query terms
    for (final ByteArray queryTerm : uniqueQueryTerms) {
      // number of times a query term appears in the query
      int termCount = 0;
      // count the number of occurrences in the non-unique list
      for (final ByteArray aTerm : queryTerms) {
        if (queryTerm.equals(aTerm)) {
          termCount++;
        }
      }
      assert termCount > 0;

      final double pMl =
          Integer.valueOf(termCount).doubleValue() / Integer.valueOf(
              queryLength);
      final double pColl = this.metrics.collection().tf(queryTerm).doubleValue()
          / (double) collTermCount;
      result += pMl * MathUtils.log2(pMl / pColl);
    }

    return result;
  }

  @Override
  public void close()
      throws Exception {
    // No function, only added to be compatible to other score types
  }

  /**
   * Builder to create a new {@link SimplifiedClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractClarityScoreCalculationBuilder<Builder> {

    /**
     * Initializes the builder.
     */
    public Builder() {
      super(IDENTIFIER);
    }

    @Override
    public SimplifiedClarityScore build()
        throws ConfigurationException {
      validate();
      return new SimplifiedClarityScore(this);
    }

    @Override
    protected Builder getThis() {
      return this;
    }
  }

  /**
   * Wraps the calculation reslt and additional meta information.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Result
      extends ClarityScoreResult {

    /**
     * Initializes the result.
     */
    Result() {
      super(SimplifiedClarityScore.class);
    }

    @Override
    public ScoringResultXml getXml() {
      final ScoringResultXml xml = new ScoringResultXml();

      getXml(xml);

      return xml;
    }
  }
}
