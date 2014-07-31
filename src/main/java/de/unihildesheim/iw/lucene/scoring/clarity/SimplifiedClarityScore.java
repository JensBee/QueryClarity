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
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.util.BigDecimalCache;
import de.unihildesheim.iw.util.Configuration;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import org.apache.lucene.analysis.Analyzer;
import org.mapdb.Fun;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  static final MathContext MATH_CONTEXT = MathContext.DECIMAL128;
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
    this.metrics = new Metrics(builder.getIndexDataProvider());
    this.analyzer = builder.getAnalyzer();
  }

  @Override
  public Result calculateClarity(final String query)
      throws ClarityScoreCalculationException, DataProviderException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }
    final Result result = new Result();

    // pre-check query terms
    // mapping of term->in query freq. Does not remove unknown terms
    final Map<ByteArray, Integer> queryTerms =
        QueryUtils.tokenizeAndMapQuery(query, this.analyzer);

    if (queryTerms.isEmpty()) {
      result.setEmpty("No query terms.");
      return result;
    }
    result.setQueryTerms(queryTerms);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // length of the query
    int queryLength = 0;
    for (final Integer count : queryTerms.values()) {
      queryLength += count;
    }
    // number of terms in collection
    final long collTermCount = this.metrics.collection().tf();

    // calculate max likelihood of the query model for each term in the
    // query
    // iterate over all unique query terms
    final List<Fun.Tuple2<BigDecimal, BigDecimal>> dataSet =
        new ArrayList<>(queryTerms.size());
    for (final Map.Entry<ByteArray, Integer> qTermEntry :
        queryTerms.entrySet()) {
      final BigDecimal pMl =
          BigDecimalCache.get(qTermEntry.getValue())
              .divide(BigDecimalCache.get(queryLength), MATH_CONTEXT);
//          qTermEntry.getValue().doubleValue() / (double) queryLength;
      dataSet.add(
          Fun.t2(pMl, this.metrics.collection().relTf(qTermEntry.getKey())));
    }

    final double score;
    try {
      score = MathUtils.KlDivergence.calcBig(
          dataSet, MathUtils.KlDivergence.sumBigValues(dataSet)
      ).doubleValue();
    } catch (final ProcessingException e) {
      final String msg = "Caught exception while calculating score.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

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
      extends ClarityScoreCalculationBuilder<SimplifiedClarityScore,
      Configuration> {

    /**
     * Initializes the builder.
     */
    public Builder() {
      super(IDENTIFIER);
    }

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public SimplifiedClarityScore build()
        throws ConfigurationException {
      validate();
      return new SimplifiedClarityScore(this);
    }

    @Override
    public void validate()
        throws ConfigurationException {
      new Validator(this, new Feature[]{
          Feature.ANALYZER,
          Feature.DATA_PROVIDER,
          Feature.INDEX_READER
      });
    }
  }

  /**
   * Wraps the calculation result and additional meta information.
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
