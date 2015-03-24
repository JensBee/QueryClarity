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

import de.unihildesheim.iw.Buildable.ConfigurationException;
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.util.MathUtils.KlDivergenceHighPrecision;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Map;
import java.util.Map.Entry;

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
    extends AbstractClarityScoreCalculation {

  /**
   * Prefix used to identify externally stored data.
   */
  @SuppressWarnings("WeakerAccess")
  public static final String IDENTIFIER = "SCS";
  /**
   * Default math context for model calculations.
   */
  private static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf()
          .getString(DefaultKeys.MATH_CONTEXT.toString(),
              GlobalConfiguration.DEFAULT_MATH_CONTEXT));
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      SimplifiedClarityScore.class);
  /**
   * Provider for general index data.
   */
  private final IndexDataProvider dataProv;
  /**
   * Lucene query analyzer.
   */
  private final Analyzer analyzer;

  /**
   * Default constructor. Called from builder.
   *
   * @param builder Builder to use for constructing the instance
   */
  @SuppressWarnings("WeakerAccess")
  SimplifiedClarityScore(@NotNull final Builder builder) {
    super(IDENTIFIER);

    // set configuration
    this.dataProv = builder.getIndexDataProvider();
    this.analyzer = builder.getAnalyzer();
  }

  /**
   * Calculate a portion of the score for a single term.
   * @param term Term
   * @param termFreq Frequency of term in the query
   * @param queryLength Number of terms in the query
   * @return Scoring tuple for the term
   */
  ScoreTupleHighPrecision calcScorePortion(
      @NotNull final BytesRef term,
      final long termFreq,
      final long queryLength) {
   return new ScoreTupleHighPrecision(
       BigDecimal.valueOf(termFreq)
           .divide(BigDecimal.valueOf(queryLength), MATH_CONTEXT),
       BigDecimal.valueOf(this.dataProv.metrics().relTf(term))
   );
  }

  @Override
  public Result calculateClarity(@NotNull final String query) {
    if (StringUtils.isStrippedEmpty(query)) {
      throw new IllegalArgumentException("Query was empty.");
    }
    final Result result = new Result();

    // pre-check query terms
    // mapping of term->in query freq. Does not remove unknown terms
    final Map<BytesRef, Integer> queryTerms =
        QueryUtils.tokenizeAndMapQuery(query, this.analyzer);

    if (queryTerms.isEmpty()) {
      result.setEmpty("No query terms.");
      return result;
    }
    result.setQueryTerms(queryTerms.keySet());

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // length of the query
    long queryLength = 0L;
    for (final Integer count : queryTerms.values()) {
      queryLength += count;
    }

    // calculate max likelihood of the query model for each term in the
    // query
    final ScoreTupleHighPrecision[] dataSets = new
        ScoreTupleHighPrecision[queryTerms.size()];
    int idx = 0;
    for (final Entry<BytesRef, Integer> qTermEntry : queryTerms.entrySet()) {
      dataSets[idx++] = calcScorePortion(
          qTermEntry.getKey(), qTermEntry.getValue(), queryLength);
    }
    final double score =
        KlDivergenceHighPrecision.sumAndCalc(dataSets).doubleValue();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Calculation results: query={} score={}.", query, score);
      LOG.debug("Calculating simplified clarity score for query {} "
          + "took {}. {}", query, timeMeasure.getTimeString(), score);
    }

    result.setScore(score);
    return result;
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  /**
   * Builder to create a new {@link SimplifiedClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractCSCBuilder<Builder, SimplifiedClarityScore> {

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public SimplifiedClarityScore build()
        throws ConfigurationException {
      validateFeatures(
          Feature.ANALYZER,
          Feature.DATA_PROVIDER);
      return new SimplifiedClarityScore(this);
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
