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
import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.Tuple.Tuple2;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.util.MathUtils.KlDivergence;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
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
    extends AbstractClarityScoreCalculation {

  /**
   * Prefix used to identify externally stored data.
   */
  public static final String IDENTIFIER = "SCS";
  /**
   * Default math context for model calculations.
   */
  private static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf().getString(
          DefaultKeys.MATH_CONTEXT.toString()));
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
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
  private SimplifiedClarityScore(final Builder builder) {
    super(IDENTIFIER);
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    this.dataProv = builder.getIndexDataProvider();
    this.analyzer = builder.getAnalyzer();
  }

  @Override
  public Result calculateClarity(final String query) {
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
    long queryLength = 0L;
    for (final Integer count : queryTerms.values()) {
      queryLength += count;
    }

    // calculate max likelihood of the query model for each term in the
    // query
    // iterate over all unique query terms
    final Collection<Tuple2<BigDecimal, BigDecimal>> dataSet =
        new ArrayList<>(queryTerms.size());
    for (final Entry<ByteArray, Integer> qTermEntry : queryTerms.entrySet()) {
      dataSet.add(Tuple.tuple2(
          BigDecimal.valueOf(qTermEntry.getValue())
              .divide(BigDecimal.valueOf(queryLength), MATH_CONTEXT),
          BigDecimal.valueOf(this.dataProv.metrics().relTf(qTermEntry.getKey
              ()))));
    }

    final double score = KlDivergence.sumAndCalc(dataSet).doubleValue();

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
   * Builder to create a new {@link SimplifiedClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractBuilder<
                  SimplifiedClarityScore, Builder> {

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public SimplifiedClarityScore build()
        throws ConfigurationException {
      validateFeatures(new Feature[]{
          Feature.CONFIGURATION,
          Feature.ANALYZER,
          Feature.DATA_PROVIDER,
          Feature.INDEX_READER
      });
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
