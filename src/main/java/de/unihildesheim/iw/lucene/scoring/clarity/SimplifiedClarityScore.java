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
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

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
  Metrics metrics;
  /**
   * {@link IndexDataProvider} to use.
   */
  private IndexDataProvider dataProv;
  /**
   * Reader to access the Lucene index.
   */
  private IndexReader idxReader;
  /**
   * Lucene query analyzer.
   */
  private Analyzer analyzer;

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
    Objects.requireNonNull(builder, "Builder was null.");
    final SimplifiedClarityScore instance = new SimplifiedClarityScore();

    // set configuration
    instance.dataProv = builder.idxDataProvider;
    instance.idxReader = builder.idxReader;
    instance.metrics = new Metrics(builder.idxDataProvider);
    instance.analyzer = builder.analyzer;

    return instance;
  }

  @Override
  public ClarityScoreResult calculateClarity(final String query)
      throws ClarityScoreCalculationException {
    if (Objects.requireNonNull(query, "Query was null.").trim().isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // pre-check query terms
    final Collection<ByteArray> queryTerms;
    try {
      // get all query terms - list must NOT be unique!
      final QueryUtils queryUtils =
          new QueryUtils(this.analyzer, this.idxReader,
              this.dataProv.getDocumentFields());
      queryTerms = queryUtils.getAllQueryTerms(query);
      // remove stopwords from query
      queryTerms.removeAll(this.dataProv.getStopwordsBytes());
    } catch (ParseException | UnsupportedEncodingException e) {
      throw new ClarityScoreCalculationException(
          "Caught exception while preparing calculation.", e);
    } catch (Buildable.BuildableException e) {
      throw new ClarityScoreCalculationException(
          "Caught exception while building query.", e);
    }
    if (queryTerms == null || queryTerms.isEmpty()) {
      throw new IllegalStateException("No query terms.");
    }

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final double score;
    try {
      score = calculateScore(queryTerms);
    } catch (DataProviderException e) {
      throw new ClarityScoreCalculationException(e);
    }

    LOG.debug("Calculation results: query={} score={}.", query, score);

    LOG.debug("Calculating simplified clarity score for query {} "
        + "took {}. {}", query, timeMeasure.getTimeString(), score);

    return new Result(this.getClass(), score);
  }

  @Override
  public final String getIdentifier() {
    return IDENTIFIER;
  }

  /**
   * Calculate the Simplified Clarity Score for the given query terms.
   *
   * @param queryTerms Query terms to use for calculation
   * @return The calculated score
   */
  private double calculateScore(final Collection<ByteArray> queryTerms)
      throws DataProviderException {
    // length of the (rewritten) query
    final int queryLength = queryTerms.size();
    // number of terms in collection
    final long collTermCount = this.metrics.collection.tf();
    // create a unique list of query terms
    final Set<ByteArray> uniqueQueryTerms = new HashSet<>(queryTerms);

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
      final double pColl = this.metrics.collection.tf(queryTerm).doubleValue()
          / collTermCount;
      result += pMl * MathUtils.log2(pMl / pColl);
    }

    return result;
  }

  /**
   * Builder to create a new {@link SimplifiedClarityScore} instance.
   */
  public static final class Builder
      extends AbstractClarityScoreCalculationBuilder<Builder> {

    public Builder() {
      super(IDENTIFIER);
    }

    protected Builder getThis() {
      return this;
    }

    @Override
    public void validate()
        throws ConfigurationException {
      super.validate();
    }

    @Override
    public SimplifiedClarityScore build()
        throws ConfigurationException {
      validate();
      return SimplifiedClarityScore.build(this);
    }
  }

  @SuppressWarnings("PublicInnerClass")
  public static final class Result
      extends ClarityScoreResult {

    Result(
        final Class<? extends ClarityScoreCalculation> cscType,
        final double clarityScore) {
      super(cscType, clarityScore);
    }

    @Override
    public ScoringResultXml getXml() {
      // TODO: implement
      throw new UnsupportedOperationException("Not implemented yet.");
    }
  }
}
