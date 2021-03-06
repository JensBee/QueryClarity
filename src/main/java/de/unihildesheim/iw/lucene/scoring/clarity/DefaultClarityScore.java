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

import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreResult
    .EmptyReason;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.lucene.util.DocIdSetUtils;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.util.Buildable.BuildableException;
import de.unihildesheim.iw.util.GlobalConfiguration;
import de.unihildesheim.iw.util.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanQuery.TooManyClauses;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default Clarity Score implementation as described by Cronen-Townsend, Steve,
 * Yun Zhou, and W. Bruce Croft. <br> Reference: <br> “Predicting Query
 * Performance.” In Proceedings of the 25th Annual International ACM SIGIR
 * Conference on Research and Development in Information Retrieval, 299–306.
 * SIGIR ’02. New York, NY, USA: ACM, 2002. doi:10.1145/564376.564429.
 *
 * @author Jens Bertram
 */
public final class DefaultClarityScore
    extends AbstractClarityScoreCalculation {
  /**
   * Prefix used to identify externally stored data.
   */
  @SuppressWarnings("WeakerAccess")
  public static final String IDENTIFIER = "DCS";
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultClarityScore.class);
  /**
   * Context for high precision math calculations.
   */
  @SuppressWarnings("WeakerAccess")
  static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf()
          .getString(DefaultKeys.MATH_CONTEXT.toString(),
              GlobalConfiguration.DEFAULT_MATH_CONTEXT));
  /**
   * {@link IndexDataProvider} to use.
   */
  private final IndexDataProvider dataProv;
  /**
   * Analyzer for parsing queries.
   */
  private final Analyzer analyzer;
  /**
   * Provider for feedback vocabulary.
   */
  private final VocabularyProvider vocProvider;
  /**
   * Provider for feedback documents.
   */
  private final FeedbackProvider fbProvider;
  /**
   * Configuration object used for all parameters of the calculation.
   */
  private final DefaultClarityScoreConfiguration conf;
  /**
   * If true, low precision math is used for doing calculations.
   */
  private static final boolean MATH_LOW_PRECISION = GlobalConfiguration.conf()
      .getBoolean(DefaultKeys.MATH_LOW_PRECISION.toString(), false);
  /**
   * Language model weighting parameter value.
   */
  private final double docLangModelWeight;

  /**
   * Abstract model class. Shared methods for low/high precision model
   * calculations.
   */
  private abstract static class AbstractModel {
    /**
     * Logger instance for this class.
     */
    private static final org.slf4j.Logger LOG =
        LoggerFactory.getLogger(AbstractModel.class);
    /**
     * List of query terms issued.
     */
    final BytesRefArray queryTerms;
    /**
     * List of feedback documents to use.
     */
    final int[] feedbackDocs;
    /**
     * Stores the feedback document models.
     */
    final Map<Integer, DocumentModel> docModels;
    /**
     * IndexDataProvider instance.
     */
    final IndexDataProvider dataProv;

    /**
     * Initialize the abstract model with a set of query terms and feedback
     * documents.
     *
     * @param dataProv DataProvider instance from parent class
     * @param qt Query terms
     * @param fb Feedback documents
     * @throws IOException Thrown on low-level I/O-errors
     */
    AbstractModel(
        @NotNull final IndexDataProvider dataProv,
        @NotNull final BytesRefArray qt,
        @NotNull final DocIdSet fb)
        throws IOException {
      LOG.debug("Create runtime cache.");
      this.dataProv = dataProv;
      // add query terms, skip those not in index
      this.queryTerms = new BytesRefArray(Counter.newCounter(false));
      StreamUtils.stream(qt)
          .filter(queryTerm -> this.dataProv.getTermFrequency(queryTerm) > 0L)
          .forEach(this.queryTerms::append);

      this.docModels = new ConcurrentHashMap<>((int) (
          (double) DocIdSetUtils.cardinality(fb) * 1.8));

      LOG.info("Caching document models");
      this.feedbackDocs = StreamUtils.stream(fb)
          .peek(docId -> this.docModels.put(docId,
              this.dataProv.getDocumentModel(docId)))
          .toArray();
    }
  }

  /**
   * Class wrapping all methods needed for high-precision calculation of model
   * values. Also holds results of the calculations.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class ModelHighPrecision
      extends AbstractModel {
    /**
     * Logger instance for this class.
     */
    private static final org.slf4j.Logger LOG =
        LoggerFactory.getLogger(ModelHighPrecision.class);
    /**
     * Stores the static part of the query model calculation.
     */
    private final Map<Integer, BigDecimal> staticQueryModelParts;
    /**
     * Language model weighting parameter value.
     */
    private final BigDecimal docLangModelWeight;
    /**
     * Language model weighting parameter value remainder of 1d- value.
     */
    private final BigDecimal docLangModelWeight1Sub; // 1d - docLangModelWeight

    /**
     * Initialize the model calculation object.
     *
     * @param dataProv DataProvider instance from parent class
     * @param docLangModelWeight Language model weighting value
     * @param qt Query terms. Query terms not found in the collection (TF=0)
     * will be skipped.
     * @param fb Feedback documents
     * @throws IOException Thrown on low-level I/O-errors
     */
    ModelHighPrecision(
        @NotNull final IndexDataProvider dataProv,
        @NotNull final BigDecimal docLangModelWeight,
        @NotNull final BytesRefArray qt,
        @NotNull final DocIdSet fb)
        throws IOException {
      super(dataProv, qt, fb);

      // initialize other properties
      this.staticQueryModelParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));
      this.docLangModelWeight = docLangModelWeight;
      this.docLangModelWeight1Sub = BigDecimal.ONE.subtract(docLangModelWeight);

      LOG.info("Pre-calculating static query model values");
      // pre-calculate query term document models
      Arrays.stream(this.feedbackDocs).forEach(docId -> {
        final BigDecimal staticPart =
            StreamUtils.stream(this.queryTerms)
                .map(br -> document(
                    this.dataProv.getDocumentModel(docId), br))
                .reduce(BigDecimal.ONE, (r, c) -> r.multiply(c, MATH_CONTEXT));
        this.staticQueryModelParts.put(docId, staticPart);
      });
    }

    /**
     * Document model.
     *
     * @param docModel Document data model
     * @param term Term to calculate the document model value for
     * @return Document model value
     */
    BigDecimal document(
        @NotNull final DocumentModel docModel,
        @NotNull final BytesRef term) {
      return this.docLangModelWeight.multiply(
          BigDecimal.valueOf(docModel.relTf(term)), MATH_CONTEXT)
          .add(this.docLangModelWeight1Sub
                  .multiply(BigDecimal.valueOf(
                      this.dataProv.getRelativeTermFrequency(term)
                  ), MATH_CONTEXT),
              MATH_CONTEXT);
    }

    /**
     * Query model for all feedback documents.
     *
     * @param term Term to calculate the query model value for
     * @return Query model value for all feedback documents
     */
    BigDecimal query(@NotNull final BytesRef term) {
      return Arrays.stream(this.feedbackDocs)
          .mapToObj(d -> document(this.docModels.get(d), term)
              .multiply(this.staticQueryModelParts.get(d),
                  MATH_CONTEXT))
          .reduce(BigDecimal.ZERO, (sum, qm) -> sum.add(qm, MATH_CONTEXT),
              (sum1, sum2) -> sum1.add(sum2, MATH_CONTEXT));
    }
  }

  /**
   * Class wrapping all methods needed for low-precision calculation of model
   * values. Also holds results of the calculations.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class ModelLowPrecision
      extends AbstractModel {
    /**
     * Logger instance for this class.
     */
    private static final org.slf4j.Logger LOG =
        LoggerFactory.getLogger(ModelLowPrecision.class);
    /**
     * Stores the static part of the query model calculation.
     */
    private final Map<Integer, Double> staticQueryModelParts;
    /**
     * Language model weighting parameter value. (low precision)
     */
    private final double docLangModelWeight;
    /**
     * Language model weighting parameter value remainder of 1d- value. (low
     * precision)
     */
    private final double docLangModelWeight1Sub; // 1d - docLangModelWeight

    /**
     * Initialize the model calculation object.
     *
     * @param dataProv DataProvider instance from parent class
     * @param docLangModelWeight Language model weighting value
     * @param qt Query terms. Query terms not found in the collection (TF=0)
     * will be skipped.
     * @param fb Feedback documents
     * @throws IOException Thrown on low-level I/O-errors
     */
    ModelLowPrecision(
        @NotNull final IndexDataProvider dataProv,
        final double docLangModelWeight,
        @NotNull final BytesRefArray qt,
        @NotNull final DocIdSet fb)
        throws IOException {
      super(dataProv, qt, fb);

      // initialize other properties
      this.staticQueryModelParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));
      this.docLangModelWeight = docLangModelWeight;
      this.docLangModelWeight1Sub = 1d - docLangModelWeight;

      LOG.info("Pre-calculating static query model values");
      // pre-calculate query term document models
      Arrays.stream(this.feedbackDocs).forEach(docId -> {
        final double staticPart =
            StreamUtils.stream(this.queryTerms)
                .mapToDouble(br -> document(
                    this.dataProv.getDocumentModel(docId), br))
                .reduce(1d, (g, c) -> g * c);

        this.staticQueryModelParts.put(docId, staticPart);
      });
    }

    /**
     * Document model.
     *
     * @param docModel Document data model
     * @param term Term to calculate the document model value for
     * @return Document model value
     */
    double document(
        @NotNull final DocumentModel docModel,
        @NotNull final BytesRef term) {
      return (this.docLangModelWeight * docModel.relTf(term)) +
          (this.docLangModelWeight1Sub *
              this.dataProv.getRelativeTermFrequency(term));
    }

    /**
     * Query model for all feedback documents.
     *
     * @param term Term to calculate the query model value for
     * @return Query model value for all feedback documents
     */
    double query(@NotNull final BytesRef term) {
      return Arrays.stream(this.feedbackDocs)
          .mapToDouble(d -> document(this.docModels.get(d), term) *
              this.staticQueryModelParts.get(d))
          .sum();
    }
  }

  /**
   * Create a new instance using a builder.
   *
   * @param builder Builder to use for constructing the instance
   */
  @SuppressWarnings("WeakerAccess")
  DefaultClarityScore(@NotNull final Builder builder) {
    super(IDENTIFIER);

    // set configuration
    assert builder.getConfiguration() != null;
    this.conf = builder.getConfiguration();
    this.docLangModelWeight = this.conf.getLangModelWeight();

    this.conf.debugDump();

    assert builder.getIndexDataProvider() != null;
    this.dataProv = builder.getIndexDataProvider();
    assert builder.getAnalyzer() != null;
    this.analyzer = builder.getAnalyzer();

    this.vocProvider = builder.getVocabularyProvider();
    this.vocProvider.indexDataProvider(this.dataProv);

    this.fbProvider = builder.getFeedbackProvider();
    assert builder.getIndexReader() != null;
    this.fbProvider
        .dataProvider(this.dataProv)
        .indexReader(builder.getIndexReader())
        .analyzer(this.analyzer);
  }

  /**
   * Calculate the clarity score. This method does only pre-checks. The real
   * calculation is done in {@link #calculateClarity(Result, BytesRefArray,
   * DocIdSet)}.
   *
   * @param query Query used for term extraction
   * @return Clarity score result
   * @throws ClarityScoreCalculationException
   */
  @Override
  public Result calculateClarity(@NotNull final String query)
      throws ClarityScoreCalculationException {
    if (StringUtils.isStrippedEmpty(query)) {
      throw new IllegalArgumentException("Query was empty.");
    }

    LOG.info("Calculating clarity score. query={}", query);
    @Nullable
    final TimeMeasure timeMeasure;
    timeMeasure = LOG.isDebugEnabled() ? new TimeMeasure().start() : null;

    // get a normalized unique list of query terms
    // skips stopwords and removes unknown terms (not visible in current
    // fields, etc.)
    final BytesRefArray queryTerms = QueryUtils.tokenizeQuery(query,
        this.analyzer, this.dataProv);
    // check query term extraction result
    if (queryTerms == null || queryTerms.size() == 0) {
      final Result result = new Result();
      result.setEmpty(EmptyReason.NO_QUERY_TERMS);
      return result;
    }

    Result result = new Result();
    boolean resultNotEmpty = true;
    DocIdSet feedbackDocIds;
    int fbDocCount;
    try {
      feedbackDocIds = this.fbProvider
          .query(queryTerms)
          .fields(this.dataProv.getDocumentFields())
          .amount(this.conf.getFeedbackDocCount())
          .get();
      fbDocCount = DocIdSetUtils.cardinality(feedbackDocIds);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Feedback size: {} documents.", fbDocCount);
      }
    } catch (final TooManyClauses e) {
      resultNotEmpty = false;
      feedbackDocIds = EMPTY_DOCIDSET;
      fbDocCount = 0;
      result.setEmpty(EmptyReason.TOO_MANY_BOOLCLAUSES);
    } catch (final Exception e) {
      final String msg = "Caught exception while getting feedback documents.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    if (resultNotEmpty) {
      if (fbDocCount == 0) {
        resultNotEmpty = false;
        result.setEmpty(EmptyReason.NO_FEEDBACK);
      } else {
        try {
          result = calculateClarity(result, queryTerms, feedbackDocIds);
        } catch (final IOException e) {
          throw new ClarityScoreCalculationException("Calculation failed.", e);
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      assert timeMeasure != null;
      if (resultNotEmpty) {
        LOG.debug("Calculating default clarity score for query '{}' "
                + "with {} document models took {}. score={}",
            query, fbDocCount, timeMeasure.stop().getTimeString(),
            result.getScore());
      } else {
        String msg = "Calculating default clarity score for query '{}' "
            + "with {} document models took {}. Result is empty.";
        if (result.getEmptyReason().isPresent()) {
          msg += " reason=" + result.getEmptyReason().get();
        }
        LOG.debug(msg, query, fbDocCount, timeMeasure.stop().getTimeString());
      }
    }
    return result;
  }

  /**
   * Calculate the clarity score. Calculation is based on a set of feedback
   * documents and a list of query terms.
   *
   * @param result Result object
   * @param queryTerms Query terms
   * @param feedbackDocIds Feedback document ids to use
   * @return Result of the calculation
   * @throws IOException Thrown on low-level I/O-errors
   */
  private Result calculateClarity(
      @NotNull final Result result,
      @NotNull final BytesRefArray queryTerms,
      @NotNull final DocIdSet feedbackDocIds)
      throws IOException {
    result.setConf(this.conf);
    result.setFeedbackDocIds(feedbackDocIds);
    result.setQueryTerms(queryTerms);

    // object containing all methods for model calculations
    if (MATH_LOW_PRECISION) {
      // low precision math
      final ModelLowPrecision model =
          new ModelLowPrecision(this.dataProv, this.docLangModelWeight,
              queryTerms, feedbackDocIds);

      LOG.info("Calculating query models using feedback vocabulary. " +
          "(low precision)");
      // calculate query models
      final ScoreTupleLowPrecision[] dataSets = this.vocProvider
          .documentIds(feedbackDocIds).get()
          .map(term -> new ScoreTupleLowPrecision(
              model.query(term), this.dataProv.getRelativeTermFrequency(term)))
          .toArray(ScoreTupleLowPrecision[]::new);

      LOG.info("Calculating final score.");
      result.setScore(MathUtils.klDivergence(dataSets));
    } else {
      // high precision math
      final ModelHighPrecision model = new ModelHighPrecision(this.dataProv,
          BigDecimal.valueOf(this.docLangModelWeight),
          queryTerms, feedbackDocIds);

      LOG.info("Calculating query models using feedback vocabulary. " +
          "(high precision)");
      // calculate query models
      final ScoreTupleHighPrecision[] dataSets = this.vocProvider
          .documentIds(feedbackDocIds).get()
          .map(term -> new ScoreTupleHighPrecision(
              model.query(term),
              BigDecimal.valueOf(this.dataProv.getRelativeTermFrequency(term))))
          .toArray(ScoreTupleHighPrecision[]::new);

      LOG.info("Calculating final score.");
      result.setScore(MathUtils.klDivergence(dataSets).doubleValue());
    }

    return result;
  }

  /**
   * Extended result object containing additional meta information about what
   * values were actually used for calculation.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Result
      extends ClarityScoreResult {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Result.class);
    /**
     * Ids of feedback documents used for calculation.
     */
    @Nullable
    private BitSet feedbackDocIds;
    /**
     * Configuration that was used.
     */
    @Nullable
    private DefaultClarityScoreConfiguration conf;

    /**
     * Creates an object wrapping the result with meta information.
     */
    Result() {
      super(DefaultClarityScore.class);
      this.feedbackDocIds = null;
    }

    /**
     * Set the list of feedback documents used.
     *
     * @param fbDocIds List of feedback documents
     */
    void setFeedbackDocIds(@NotNull final DocIdSet fbDocIds) {
      try {
        this.feedbackDocIds = DocIdSetUtils.bits(fbDocIds);
      } catch (final IOException e) {
        LOG.error("Failed to retrieve ids for feedback documents.", e);
      }
    }

    /**
     * Set the configuration that was used.
     *
     * @param newConf Configuration used
     */
    void setConf(@NotNull final DefaultClarityScoreConfiguration newConf) {
      this.conf = Objects.requireNonNull(newConf);
    }

    /**
     * Configuration prefix.
     */
    private static final String CONF_PREFIX = IDENTIFIER + "-result";

    /**
     * Get the configuration used for this calculation result.
     *
     * @return Configuration used for this calculation result
     */
    @Nullable
    public DefaultClarityScoreConfiguration getConfiguration() {
      return this.conf;
    }
  }

  /**
   * Builder creating a new {@link DefaultClarityScore} scoring instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractCSCBuilder<Builder, DefaultClarityScore> {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

    @Override
    Builder getThis() {
      return this;
    }

    @Override
    DefaultClarityScoreConfiguration getConfiguration() {
      if (this.conf == null) {
        LOG.info("Using default configuration.");
        return new DefaultClarityScoreConfiguration();
      }
      return (DefaultClarityScoreConfiguration) this.conf;
    }

    @Override
    public DefaultClarityScore build()
        throws BuildableException {
      validateFeatures(
          Feature.CONFIGURATION,
          Feature.ANALYZER,
          Feature.DATA_PROVIDER,
          Feature.INDEX_READER);
      validateConfiguration(DefaultClarityScoreConfiguration.class);
      return new DefaultClarityScore(this);
    }
  }
}
