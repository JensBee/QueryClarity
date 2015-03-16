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

import de.unihildesheim.iw.Buildable.BuildableException;
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.Tuple.Tuple2;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.CollectionMetrics;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.scoring.ScoringResult.ScoringResultXml.Keys;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.lucene.util.DocIdSetUtils;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.util.MathUtils.KlDivergenceHighPrecision;
import de.unihildesheim.iw.util.MathUtils.KlDivergenceLowPrecision;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Improved Clarity Score implementation as described by Hauff, Murdock,
 * Baeza-Yates. <br> Reference <br> Hauff, Claudia, Vanessa Murdock, and Ricardo
 * Baeza-Yates. “Improved Query Difficulty Prediction for the Web.” In
 * Proceedings of the 17th ACM Conference on Information and Knowledge
 * Management, 439–448. CIKM ’08. New York, NY, USA: ACM, 2008.
 * doi:10.1145/1458082.1458142.
 *
 * @author Jens Bertram
 */
public final class ImprovedClarityScore
    extends AbstractClarityScoreCalculation {
  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link de.unihildesheim.iw.lucene.index
   * .IndexDataProvider}.
   */
  public static final String IDENTIFIER = "ICS";
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      ImprovedClarityScore.class);
  /**
   * Default math context for model calculations.
   */
  static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf()
          .getString(DefaultKeys.MATH_CONTEXT.toString(),
              GlobalConfiguration.DEFAULT_MATH_CONTEXT));
  /**
   * If true, low precision math is used for doing calculations.
   */
  private static final boolean MATH_LOW_PRECISION = GlobalConfiguration.conf()
      .getBoolean(DefaultKeys.MATH_LOW_PRECISION.toString(), false);
  /**
   * {@link IndexDataProvider} to use.
   */
  private final IndexDataProvider dataProv;
  /**
   * Lucene query analyzer.
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
  private final ImprovedClarityScoreConfiguration conf;

  /**
   * Abstract model class. Shared methods for low/high precision model
   * calculations.
   */
  private abstract static class AbstractModel {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
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
     * DataProvider instance from parent class.
     */
    final IndexDataProvider dataProv;
    /**
     * Collection metrics instance.
     */
    final CollectionMetrics cMetrics;

    /**
     * Initialize the abstract model with a set of query terms and feedback
     * documents.
     *
     * @param dataProv DataProvider
     * @param qt Query terms
     * @param fb Feedback documents
     * @throws IOException Thrown on low-level I/O-errors
     */
    AbstractModel(final IndexDataProvider dataProv,
        final BytesRefArray qt, final DocIdSet fb)
        throws IOException {
      LOG.debug("Create runtime cache.");
      this.dataProv = dataProv;
      this.cMetrics = dataProv.metrics();
      // add query terms, skip those not in index
      this.queryTerms = new BytesRefArray(Counter.newCounter(false));
      StreamUtils.stream(qt)
          .filter(queryTerm -> this.cMetrics.tf(queryTerm) > 0L)
          .forEach(this.queryTerms::append);

      // init feedback documents list
//      this.feedbackDocs = new int[DocIdSetUtils.cardinality(fb)];
      // init store for cached document models
//      this.docModels = new ConcurrentHashMap<>((int) (
//          (double) this.feedbackDocs.length * 1.8));
      this.docModels = new ConcurrentHashMap<>((int) (
          (double) DocIdSetUtils.cardinality(fb) * 1.8));

      LOG.info("Caching document models");
      this.feedbackDocs = StreamUtils.stream(fb)
          .peek(docId -> this.docModels.put(docId,
              this.cMetrics.docData(docId)))
          .toArray();

//      final DocIdSetIterator disi = fb.iterator();
//      int cnt = 0;
//      for (int docId = disi.nextDoc();
//           docId != DocIdSetIterator.NO_MORE_DOCS;
//           docId = disi.nextDoc()) {
//        // fill list of feedback documents
//        this.feedbackDocs[cnt++] = docId;
//        // pre-cache document models
//        this.docModels.put(docId, this.cMetrics.docData(docId));
//      }
    }
  }

  /**
   * Class wrapping all methods needed for calculation needed models. Also holds
   * results of the calculations.
   */
  private static final class ModelHighPrecision
      extends AbstractModel {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(ModelHighPrecision.class);
    /**
     * Stores the static part of the query model calculation for each document.
     */
    private final Map<Integer, BigDecimal> staticQueryModelParts;
    /**
     * Stores the static part of the smoothed model calculation for each
     * document.
     */
    private final Map<Integer, BigDecimal> staticSmoothingParts;
    /**
     * Document model calculation parameters: [0] Smoothing (mu), [1] Beta, [2]
     * Lambda, [3] 1 - Beta, [4] 1 - Lambda
     */
    private final BigDecimal[] dmParams;

    /**
     * Initialize the model calculation object.
     *
     * @param dataProv DataProvider instance from parent class
     * @param qt Query terms. Query terms not found in the collection (TF=0)
     * will be skipped.
     * @param fb Feedback documents
     * @param dmSmoothing Document model: Smoothing parameter value
     * @param dmBeta Document model: Beta parameter value
     * @param dmLambda Document model: Lambda parameter value
     * @throws IOException Thrown on low-level I/O-errors
     */
    ModelHighPrecision(final IndexDataProvider dataProv,
        final BytesRefArray qt, final DocIdSet fb,
        final double dmSmoothing, final double dmBeta, final double dmLambda)
        throws IOException {
      super(dataProv, qt, fb);

      this.dmParams = new BigDecimal[]{
          BigDecimal.valueOf(dmSmoothing),
          BigDecimal.valueOf(dmBeta),
          BigDecimal.valueOf(dmLambda),
          null, null // added afterwards
      };
      this.dmParams[3] = BigDecimal.ONE.subtract(
          this.dmParams[1], MATH_CONTEXT);
      this.dmParams[4] = BigDecimal.ONE.subtract(
          this.dmParams[2], MATH_CONTEXT);

      // initialize other properties
      this.staticQueryModelParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));
      this.staticSmoothingParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));

      LOG.info("Pre-calculating static query model and smoothing values");
      for (final Integer docId : this.feedbackDocs) {
        final DocumentModel docModel = this.cMetrics.docData(docId);

        // smoothing value
        final BigDecimal sSmooth = BigDecimal.valueOf(docModel.tf())
            .add(this.dmParams[0]
                .multiply(BigDecimal.valueOf((long) docModel.termCount()),
                    MATH_CONTEXT), MATH_CONTEXT);
        this.staticSmoothingParts.put(docId, sSmooth);

        // static query model part (needs smoothing values)
        final BigDecimal staticPart = StreamUtils.stream(this.queryTerms)
            .map(br -> document(docModel, br))
            .reduce(BigDecimal.ONE, (r, c) -> r.multiply(c, MATH_CONTEXT));
        this.staticQueryModelParts.put(docId, staticPart);
      }
    }

    /**
     * Document model.
     *
     * @param docModel Document data model
     * @param term Term to calculate the document model value for
     * @return Document model value
     */
    BigDecimal document(
        final DocumentModel docModel, final BytesRef term) {
      // collection model of current term
      final BigDecimal cModel = BigDecimal.valueOf(
          this.cMetrics.relTf(term));

      // smoothed document-term model
      final BigDecimal smoothing = BigDecimal.valueOf(docModel.tf(term))
          .add(this.dmParams[0].multiply(cModel), MATH_CONTEXT)
          .divide(this.staticSmoothingParts.get(docModel.id), MATH_CONTEXT);

      // final model calculation
      return this.dmParams[2]
          .multiply(this.dmParams[1].multiply(smoothing, MATH_CONTEXT)
              .add(this.dmParams[3].multiply(cModel,
                  MATH_CONTEXT), MATH_CONTEXT), MATH_CONTEXT)
          .add(this.dmParams[4].multiply(cModel,
              MATH_CONTEXT), MATH_CONTEXT);
    }

    /**
     * Query model for all feedback documents.
     *
     * @param term Term to calculate the query model value for
     * @return Query model value for all feedback documents
     */
    BigDecimal query(final BytesRef term) {
      return Arrays.stream(this.feedbackDocs)
          .mapToObj(d -> document(this.docModels.get(d), term)
              .multiply(this.staticQueryModelParts.get(d),
                  MATH_CONTEXT))
          .reduce(BigDecimal.ZERO, (sum, qm) -> sum.add(qm, MATH_CONTEXT));
    }
  }

  /**
   * Class wrapping all methods needed for low-precision calculation of model
   * values. Also holds results of the calculations.
   */
  private static final class ModelLowPrecision
      extends AbstractModel {
    /**
     * Logger instance for this class.
     */
    private static final org.slf4j.Logger LOG =
        LoggerFactory.getLogger(ModelLowPrecision.class);
    /**
     * Stores the static part of the query model calculation for each document.
     */
    private final Map<Integer, Double> staticQueryModelParts;
    /**
     * Stores the static part of the smoothed model calculation for each
     * document.
     */
    private final Map<Integer, Double> staticSmoothingParts;
    /**
     * Document model calculation parameters: [0] Smoothing (mu), [1] Beta, [2]
     * Lambda, [3] 1 - Beta, [4] 1 - Lambda
     */
    private final double[] dmParams;

    /**
     * Initialize the model calculation object.
     *
     * @param dataProv DataProvider instance from parent class
     * @param qt Query terms. Query terms not found in the collection (TF=0)
     * will be skipped.
     * @param fb Feedback documents
     * @param dmSmoothing Document model: Smoothing parameter value
     * @param dmBeta Document model: Beta parameter value
     * @param dmLambda Document model: Lambda parameter value
     * @throws IOException Thrown on low-level I/O-errors
     */
    ModelLowPrecision(final IndexDataProvider dataProv,
        final BytesRefArray qt, final DocIdSet fb,
        final double dmSmoothing, final double dmBeta, final double dmLambda)
        throws IOException {
      super(dataProv, qt, fb);

      this.dmParams = new double[]{dmSmoothing, dmBeta, dmLambda,
          1d - dmBeta, 1d - dmLambda};

      // initialize other properties
      this.staticQueryModelParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));
      this.staticSmoothingParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));

      LOG.info("Pre-calculating static query model and smoothing values");
      for (final Integer docId : this.feedbackDocs) {
        final DocumentModel docModel = this.cMetrics.docData(docId);

        // smoothing value
        final double sSmooth = (double) docModel.tf() +
            (this.dmParams[0] * (double) docModel.termCount());
        this.staticSmoothingParts.put(docId, sSmooth);

        // static query model part (needs smoothing values)
        final double staticPart = StreamUtils.stream(this.queryTerms)
            .mapToDouble(br -> document(docModel, br))
            .reduce(1d, (g, c) -> g * c);

        this.staticQueryModelParts.put(docId, staticPart);
      }
    }

    /**
     * Document model.
     *
     * @param docModel Document data model
     * @param term Term to calculate the document model value for
     * @return Document model value
     */
    double document(
        final DocumentModel docModel, final BytesRef term) {
      // collection model of current term
      final double cModel = this.cMetrics.relTf(term);

      // smoothed document-term model
      final double smoothing = ((double) docModel.tf(term) + (this.dmParams[0] *
          cModel)) / this.staticSmoothingParts.get(docModel.id);

      // final model calculation
      return (this.dmParams[2] * ((this.dmParams[1] * smoothing) +
          (this.dmParams[3] * cModel))) + (this.dmParams[4] * cModel);
    }

    /**
     * Query model for all feedback documents.
     *
     * @param term Term to calculate the query model value for
     * @return Query model value for all feedback documents
     */
    double query(final BytesRef term) {
      return Arrays.stream(this.feedbackDocs)
          .mapToDouble(d -> document(this.docModels.get(d), term) *
              this.staticQueryModelParts.get(d)).sum();
    }
  }

  /**
   * Create a new instance using a builder.
   *
   * @param builder Builder to use for constructing the instance
   */
  ImprovedClarityScore(final Builder builder) {
    super(IDENTIFIER);
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    this.dataProv = builder.getIndexDataProvider();
    this.conf = builder.getConfiguration();

    // check config
    if (this.conf.getMinFeedbackDocumentsCount() >
        this.dataProv.getDocumentCount()) {
      throw new IllegalStateException(
          "Required minimum number of feedback documents ("
              + this.conf.getMinFeedbackDocumentsCount() + ") is larger "
              + "or equal compared to the total amount of indexed documents "
              + '(' + this.dataProv.getDocumentCount()
              + "). Unable to provide feedback."
      );
    }
    this.conf.debugDump();

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
   * Calculates the improved clarity score for a given query.
   *
   * @param query Query to calculate for
   * @return Clarity score result object
   */
  @Override
  public Result calculateClarity(@NotNull final String query)
      throws ClarityScoreCalculationException {
    if (StringUtils.isStrippedEmpty(query)) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // result object
    final Result result = new Result(this.getClass());

    // cache metrics instance getting used frequently
    final CollectionMetrics cMetrics = this.dataProv.metrics();

    // get a normalized unique list of query terms
    // skips stopwords and removes unknown terms (not visible in current
    // fields, etc.)
    final BytesRefArray queryTerms = QueryUtils.tokenizeQuery(query,
        this.analyzer, cMetrics);
    // check query term extraction result
    if (queryTerms.size() == 0) {
      result.setEmpty("No query terms.");
      return result;
    }

    // save base data to result object
    result.setConf(this.conf);
    result.setQueryTerms(queryTerms);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // set of feedback documents to use for calculation.
    final DocIdSet feedbackDocIds;
    final int fbDocCount;
    try {
      feedbackDocIds = this.fbProvider
          .query(query)
          .fields(this.dataProv.getDocumentFields())
          .amount(
              this.conf.getMinFeedbackDocumentsCount(),
              this.conf.getMaxFeedbackDocumentsCount())
          .get();
      fbDocCount = DocIdSetUtils.cardinality(feedbackDocIds);
    } catch (final Exception e) {
      final String msg = "Caught exception while getting feedback documents.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    if (fbDocCount == 0) {
      result.setEmpty("No feedback documents.");
      return result;
    }
    if (fbDocCount < this.conf
        .getMinFeedbackDocumentsCount()) {
      result.setEmpty("Not enough feedback documents. " +
          this.conf.getMinFeedbackDocumentsCount() +
          " requested, " + fbDocCount + " retrieved.");
      return result;
    }
    result.setFeedbackDocIds(feedbackDocIds);

    // get document frequency threshold - allowed terms must be in bounds
    final BigDecimal minFreq = BigDecimal.valueOf(this.conf
        .getMinFeedbackTermSelectionThreshold());
    final BigDecimal maxFreq = BigDecimal.valueOf(this.conf
        .getMaxFeedbackTermSelectionThreshold());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Feedback term Document frequency threshold: {}%-{}%",
          minFreq.doubleValue() * 100d, maxFreq.doubleValue() * 100d);
    }

    // feedback terms stream used for calculation
    final Stream<BytesRef> fbTermStream = this.vocProvider
        .documentIds(feedbackDocIds)
        .get()
        .filter(t -> {
          final BigDecimal relDf = BigDecimal.valueOf(cMetrics.relDf(t));
          return relDf.compareTo(minFreq) >= 0
              && relDf.compareTo(maxFreq) <= 0;
        });

    try {
      if (MATH_LOW_PRECISION) {
        final ModelLowPrecision model = new ModelLowPrecision(
            this.dataProv, queryTerms, feedbackDocIds,
            this.conf.getDocumentModelSmoothingParameter(),
            this.conf.getDocumentModelParamBeta(),
            this.conf.getDocumentModelParamLambda());

        LOG.info("Calculating query models using feedback vocabulary. " +
            "(low precision)");
        final ScoreTupleLowPrecision[] dataSets = fbTermStream
            .map(term -> new ScoreTupleLowPrecision(
                model.query(term), cMetrics.relTf(term)))
            .toArray(ScoreTupleLowPrecision[]::new);

        LOG.info("Calculating final score.");
        result.setScore(
            KlDivergenceLowPrecision.sumAndCalc(dataSets));
      } else {
        final ModelHighPrecision model = new ModelHighPrecision(
            this.dataProv, queryTerms, feedbackDocIds,
            this.conf.getDocumentModelSmoothingParameter(),
            this.conf.getDocumentModelParamBeta(),
            this.conf.getDocumentModelParamLambda());

        LOG.info("Calculating query models using feedback vocabulary. " +
            "(high precision)");
        final ScoreTupleHighPrecision[] dataSets = fbTermStream
            .map(term -> new ScoreTupleHighPrecision(
                model.query(term), BigDecimal.valueOf(cMetrics.relTf(term))))
            .toArray(ScoreTupleHighPrecision[]::new);

        LOG.info("Calculating final score.");
        result.setScore(
            KlDivergenceHighPrecision.sumAndCalc(dataSets).doubleValue());
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Calculating improved clarity score for query {} "
              + "with {} document models took {}. {}",
          query, fbDocCount,
          timeMeasure.stop().getTimeString(), result.getScore());
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
     * Configuration that was used.
     */
    @Nullable
    private ImprovedClarityScoreConfiguration conf;
    /**
     * Ids of feedback documents used for calculation.
     */
    @Nullable
    private BitSet feedbackDocIds;

    /**
     * Creates an object wrapping the result with meta information.
     *
     * @param cscType Type of the calculation class
     */
    public Result(final Class<? extends ClarityScoreCalculation> cscType) {
      super(cscType);
    }

    /**
     * Set the list of feedback documents used.
     *
     * @param fbDocIds List of feedback documents
     */
    void setFeedbackDocIds(@Nullable final DocIdSet fbDocIds) {
      if (fbDocIds != null) {
        try {
          this.feedbackDocIds = DocIdSetUtils.bits(fbDocIds);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    /**
     * Set the configuration that was used.
     *
     * @param newConf Configuration used
     */
    void setConf(@NotNull final ImprovedClarityScoreConfiguration newConf) {
      this.conf = newConf;
    }

    /**
     * Get the configuration used for this calculation result.
     *
     * @return Configuration used for this calculation result
     */
    @Nullable
    public ImprovedClarityScoreConfiguration getConfiguration() {
      return this.conf;
    }

    /**
     * Configuration prefix.
     */
    private static final String CONF_PREFIX = IDENTIFIER + "-result";

    @Override
    public ScoringResultXml getXml() {
      final ScoringResultXml xml = new ScoringResultXml();

      getXml(xml);
      // number of feedback documents
      if (this.feedbackDocIds == null) {
        // unknown - maybe an error
        xml.getItems().put(Keys.FEEDBACK_DOCUMENTS.toString(), "-1");
      } else {
        final int fbDocCount = this.feedbackDocIds.cardinality();
        xml.getItems().put(
            Keys.FEEDBACK_DOCUMENTS.toString(),
            Integer.toString(fbDocCount));

        // feedback documents
        if (GlobalConfiguration.conf()
            .getAndAddBoolean(CONF_PREFIX + "ListFeedbackDocuments", true)) {
          final List<Tuple2<String, String>> fbDocsList =
              new ArrayList<>(fbDocCount);

          for (int i = this.feedbackDocIds.nextSetBit(0);
               i != DocIdSetIterator.NO_MORE_DOCS;
               i = this.feedbackDocIds.nextSetBit(++i)) {
            fbDocsList.add(Tuple.tuple2(
                Keys.FEEDBACK_DOCUMENT_KEY.toString(), Integer.toString(i)));
          }
          xml.getLists().put(
              Keys.FEEDBACK_DOCUMENTS.toString(), fbDocsList);
        }
      }

      return xml;
    }
  }

  /**
   * Builder to create a new {@link ImprovedClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractBuilder<
      ImprovedClarityScore, Builder> {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(Builder.class);

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    ImprovedClarityScoreConfiguration getConfiguration() {
      if (this.conf == null) {
        LOG.info("Using default configuration.");
        return new ImprovedClarityScoreConfiguration();
      }
      return (ImprovedClarityScoreConfiguration) this.conf;
    }

    @Override
    public ImprovedClarityScore build()
        throws BuildableException {
      validateFeatures(new Feature[]{
          Feature.CONFIGURATION,
          Feature.ANALYZER,
          Feature.DATA_PROVIDER,
          Feature.INDEX_READER
      });
      validateConfiguration(ImprovedClarityScoreConfiguration.class);
      return new ImprovedClarityScore(this);
    }
  }
}
