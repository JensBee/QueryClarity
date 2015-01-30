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
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.Tuple.Tuple2;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.scoring.ScoringResult.ScoringResultXml.Keys;
import de.unihildesheim.iw.lucene.scoring.data.DefaultFeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultVocabularyProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.util.MathUtils.KlDivergence;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
    implements ClarityScoreCalculation {
  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link de.unihildesheim.iw.lucene.index
   * .IndexDataProvider}.
   */
  static final String IDENTIFIER = "ICS";
  /**
   * Logger instance for this class.
   */
  static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      ImprovedClarityScore.class);
  /**
   * Default math context for model calculations.
   */
  private static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf().getString(
          DefaultKeys.MATH_CONTEXT.toString()));
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
   * Class wrapping all methods needed for calculation needed models. Also holds
   * results of the calculations.
   */
  private class Model {
    /**
     * List of query terms issued.
     */
    private final Collection<ByteArray> queryTerms;
    /**
     * List of feedback documents to use.
     */
    private final Collection<Integer> feedbackDocs;
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
     * Caches values of document and query models. Those need to be normalized
     * before calculating the score.
     */
    private final Map<Long, Tuple2<BigDecimal, BigDecimal>> dataSets;
    /**
     * Counter for entries in {@link #dataSets}. Gets used as map key.
     */
    private final AtomicLong dataSetCounter;
    /**
     * Stores the feedback document models.
     */
    private final Map<Integer, DocumentModel> docModels;

    /**
     * Initialize the model calculation object.
     *
     * @param qt Query terms. Query terms not found in the collection (TF=0)
     * will be skipped.
     * @param fb Feedback documents
     * @param dmSmoothing Document model: Smoothing parameter value
     * @param dmBeta Document model: Beta parameter value
     * @param dmLambda Document model: Lambda parameter value
     */
    private Model(final Collection<ByteArray> qt,
        final Collection<Integer> fb,
        final double dmSmoothing, final double dmBeta, final double dmLambda) {
      LOG.debug("Create runtime cache.");

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

      // add query terms, skip those not in index
      this.queryTerms = new ArrayList<>(qt.size());
      for (final ByteArray queryTerm : qt) {
        if (ImprovedClarityScore.this.dataProv.metrics().tf(queryTerm) > 0L) {
          this.queryTerms.add(queryTerm);
        }
      }

      // add feedback documents
      this.feedbackDocs = new ArrayList<>(fb.size());
      this.feedbackDocs.addAll(fb);

      // initialize other properties
      this.staticQueryModelParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.size() * 1.8));
      this.staticSmoothingParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.size() * 1.8));
      this.docModels = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.size() * 1.8));
      this.dataSets = new ConcurrentHashMap<>(2000);
      this.dataSetCounter = new AtomicLong(0L);

      LOG.info("Pre-calculating static query model and smoothing values");
      for (final Integer docId : this.feedbackDocs) {
        final DocumentModel docModel = ImprovedClarityScore.this.dataProv
            .metrics().docData(docId);
        // smoothing value
        final BigDecimal sSmooth = BigDecimal.valueOf(docModel.tf())
            .add(this.dmParams[0]
                .multiply(BigDecimal.valueOf(docModel.termCount()),
                    MATH_CONTEXT), MATH_CONTEXT);
        this.staticSmoothingParts.put(docId, sSmooth);

        // static query model part (needs smoothing values)
        BigDecimal staticPart = BigDecimal.ONE;
        for (final ByteArray queryTerm : this.queryTerms) {
          staticPart = staticPart.multiply(
              document(docModel, queryTerm), MATH_CONTEXT);
        }
        this.staticQueryModelParts.put(docId, staticPart);
      }

      LOG.info("Caching document models");
      for (final Integer docId : this.feedbackDocs) {
        this.docModels.put(docId,
            ImprovedClarityScore.this.dataProv.metrics().docData(docId));
      }
    }

    /**
     * Document model.
     *
     * @param docModel Document data model
     * @param term Term to calculate the document model value for
     * @return Document model value
     */
    final BigDecimal document(
        final DocumentModel docModel, final ByteArray term) {
      // collection model of current term
      final BigDecimal cModel = ImprovedClarityScore.this.dataProv
          .metrics().relTf(term);

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
    final BigDecimal query(final ByteArray term) {
      return this.feedbackDocs.stream()
          .map(d -> document(this.docModels.get(d), term)
              .multiply(this.staticQueryModelParts.get(d),
                  MATH_CONTEXT))
          .reduce((x, y) -> x.add(y, MATH_CONTEXT)).get();
      /*
      BigDecimal result = BigDecimal.ZERO;

      for (final Integer docId : this.feedbackDocs) {
        result = result.add(document(this.docModels.get(docId), term)
            .multiply(this.staticQueryModelParts.get(docId),
                MATH_CONTEXT), MATH_CONTEXT);
      }
      return result;
      */
    }
  }

  /**
   * Create a new instance using a builder.
   *
   * @param builder Builder to use for constructing the instance
   */
  private ImprovedClarityScore(final Builder builder) {
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
              + "(" + this.dataProv.getDocumentCount()
              + "). Unable to provide feedback."
      );
    }
    this.conf.debugDump();

    this.analyzer = builder.getAnalyzer();

    if (builder.getVocabularyProvider() != null) {
      this.vocProvider = builder.getVocabularyProvider();
    } else {
      this.vocProvider = new DefaultVocabularyProvider();
    }
    this.vocProvider.indexDataProvider(this.dataProv);

    if (builder.getFeedbackProvider() != null) {
      this.fbProvider = builder.getFeedbackProvider();
    } else {
      this.fbProvider = new DefaultFeedbackProvider();
    }
    this.fbProvider
        .indexReader(builder.getIndexReader())
        .analyzer(this.analyzer);
  }


  /**
   * Close this instance and release any resources.
   */
  @Override
  public void close() {
    // NOP
  }

  /**
   * Calculates the improved clarity score for a given query.
   *
   * @param query Query to calculate for
   * @return Clarity score result object
   */
  @Override
  public Result calculateClarity(final String query)
      throws ClarityScoreCalculationException, DataProviderException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // result object
    final Result result = new Result(this.getClass());

    // get a normalized unique list of query terms
    // skips stopwords and removes unknown terms
    final Collection<ByteArray> queryTerms = QueryUtils.tokenizeQuery(query,
        this.analyzer, this.dataProv.metrics());

    // check query term extraction result
    if (queryTerms.isEmpty()) {
      result.setEmpty("No query terms.");
      return result;
    }

    // save base data to result object
    result.setConf(this.conf);
    result.setQueryTerms(queryTerms);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // set of feedback documents to use for calculation.
    final Set<Integer> feedbackDocIds;
    try {
      feedbackDocIds = this.fbProvider
          .query(query)
          .fields(this.dataProv.getDocumentFields())
          .amount(
              this.conf.getMinFeedbackDocumentsCount(),
              this.conf.getMaxFeedbackDocumentsCount())
          .get();
    } catch (final Exception e) {
      final String msg = "Caught exception while getting feedback documents.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    if (feedbackDocIds.isEmpty()) {
      result.setEmpty("No feedback documents.");
      return result;
    } else if (feedbackDocIds.size() < this.conf
        .getMinFeedbackDocumentsCount()) {
      result.setEmpty("Not enough feedback documents. " +
          this.conf.getMinFeedbackDocumentsCount() +
          " requested, " + feedbackDocIds.size() + " retrieved.");
      return result;
    }
    result.setFeedbackDocIds(feedbackDocIds);

    // get document frequency threshold - allowed terms must be in bounds
    final BigDecimal minFreq = BigDecimal.valueOf(this.conf
        .getMinFeedbackTermSelectionThreshold());
    final BigDecimal maxFreq = BigDecimal.valueOf(this.conf
        .getMaxFeedbackTermSelectionThreshold());
    LOG.debug("Feedback term Document frequency threshold: {}%-{}%",
        minFreq.doubleValue() * 100d, maxFreq.doubleValue() * 100d);

    final Model model = new Model(queryTerms, feedbackDocIds,
        this.conf.getDocumentModelSmoothingParameter(), this.conf
        .getDocumentModelParamBeta(), this.conf.getDocumentModelParamLambda());

    LOG.info("Calculating query models using feedback vocabulary.");
    this.vocProvider
        .documentIds(feedbackDocIds)
        .get()
        .filter(t -> {
          final BigDecimal relDf = this.dataProv.metrics().relDf(t);
          return (relDf.compareTo(minFreq) >= 0
              && relDf.compareTo(maxFreq) <= 0);
        })
        .map(term ->
            Tuple.tuple2(
                model.query(term), this.dataProv.metrics().relTf(term)))
        .forEach(t2 ->
            model.dataSets.put(model.dataSetCounter.incrementAndGet(), t2));

    LOG.info("Calculating final score.");
    result.setScore(
        KlDivergence.sumAndCalc(model.dataSets.values()).doubleValue());

    LOG.debug("Calculating improved clarity score for query {} "
            + "with {} document models took {}. {}",
        query, feedbackDocIds.size(),
        timeMeasure.stop().getTimeString(), result.getScore());

    return result;
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
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
    private ImprovedClarityScoreConfiguration conf;
    /**
     * Ids of feedback documents used for calculation.
     */
    private Set<Integer> feedbackDocIds;

    /**
     * Creates an object wrapping the result with meta information.
     *
     * @param cscType Type of the calculation class
     */
    public Result(final Class<? extends ClarityScoreCalculation> cscType) {
      super(cscType);
      this.feedbackDocIds = Collections.emptySet();
    }

    /**
     * Set the list of feedback documents used.
     *
     * @param fbDocIds List of feedback documents
     */
    void setFeedbackDocIds(final Set<Integer> fbDocIds) {
      this.feedbackDocIds = Collections.unmodifiableSet(
          Objects.requireNonNull(fbDocIds, "Feedback documents were null."));
    }

    /**
     * Set the configuration that was used.
     *
     * @param newConf Configuration used
     */
    void setConf(final ImprovedClarityScoreConfiguration newConf) {
      this.conf = Objects.requireNonNull(newConf, "Configuration was null.");
    }

    /**
     * Get the configuration used for this calculation result.
     *
     * @return Configuration used for this calculation result
     */
    public ImprovedClarityScoreConfiguration getConfiguration() {
      return this.conf;
    }

    /**
     * Configuration prefix.
     */
    private static final String CONF_PREFIX = IDENTIFIER + "-result";

    /**
     * Get the collection of feedback documents used for calculation.
     *
     * @return Feedback documents used for calculation
     */
    public Set<Integer> getFeedbackDocuments() {
      return Collections.unmodifiableSet(this.feedbackDocIds);
    }

    @Override
    public ScoringResultXml getXml() {
      final ScoringResultXml xml = new ScoringResultXml();

      getXml(xml);
      // number of feedback documents
      xml.getItems().put(
          Keys.FEEDBACK_DOCUMENTS.toString(),
          Integer.toString(this.feedbackDocIds.size()));

      // feedback documents
      if (GlobalConfiguration.conf()
          .getAndAddBoolean(CONF_PREFIX + "ListFeedbackDocuments",
              Boolean.TRUE)) {
        final List<Tuple2<String, String>> fbDocsList = new ArrayList<>
            (this.feedbackDocIds.size());
        for (final Integer docId : this.feedbackDocIds) {
          fbDocsList.add(Tuple.tuple2(
              Keys.FEEDBACK_DOCUMENT_KEY.toString(),
              docId.toString()));
        }
        xml.getLists().put(Keys.FEEDBACK_DOCUMENTS.toString(), fbDocsList);
      }

      return xml;
    }
  }

  /**
   * Builder to create a new {@link ImprovedClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends ClarityScoreCalculationBuilder<ImprovedClarityScore,
      ImprovedClarityScoreConfiguration> {
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
    public ImprovedClarityScore build()
        throws BuildableException {
      validate();
      return new ImprovedClarityScore(this);
    }

    @Override
    public void validate()
        throws ConfigurationException {
      new Validator(this, new Feature[]{
          Feature.CONFIGURATION,
          Feature.ANALYZER,
          Feature.CACHE,
          Feature.DATA_PATH,
          Feature.DATA_PROVIDER,
          Feature.INDEX_READER
      });
    }
  }
}
