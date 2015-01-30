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
import de.unihildesheim.iw.Closable;
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
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils.KlDivergence;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall.TargetFunc;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.jetbrains.annotations.Nullable;
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
 * Default Clarity Score implementation as described by Cronen-Townsend, Steve,
 * Yun Zhou, and W. Bruce Croft. <br> Reference: <br> “Predicting Query
 * Performance.” In Proceedings of the 25th Annual International ACM SIGIR
 * Conference on Research and Development in Information Retrieval, 299–306.
 * SIGIR ’02. New York, NY, USA: ACM, 2002. doi:10.1145/564376.564429.
 *
 * @author Jens Bertram
 */
public final class DefaultClarityScore
    implements ClarityScoreCalculation {
  /**
   * Prefix used to identify externally stored data.
   */
  static final String IDENTIFIER = "DCS";
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      DefaultClarityScore.class);
  /**
   * Context for high precision math calculations.
   */
  private static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf().getString(
          DefaultKeys.MATH_CONTEXT.toString()));
  /**
   * {@link IndexDataProvider} to use.
   */
  private final IndexDataProvider dataProv;
  /**
   * Reader to access Lucene index.
   */
  private final IndexReader idxReader;
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
   * Language model weighting parameter value.
   */
  private final BigDecimal docLangModelWeight;
  /**
   * Language model weighting parameter value remainder of 1d- value.
   */
  private final BigDecimal docLangModelWeight1Sub; // 1d - docLangModelWeight

  /**
   * Class wrapping all methods needed for calculation needed models. Also holds
   * results of the calculations.
   */
  private final class Model
      extends TargetFunc<ByteArray>
      implements Closable {
    /**
     * List of query terms issued.
     */
    private final Collection<ByteArray> queryTerms;
    /**
     * List of feedback documents to use.
     */
    private final Collection<Integer> feedbackDocs;
    /**
     * Stores the static part of the query model calculation.
     */
    private final Map<Integer, BigDecimal> staticQueryModelParts;
    /**
     * Stores the feedback document models.
     */
    private final Map<Integer, DocumentModel> docModels;
    /**
     * Caches values of document and query models. Those need to be normalized
     * before calculating the score.
     */
    @SuppressWarnings("ProtectedField")
    protected final Map<Long, Tuple2<BigDecimal, BigDecimal>> dataSets;
    /**
     * Counter for entries in {@link #dataSets}. Gets used as map key.
     */
    private final AtomicLong dataSetCounter;

    /**
     * Initialize the model calculation object.
     *
     * @param qt Query terms. Query terms not found in the collection (TF=0)
     * will be skipped.
     * @param fb Feedback documents
     * @throws DataProviderException Forwarded from lower-level
     */
    private Model(final Collection<ByteArray> qt, final Collection<Integer> fb)
        throws DataProviderException {
      LOG.debug("Create runtime cache.");

      // add query terms, skip those not in index
      this.queryTerms = new ArrayList<>(qt.size());
      for (final ByteArray queryTerm : qt) {
        if (DefaultClarityScore.this.dataProv.metrics().tf(queryTerm) > 0L) {
          this.queryTerms.add(queryTerm);
        }
      }

      // add feedback documents
      this.feedbackDocs = new ArrayList<>(fb.size());
      this.feedbackDocs.addAll(fb);

      // initialize other properties
      this.staticQueryModelParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.size() * 1.8));
      this.docModels = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.size() * 1.8));
      // final size depends on feedback vocabulary amount
      this.dataSets = new ConcurrentHashMap<>(70000);
      this.dataSetCounter = new AtomicLong(0L);

      LOG.info("Pre-calculating static query model values");
      // pre-calculate query term document models
      for (final Integer docId : this.feedbackDocs) {
        final DocumentModel docModel = DefaultClarityScore.this.dataProv
            .metrics().docData(docId);
        BigDecimal staticPart = BigDecimal.ONE;
        for (final ByteArray queryTerm : this.queryTerms) {
          staticPart = staticPart.multiply(
              document(docModel, queryTerm), MATH_CONTEXT);
        }
        this.staticQueryModelParts.put(docModel.id, staticPart);
      }

      LOG.info("Caching document models");
      for (final Integer docId : this.feedbackDocs) {
        this.docModels.put(docId,
            DefaultClarityScore.this.dataProv.metrics().docData(docId));
      }
    }

    /**
     * Document model.
     *
     * @param docModel Document data model
     * @param term Term to calculate the document model value for
     * @return Document model value
     * @throws DataProviderException Forwarded from lower-level
     */
    final BigDecimal document(
        final DocumentModel docModel, final ByteArray term)
        throws DataProviderException {
      return DefaultClarityScore.this.docLangModelWeight.multiply(
          BigDecimal.valueOf(docModel.relTf(term)), MATH_CONTEXT)
          .add(DefaultClarityScore.this.docLangModelWeight1Sub
              .multiply(DefaultClarityScore.this.dataProv.metrics().relTf(term),
                  MATH_CONTEXT), MATH_CONTEXT);
    }

    /**
     * Query model for all feedback documents.
     *
     * @param term Term to calculate the query model value for
     * @return Query model value for all feedback documents
     * @throws DataProviderException Forwarded from lower-level
     */
    final BigDecimal query(final ByteArray term)
        throws DataProviderException {
      BigDecimal result = BigDecimal.ZERO;

      for (final Integer docId : this.feedbackDocs) {
        result = result.add(document(this.docModels.get(docId), term)
            .multiply(this.staticQueryModelParts.get(docId),
                MATH_CONTEXT), MATH_CONTEXT);
      }

      return result;
    }

    @Override
    public void close() {
      LOG.debug("Close runtime cache.");
    }

    Tuple2<BigDecimal, BigDecimal> calulateQModForFeedbackTerm(final
        ByteArray term) {
      try {
        return Tuple.tuple2(query(term),
                DefaultClarityScore.this.dataProv.metrics().relTf(term));
      } catch (final DataProviderException e) {
        LOG.error("Failed processing term. t={}",
            ByteArrayUtils.utf8ToString(term));
        throw new IllegalStateException("Error while processing term.");
      }
    }

    void addDataSet(final Tuple2<BigDecimal, BigDecimal> t2) {
      this.dataSets.put(
          this.dataSetCounter.incrementAndGet(), t2);
    }

    /**
     * {@link Processing} callback function to calculate the score portion
     * for a given term using already calculated query models.
     */
    @Override
    public void call(@Nullable final ByteArray term) {
      if (term != null) {
        try {
          this.dataSets.put(
              this.dataSetCounter.incrementAndGet(),
              Tuple.tuple2(query(term),
                  DefaultClarityScore.this.dataProv.metrics().relTf(term)));
        } catch (final DataProviderException e) {
          LOG.error("Failed processing term. t={}",
              ByteArrayUtils.utf8ToString(term));
          throw new IllegalStateException("Error while processing term.");
        }
      }
    }
  }

  /**
   * Create a new instance using a builder.
   *
   * @param builder Builder to use for constructing the instance
   */
  private DefaultClarityScore(final Builder builder) {
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    this.conf = builder.getConfiguration();
    // localize some values for time critical calculations
    this.docLangModelWeight = BigDecimal.valueOf(
        this.conf.getLangModelWeight());
    this.docLangModelWeight1Sub = BigDecimal.valueOf(
        1d - this.conf.getLangModelWeight());
    this.conf.debugDump();

    this.dataProv = builder.getIndexDataProvider();
    this.idxReader = builder.getIndexReader();
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
  }

  /**
   * Close this instance and release any resources.
   */
  @Override
  public void close() {
    // NOP
  }

  /**
   * Calculate the clarity score. This method does only pre-checks. The real
   * calculation is done in {@link #calculateClarity(Collection, Set)}.
   *
   * @param query Query used for term extraction
   * @return Clarity score result
   * @throws ClarityScoreCalculationException
   */
  @Override
  public Result calculateClarity(final String query)
      throws ClarityScoreCalculationException, DataProviderException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final Set<Integer> feedbackDocIds;
    try {
      feedbackDocIds = this.fbProvider
          .indexReader(this.idxReader)
          .analyzer(this.analyzer)
          .query(query)
          .fields(this.dataProv.getDocumentFields())
          .amount(this.conf.getFeedbackDocCount())
          .get();
    } catch (final Exception e) {
      final String msg = "Caught exception while getting feedback documents.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    if (feedbackDocIds.isEmpty()) {
      final Result result = new Result();
      result.setEmpty("No feedback documents.");
      return result;
    }

    final Collection<ByteArray> queryTerms = QueryUtils.tokenizeQuery(query,
        this.analyzer, this.dataProv.metrics());

    // check query term extraction result
    if (queryTerms == null || queryTerms.isEmpty()) {
      final Result result = new Result();
      result.setEmpty("No query terms.");
      return result;
    }

    try {
      final Result r = calculateClarity(queryTerms, feedbackDocIds);
      LOG.debug("Calculating default clarity score for query '{}' "
              + "with {} document models took {}. {}", query,
          feedbackDocIds.size(), timeMeasure.stop().getTimeString(),
          r.getScore()
      );
      return r;
    } catch (final DataProviderException | ProcessingException e) {
      timeMeasure.stop();
      final String msg = "Caught exception while calculating score.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }
  }

  /**
   * Calculate the clarity score. Calculation is based on a set of feedback
   * documents and a list of query terms.
   *
   * @param queryTerms Query terms
   * @param feedbackDocIds Feedback document ids to use
   * @return Result of the calculation
   * @throws ProcessingException Thrown if any of the threaded calculations
   * encountered an error
   * @throws DataProviderException Thrown on low-level errors
   */
  private Result calculateClarity(
      final Collection<ByteArray> queryTerms,
      final Set<Integer> feedbackDocIds)
      throws ProcessingException, DataProviderException {
    final Result result = new Result();
    result.setConf(this.conf);
    result.setFeedbackDocIds(feedbackDocIds);
    result.setQueryTerms(queryTerms);

    // object containing all methods for model calculations
    final Model model = new Model(queryTerms, feedbackDocIds);

    LOG.info("Calculating query models using feedback vocabulary.");
    // calculate query models
    this.vocProvider
        .documentIds(feedbackDocIds)
        .getStream()
        .map(model::calulateQModForFeedbackTerm)
        .forEach(model::addDataSet);

    LOG.info("Calculating final score.");
    result.setScore(
        KlDivergence.calc(
            model.dataSets.values(),
            KlDivergence.sumValues(model.dataSets.values())
        ).doubleValue());

    model.close();
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
     * Ids of feedback documents used for calculation.
     */
    private Collection<Integer> feedbackDocIds;
    /**
     * Configuration that was used.
     */
    private DefaultClarityScoreConfiguration conf;

    /**
     * Creates an object wrapping the result with meta information.
     */
    Result() {
      super(DefaultClarityScore.class);
      this.feedbackDocIds = Collections.emptyList();
    }

    /**
     * Set the list of feedback documents used.
     *
     * @param fbDocIds List of feedback documents
     */
    void setFeedbackDocIds(final Collection<Integer> fbDocIds) {
      Objects.requireNonNull(fbDocIds);

      this.feedbackDocIds = new ArrayList<>(fbDocIds.size());
      this.feedbackDocIds.addAll(fbDocIds);
    }

    /**
     * Set the configuration that was used.
     *
     * @param newConf Configuration used
     */
    void setConf(final DefaultClarityScoreConfiguration newConf) {
      this.conf = Objects.requireNonNull(newConf);
    }

    /**
     * Get the collection of feedback documents used for calculation.
     *
     * @return Feedback documents used for calculation
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public Collection<Integer> getFeedbackDocuments() {
      return Collections.unmodifiableCollection(this.feedbackDocIds);
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
    public DefaultClarityScoreConfiguration getConfiguration() {
      return this.conf;
    }

    /**
     * Provides information about the query issued and the feedback documents
     * used.
     *
     * @return Object containing information to include in result XML
     */
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
        xml.getLists().put(
            Keys.FEEDBACK_DOCUMENTS.toString(), fbDocsList);
      }

      return xml;
    }
  }

  /**
   * Builder to create a new {@link DefaultClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends ClarityScoreCalculationBuilder<DefaultClarityScore,
      DefaultClarityScoreConfiguration> {

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
    public DefaultClarityScore build()
        throws BuildableException {
      validate();
      return new DefaultClarityScore(this);
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
