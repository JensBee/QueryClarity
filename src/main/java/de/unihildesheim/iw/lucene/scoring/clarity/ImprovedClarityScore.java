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
import de.unihildesheim.iw.Closable;
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.document.Feedback;
import de.unihildesheim.iw.lucene.index.ExternalDocTermDataManager;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.query.TryExactTermsQuery;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.AtomicDouble;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.mapdb.Atomic;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    implements ClarityScoreCalculation, Closable {

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
   * Provider for general index metrics.
   */
  private final Metrics metrics;
  /**
   * {@link IndexDataProvider} to use.
   */
  private final IndexDataProvider dataProv;
  /**
   * Reader to access the Lucene index.
   */
  private final IndexReader idxReader;
  /**
   * Lucene query analyzer.
   */
  private final Analyzer analyzer;
  /**
   * Synchronization lock for document model calculation.
   */
  private final Object docModelCalcSync = new Object();
  /**
   * Synchronization lock for document model map calculation.
   */
  private final Object docModelMapCalcSync = new Object();
  /**
   * Configuration object used for all parameters of the calculation.
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private ImprovedClarityScoreConfiguration conf;
  /**
   * Database instance.
   */
  private Persistence persist;
  /**
   * Manager for extended document meta-data.
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private ExternalDocTermDataManager extDocMan;
  /**
   * Flag indicating, if a cache is available.
   */
  private boolean hasCache;
  /**
   * Cached storage of Document-id -> Term, model-value.
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private Map<Integer, Map<ByteArray, Double>> docModelDataCache;
  /**
   * Flag indicating, if this instance has been closed.
   */
  private volatile boolean isClosed;

  /**
   * Create a new instance using a builder.
   *
   * @param builder Builder to use for constructing the instance
   * @throws Buildable.BuildableException Thrown, if building the cache instance
   * failed
   */
  ImprovedClarityScore(final Builder builder)
      throws Buildable.BuildableException {
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    this.dataProv = builder.idxDataProvider;
    this.idxReader = builder.idxReader;
    this.metrics = new Metrics(builder.idxDataProvider);
    this.setConfiguration(builder.configuration);
    this.analyzer = builder.analyzer;
    this.initCache(builder);
  }

  /**
   * Set the configuration to use by this instance.
   *
   * @param newConf Configuration
   */
  private void setConfiguration(
      final ImprovedClarityScoreConfiguration newConf) {
    this.conf = newConf;
    newConf.debugDump();
    this.docModelDataCache = new ConcurrentHashMap<>(
        this.conf.getMaxFeedbackDocumentsCount());
    parseConfig();
  }

  /**
   * Initializes a cache using the {@link Builder}.
   *
   * @param builder Builder instance
   * @throws Buildable.BuildableException Thrown, if building the cache instance
   * failed
   */
  private void initCache(final Builder builder)
      throws Buildable.BuildableException {
    final Persistence.Builder psb = builder.persistenceBuilder;

    final Persistence persistence;
    boolean createNew = false;
    boolean rebuild = false;
    switch (psb.getCacheLoadInstruction()) {
      case MAKE:
        persistence = psb.make().build();
        createNew = true;
        break;
      case GET:
        persistence = psb.get().build();
        break;
      default:
        if (!psb.dbExists()) {
          createNew = true;
        }
        persistence = psb.makeOrGet().build();
        break;
    }

    this.persist = persistence;

    final Double smoothing = this.conf.getDocumentModelSmoothingParameter();
    final Double lambda = this.conf.getDocumentModelParamLambda();
    final Double beta = this.conf.getDocumentModelParamBeta();

    if (!createNew) {
      if (this.dataProv.getLastIndexCommitGeneration() == null || !persistence
          .getMetaData().hasGenerationValue()) {
        LOG.warn("Index commit generation not available. Assuming an " +
            "unchanged index!");
      } else {
        if (!persistence.getMetaData().isGenerationCurrent(
            this.dataProv.getLastIndexCommitGeneration())) {
          rebuild = true;
          LOG.warn("Index changed since last caching.");
        }
      }
      if (!this.persist.getDb().getAtomicString(Caches.SMOOTHING.name()).get()
          .equals(smoothing.toString())) {
        rebuild = true;
        LOG.warn("Different smoothing parameter value used in cache.");
      }
      if (!this.persist.getDb().getAtomicString(Caches.LAMBDA.name()).get()
          .equals(lambda.toString())) {
        rebuild = true;
        LOG.warn("Different lambda parameter value used in cache.");
      }
      if (!this.persist.getDb().getAtomicString(Caches.BETA.name()).get()
          .equals(beta.toString())) {
        rebuild = true;
        LOG.warn("Different beta parameter value used in cache.");
      }
      if (!persistence.getMetaData()
          .areFieldsCurrent(this.dataProv.getDocumentFields())) {
        rebuild = true;
        LOG.warn("Current fields are different from cached ones.");
      }
      if (!persistence.getMetaData()
          .areStopwordsCurrent(this.dataProv.getStopwords())) {
        rebuild = true;
        LOG.warn("Current stopwords are different from cached ones.");
      }
    }

    if (rebuild) {
      LOG.warn("Cache flagged as being invalid due to changes in parameters. " +
          "Clearing cache.");
      this.persist.getDb().delete(Caches.LAMBDA.name());
      this.persist.getDb().delete(Caches.BETA.name());
      this.extDocMan =
          new ExternalDocTermDataManager(this.persist.getDb(), IDENTIFIER);
      this.extDocMan.clear();
      createNew = true;
    }

    if (createNew) {
      this.persist.getDb().
          createAtomicString(Caches.SMOOTHING.name(), smoothing.toString());
      this.persist.getDb().createAtomicString(Caches.LAMBDA.name(),
          lambda.toString());
      this.persist.getDb().createAtomicString(Caches.BETA.name(),
          beta.toString());
      persistence.updateMetaData(this.dataProv.getDocumentFields(),
          this.dataProv.getStopwords());
    }

    if (!rebuild) {
      this.extDocMan =
          new ExternalDocTermDataManager(this.persist.getDb(), IDENTIFIER);
    }
    this.hasCache = true;
  }

  /**
   * Parse the configuration and do some simple pre-checks.
   */
  private void parseConfig() {
    if (this.conf.getMinFeedbackDocumentsCount() >
        this.metrics.collection().numberOfDocuments()) {
      throw new IllegalStateException(
          "Required minimum number of feedback documents ("
              + this.conf.getMinFeedbackDocumentsCount() + ") is larger "
              + "or equal compared to the total amount of indexed documents "
              + "(" + this.metrics.collection().numberOfDocuments()
              + "). Unable to provide feedback."
      );
    }
    this.conf.debugDump();
  }

  /**
   * Close this instance and release any resources (mainly the database
   * backend).
   */
  @Override
  public void close() {
    if (!this.isClosed) {
      this.persist.closeDb();
      this.isClosed = true;
    }
  }

  /**
   * Calculate the query model.
   *
   * @param fbTerm Feedback term
   * @param qTerms List of query terms
   * @param fbDocIds List of feedback document
   * @return Query model for the current term and set of feedback documents
   */
  double getQueryModel(final ByteArray fbTerm,
      final Collection<ByteArray> qTerms,
      final Set<Integer> fbDocIds) {
    checkClosed();
    assert fbTerm != null;
    assert qTerms != null && !qTerms.isEmpty();
    assert fbDocIds != null && !fbDocIds.isEmpty();

    Objects.requireNonNull(fbTerm);
    if (Objects.requireNonNull(fbDocIds).isEmpty()) {
      throw new IllegalArgumentException(
          "List of feedback document ids was empty.");
    }
    if (Objects.requireNonNull(qTerms).isEmpty()) {
      throw new IllegalArgumentException("List of query terms was empty.");
    }
    double model = 0d;

    for (final Integer fbDocId : fbDocIds) {
      // document model for the given term pD(t)
      final double docModel = getDocumentModel(fbDocId, fbTerm);
      // calculate the product of the document models for all query terms
      // given the current document
      double docModelQtProduct = 1d;
      for (final ByteArray qTerm : qTerms) {
        docModelQtProduct *= getDocumentModel(fbDocId, qTerm);
        if (docModelQtProduct <= 0d) {
          // short circuit, if model is already too low
          break;
        }
      }
      model += docModel * docModelQtProduct;
    }
    return model;
  }

  /**
   * Checks, if this instance has been closed. Throws a runtime exception, if
   * the instance has already been closed.
   */
  private void checkClosed() {
    if (this.isClosed) {
      throw new IllegalStateException("Instance has been closed.");
    }
  }

  /**
   * Safe method to get the document model value for a document term. This
   * method will calculate an additional value, if the term in question is not
   * contained in the document. <br> A call to this method should only needed,
   * if {@code lambda} in {@link #conf} is lesser than {@code 1}.
   *
   * @param docId Id of the document whose model to get
   * @param term Term to calculate the model for
   * @return Model value for document & term
   */
  double getDocumentModel(final int docId, final ByteArray term) {
    assert term != null;
    checkClosed();
    Double model = getDocumentModel(docId).get(term);
    // if term not in document, calculate new value
    if (model == null) {
      // double checked locking
      synchronized (this.docModelCalcSync) {
        model = getDocumentModel(docId).get(term);
        if (model == null) {
          final DocumentModel docModel = this.metrics.getDocumentModel(docId);
          final double termRelIdxFreq = this.metrics.collection().relTf(term);
          // relative collection frequency of the term
          final double docTermFreq = docModel.metrics().tf().doubleValue();
          final double termsInDoc =
              docModel.metrics().uniqueTermCount().doubleValue();

          final double smoothedTerm =
              (this.conf.getDocumentModelSmoothingParameter() *
                  termRelIdxFreq) /
                  (docTermFreq +
                      (this.conf.getDocumentModelSmoothingParameter() *
                          termsInDoc));
          model =
              (this.conf.getDocumentModelParamLambda() *
                  ((this.conf.getDocumentModelParamBeta() * smoothedTerm) +
                      ((1d - this.conf.getDocumentModelParamBeta()) *
                          termRelIdxFreq))
              ) + ((1d - this.conf.getDocumentModelParamLambda()) *
                  termRelIdxFreq);
        }
      }
    }
    assert model > 0d;
    return model;
  }

  /**
   * Calculate or get the document model (pd) for a specific document.
   *
   * @param docId Id of the document whose model to get
   * @return Mapping of each term in the document to it's model value
   */
  Map<ByteArray, Double> getDocumentModel(final int docId) {
    checkClosed();
    Map<ByteArray, Double> map;
    if (this.docModelDataCache.containsKey(docId)) {
      // get local cached map
      map = this.docModelDataCache.get(docId);
    } else {
      // double checked locking
      synchronized (this.docModelMapCalcSync) {
        if (this.docModelDataCache.containsKey(docId)) {
          // get local cached map
          map = this.docModelDataCache.get(docId);
        } else {
          // get external cached map
          map = this.extDocMan.getData(docId, DataKeys.DM.name());

          // build mapping, if needed
          if (map == null || map.isEmpty()) {
            final DocumentModel docModel = this.metrics.getDocumentModel(docId);
            final double docTermFreq = docModel.metrics().tf().doubleValue();
            final double termsInDoc =
                docModel.metrics().uniqueTermCount().doubleValue();
            final Set<ByteArray> terms = docModel.getTermFreqMap().keySet();

            map = new HashMap<>(terms.size());
            for (final ByteArray term : terms) {
              // relative collection frequency of the term
              final double termRelIdxFreq =
                  this.metrics.collection().relTf(term);
              // term frequency given the document
              final double termInDocFreq =
                  docModel.metrics().tf(term).doubleValue();

              final double smoothedTerm =
                  (termInDocFreq +
                      (this.conf.getDocumentModelSmoothingParameter() *
                          termRelIdxFreq)) /
                      (docTermFreq +
                          (this.conf.getDocumentModelSmoothingParameter() *
                              termsInDoc));
              final double value =
                  (this.conf.getDocumentModelParamLambda() *
                      ((this.conf.getDocumentModelParamBeta() * smoothedTerm) +
                          ((1d - this.conf.getDocumentModelParamBeta()) *
                              termRelIdxFreq))
                  ) + ((1d - this.conf.getDocumentModelParamLambda()) *
                      termRelIdxFreq);
              map.put(term, value);
            }
            // push data to persistent storage
            this.extDocMan.setData(docModel.id, DataKeys.DM.name(), map);
          }
          // push to local cache
          this.docModelDataCache.put(docId, map);
        }
      }
    }
    return map;
  }

  /**
   * Calculates the improved clarity score for a given query.
   *
   * @param query Query to calculate for
   * @return Clarity score result object
   */
  @Override
  public Result calculateClarity(final String query)
      throws ClarityScoreCalculationException, IOException {
    checkClosed();
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // result object
    final Result result = new Result(this.getClass());
    // final clarity score
    final AtomicDouble score = new AtomicDouble(0d);
    // stopped query terms
    final List<String> queryTerms = QueryUtils.tokenizeQueryString(query,
        this.analyzer);
    final List<ByteArray> queryTermsBa = new ArrayList<>(queryTerms.size());
    for (final String qTerm : queryTerms) {
      @SuppressWarnings("ObjectAllocationInLoop")
      final ByteArray qTermBa = new ByteArray(qTerm.getBytes("UTF-8"));
      if (this.metrics.collection().tf(qTermBa) == 0) {
        LOG.info("Removing query term '{}'. Not in collection.", qTerm);
      } else {
        queryTermsBa.add(qTermBa);
      }
    }

    // save base data to result object
    result.setConf(this.conf);
    result.setQueryTerms(queryTermsBa);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final TryExactTermsQuery relaxableQuery;
    try {
      relaxableQuery = new TryExactTermsQuery(
          this.analyzer, query, this.dataProv.getDocumentFields());
    } catch (final ParseException e) {
      throw new ClarityScoreCalculationException(
          "Caught exception while building query.", e);
    }

    // collection of feedback document ids
    final Set<Integer> feedbackDocIds;

    try {
      feedbackDocIds = Feedback.getFixed(this.idxReader, relaxableQuery,
          this.conf.getMaxFeedbackDocumentsCount());
    } catch (final IOException | ParseException e) {
      throw new ClarityScoreCalculationException("Error while retrieving " +
          "feedback documents.", e);
    }
    if (feedbackDocIds.isEmpty()) {
      result.setEmpty("No feedback documents.");
      return result;
    }
    result.setFeedbackDocIds(feedbackDocIds);

    // collect all unique terms from feedback documents
    final Set<ByteArray> fbTerms = this.dataProv.getDocumentsTermSet(
        feedbackDocIds);

    // get document frequency threshold
    int minDf = (int) (this.metrics.collection().numberOfDocuments()
        * this.conf.getMinFeedbackTermSelectionThreshold());
    if (minDf <= 0) {
      LOG.debug("Document frequency threshold was {} setting to 1", minDf);
      minDf = 1;
    }
    int maxDf = (int) (this.metrics.collection().numberOfDocuments()
        * this.conf.getMaxFeedbackTermSelectionThreshold());
    LOG.debug("Document frequency threshold: low=({} = {}) hi=({} = {})",
        minDf, this.conf.getMinFeedbackTermSelectionThreshold(),
        maxDf, this.conf.getMaxFeedbackTermSelectionThreshold()
    );
    LOG.debug("Initial term set size {}", fbTerms.size());

    // keep results of concurrent term eliminations
    final ConcurrentLinkedQueue<ByteArray> reducedFbTerms
        = new ConcurrentLinkedQueue<>();

    // remove all terms whose threshold is too low
    try {
      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(fbTerms),
              new FbTermReducerTarget(this.metrics.collection(), minDf, maxDf,
                  reducedFbTerms)
          )
      ).process(fbTerms.size());
    } catch (final ProcessingException e) {
      throw new ClarityScoreCalculationException("Failed to reduce terms by " +
          "threshold.", e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Reduced term set size {}.", reducedFbTerms.size());

      final Collection<String> sTerms = new ArrayList<>(reducedFbTerms.size());
      for (final ByteArray bTerm : reducedFbTerms) {
        sTerms.add(ByteArrayUtils.utf8ToString(bTerm));
      }
      LOG.debug("Feedback terms {}", sTerms);
    }

    if (reducedFbTerms.isEmpty()) {
      result.setEmpty("No terms remaining after elimination based " +
          "on document frequency threshold (" + minDf + ").");
      return result;
    }

    result.setFeedbackTerms(new HashSet<>(reducedFbTerms));

    // do the final calculation for all remaining feedback terms
    if (LOG.isTraceEnabled()) {
      LOG.trace("Using {} feedback documents for calculation. {}",
          feedbackDocIds.size(), feedbackDocIds);
    } else {
      LOG.debug("Using {} feedback documents for calculation.",
          feedbackDocIds.size());
    }
    try {
      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(reducedFbTerms),
              new ModelCalculatorTarget(this.metrics.collection(),
                  feedbackDocIds,
                  queryTermsBa,
                  score)
          )
      ).process(reducedFbTerms.size());
    } catch (final ProcessingException e) {
      throw new ClarityScoreCalculationException(e);
    }

    result.setScore(score.get());

    timeMeasure.stop();
    LOG.debug("Calculating improved clarity score for query {} "
            + "with {} document models and {} terms took {}. {}", query,
        feedbackDocIds.size(), fbTerms.size(), timeMeasure.
            getTimeString(), score
    );

    return result;
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   *
   * @throws ProcessingException Thrown, if threaded calculation encountered an
   * error
   */
  public void preCalcDocumentModels()
      throws ProcessingException {
    if (!this.hasCache) {
      LOG.warn("Won't pre-calculate any values. Cache not set.");
    }
    final Atomic.Boolean hasData = this.persist.getDb().getAtomicBoolean(
        Caches.HAS_PRECALC_DATA.name());
    if (hasData.get()) {
      LOG.info("Pre-calculated models are current.");
    } else {
      LOG.info("Pre-calculating models.");
      new Processing(
          new TargetFuncCall<>(
              this.dataProv.getDocumentIdSource(),
              new DocumentModelCalculatorTarget()
          )
      ).process(this.metrics.collection().numberOfDocuments().intValue());
      hasData.set(true);
    }
  }

  /**
   * Ids of temporary data caches held in the database.
   */
  private enum Caches {

    /**
     * Smoothing parameter value.
     */
    SMOOTHING,
    /**
     * Lambda parameter.
     */
    LAMBDA,
    /**
     * Beta parameter.
     */
    BETA,
    /**
     * Flag indicating, if pre-calculated models are available.
     */
    HAS_PRECALC_DATA,
    /**
     * Default document models.
     */
    DEFAULT_DOC_MODELS
  }

  @SuppressWarnings("JavaDoc")
  private enum DataKeys {

    /**
     * Document-models.
     */
    DM
  }

  /**
   * {@link Processing} {@link Target} to reduce feedback terms.
   */
  @SuppressWarnings("ProtectedInnerClass")
  protected static final class FbTermReducerTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    /**
     * Target to store terms passing through the reducing process.
     */
    private final Queue<ByteArray> reducedTermsTarget;
    /**
     * Minimum document frequency for a term to pass.
     */
    private final int minDf;
    /**
     * Maximum document frequency for a term to pass.
     */
    private final int maxDf;
    /**
     * Access collection metrics.
     */
    private final Metrics.CollectionMetrics collectionMetrics;

    /**
     * Creates a new {@link Processing} {@link Target} for reducing query
     * terms.
     *
     * @param newMetrics Metrics instance
     * @param minDocFreq Minimum document frequency
     * @param maxDocFreq Maximum document frequency
     * @param reducedFbTerms Target for reduced terms
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    FbTermReducerTarget(
        final Metrics.CollectionMetrics newMetrics,
        final int minDocFreq, final int maxDocFreq,
        final Queue<ByteArray> reducedFbTerms) {
      assert newMetrics != null;
      assert reducedFbTerms != null;

      this.reducedTermsTarget = reducedFbTerms;
      this.minDf = minDocFreq;
      this.maxDf = maxDocFreq;
      this.collectionMetrics = newMetrics;
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        final Integer df = this.collectionMetrics.df(term);
        if (df >= this.minDf && df <= this.maxDf) {
          this.reducedTermsTarget.add(term);
        }
      }
    }
  }

  /**
   * Extended result object containing additional meta information about what
   * values were actually used for calculation.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Result
      extends ClarityScoreResult {
    /**
     * Configuration prefix.
     */
    private static final String CONF_PREFIX = DefaultClarityScore.IDENTIFIER
        + "-result";
    /**
     * Configuration that was used.
     */
    private ImprovedClarityScoreConfiguration conf;
    /**
     * Ids of feedback documents used for calculation.
     */
    private Set<Integer> feedbackDocIds;
    /**
     * Terms from feedback documents used for calculation.
     */
    private Set<ByteArray> feedbackTerms;

    /**
     * Creates an object wrapping the result with meta information.
     *
     * @param cscType Type of the calculation class
     */
    public Result(final Class<? extends ClarityScoreCalculation> cscType) {
      super(cscType);
      this.feedbackDocIds = Collections.emptySet();
      this.feedbackTerms = Collections.emptySet();
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
     * Get the collection of feedback documents used for calculation.
     *
     * @return Feedback documents used for calculation
     */
    public Set<Integer> getFeedbackDocuments() {
      return Collections.unmodifiableSet(this.feedbackDocIds);
    }

    /**
     * Get the collection of feedback terms used for calculation.
     *
     * @return Feedback terms used for calculation
     */
    public Set<ByteArray> getFeedbackTerms() {
      return Collections.unmodifiableSet(this.feedbackTerms);
    }

    /**
     * Set the list of feedback terms used.
     *
     * @param fbTerms List of feedback terms
     */
    void setFeedbackTerms(final Set<ByteArray> fbTerms) {
      this.feedbackTerms = Collections.unmodifiableSet(Objects
          .requireNonNull(fbTerms, "Feedback terms were null"));
    }

    @Override
    public ScoringResultXml getXml() {
      final ScoringResultXml xml = new ScoringResultXml();

      getXml(xml);

      // feedback terms
      if (GlobalConfiguration.conf()
          .getAndAddBoolean(CONF_PREFIX + "ListFeedbackTerms",
              Boolean.TRUE)) {
        final List<String> fbTerms = new ArrayList<>
            (this.feedbackTerms.size());
        for (final ByteArray fbTerm : this.feedbackTerms) {
          fbTerms.add(ByteArrayUtils.utf8ToString(fbTerm));
        }
        xml.getItems().put("feedbackTerms", StringUtils.join(fbTerms, " "));
      }

      // feedback documents
      if (GlobalConfiguration.conf()
          .getAndAddBoolean(CONF_PREFIX + "ListFeedbackDocuments",
              Boolean.TRUE)) {
        final List<Tuple.Tuple2<String, String>> fbDocsList = new ArrayList<>
            (this.feedbackDocIds.size());
        for (final Integer docId : this.feedbackDocIds) {
          fbDocsList.add(Tuple.tuple2("id", docId.toString()));
        }
        xml.getLists().put("feedbackDocuments", fbDocsList);
      }

      // configuration
      if (this.conf != null) {
        xml.getLists().put("configuration", this.conf.entryList());
      }

      return xml;
    }
  }

  /**
   * Builder to create a new {@link ImprovedClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractClarityScoreCalculationBuilder<Builder> {
    /**
     * Configuration to use.
     */
    @SuppressWarnings("PackageVisibleField")
    ImprovedClarityScoreConfiguration configuration = new
        ImprovedClarityScoreConfiguration();

    /**
     * Initializes the builder.
     */
    public Builder() {
      super(IDENTIFIER);
    }

    /**
     * Set the configuration to use.
     *
     * @param newConf Configuration
     * @return Self reference
     */
    public Builder configuration(
        final ImprovedClarityScoreConfiguration newConf) {
      this.configuration = Objects.requireNonNull(newConf,
          "Configuration was null.");
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
      super.validate();
      validatePersistenceBuilder();
    }

    @Override
    protected Builder getThis() {
      return this;
    }
  }

  /**
   * {@link Processing} {@link Target} to calculate document models.
   */
  private final class ModelCalculatorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    /**
     * Collection related metrics instance.
     */
    private final Metrics.CollectionMetrics collectionMetrics;

    /**
     * Query terms. Must be non-unique!
     */
    private final Collection<ByteArray> queryTerms;

    /**
     * Ids of feedback documents to use.
     */
    private final Set<Integer> feedbackDocIds;

    /**
     * Final score to add calculation results to.
     */
    private final AtomicDouble score;

    /**
     * Create a new calculator for document models.
     *
     * @param cMetrics Collection metrics instance
     * @param fbDocIds Feedback document ids
     * @param qTerms Query terms
     * @param result Result to add to
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    ModelCalculatorTarget(final Metrics.CollectionMetrics cMetrics,
        final Set<Integer> fbDocIds,
        final Collection<ByteArray> qTerms, final AtomicDouble result) {
      assert fbDocIds != null : "List of feedback document ids was null.";
      assert qTerms != null : "List of query terms was null.";
      assert result != null : "Calculation result target was null.";

      this.collectionMetrics = cMetrics;
      this.queryTerms = qTerms;
      this.feedbackDocIds = fbDocIds;
      this.score = result;
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        final double queryModel = getQueryModel(term, this.queryTerms,
            this.feedbackDocIds);
        if (queryModel <= 0d) {
          LOG.warn("Query model <= 0 ({}) for term '{}'.", queryModel,
              ByteArrayUtils.utf8ToString(term));
        } else {
          this.score.addAndGet(queryModel * MathUtils.log2(queryModel
              / this.collectionMetrics.relTf(term)));
        }
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for document model creation.
   */
  private final class DocumentModelCalculatorTarget
      extends TargetFuncCall.TargetFunc<Integer> {
    /**
     * Constructor to allow parent class access.
     */
    DocumentModelCalculatorTarget() {
    }

    @Override
    public void call(final Integer docId) {
      if (docId != null) {
        // call the calculation method of the main class for each
        // document and term that is available for processing
        getDocumentModel(docId);
      }
    }
  }
}
