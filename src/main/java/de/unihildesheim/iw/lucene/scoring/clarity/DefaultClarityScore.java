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
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.AtomicDouble;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.mapdb.Atomic;
import org.mapdb.Serializer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    implements ClarityScoreCalculation, Closable {

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
   * Provider for general index metrics.
   */
  private final Metrics metrics;
  /**
   * Configuration object used for all parameters of the calculation.
   */
  private final DefaultClarityScoreConfiguration conf;
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
   * Synchronization lock for default document model calculation
   */
  private final Object defaultDocModelCalcSync = new Object();
  /**
   * Synchronization lock for document model map calculation.
   */
  private final Object docModelMapCalcSync = new Object();
  /**
   * Flag indicating, if this instance has been closed.
   */
  private volatile boolean isClosed;
  /**
   * Cached storage of Document-id -> Term, model-value.
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private Map<Integer, Map<ByteArray, Double>> docModelDataCache;
  /**
   * Cached mapping of document model values.
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private Map<ByteArray, Double> defaultDocModels;
  /**
   * Database to use.
   */
  private Persistence persist;
  /**
   * Manager object for extended document data.
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private ExternalDocTermDataManager extDocMan;

  /**
   * Create a new instance using a builder.
   *
   * @param builder Builder to use for constructing the instance
   * @throws Buildable.BuildableException Thrown, if building the persistent
   * cache failed
   */
  DefaultClarityScore(final Builder builder)
      throws Buildable.BuildableException {
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    this.conf = builder.getConfiguration();
    this.dataProv = builder.idxDataProvider;
    this.idxReader = builder.idxReader;
    this.metrics = new Metrics(builder.idxDataProvider);
    this.docModelDataCache =
        new ConcurrentHashMap<>(this.conf.getFeedbackDocCount());
    this.analyzer = builder.analyzer;

    this.conf.debugDump();

    // initialize
    this.initCache(builder);
  }

  /**
   * Initializes a cache.
   *
   * @param builder Builder instance
   * @throws Buildable.BuildableException Thrown, if building the persistent
   * cache failed
   */
  private void initCache(final Builder builder)
      throws Buildable.BuildableException {
    final Persistence.Builder psb = builder.persistenceBuilder;

    final Persistence persistence;
    boolean createNew = false;
    boolean clearCache = false;
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

    if (!createNew) {
      if (null == this.dataProv.getLastIndexCommitGeneration() || !persistence
          .getMetaData().hasGenerationValue()) {
        LOG.warn("Index commit generation not available. Assuming an " +
            "unchanged index!");
      } else {
        if (!persistence.getMetaData()
            .isGenerationCurrent(
                this.dataProv.getLastIndexCommitGeneration())) {
          LOG.warn("Index changed since last caching. Clearing cache.");
          clearCache = true;
        }
      }
      if (!clearCache) {
        if (!this.persist.getDb()
            .getAtomicString(Caches.LANGMODEL_WEIGHT.name()).get().
                equals(this.conf.getLangModelWeight().toString())) {
          LOG.warn("Different language-model weight used in cache. " +
              "Clearing cache.");
          clearCache = true;
        } else if (!persistence.getMetaData()
            .areFieldsCurrent(this.dataProv.getDocumentFields())) {
          LOG.warn("Current fields are different from cached ones. " +
              "Clearing cache.");
          clearCache = true;
        } else if (!persistence.getMetaData().areStopwordsCurrent(this.dataProv
            .getStopwords())) {
          LOG.warn("Current stopwords are different from cached ones. " +
              "Clearing cache.");
          clearCache = true;
        }
      }
    }

    if (clearCache && this.persist.getDb().exists(Caches.DEFAULT_DOC_MODELS
        .name())) {
      LOG.info("Clearing cached document models.");
      this.persist.getDb().delete(Caches.DEFAULT_DOC_MODELS.name());
    }

    if (createNew || clearCache) {
      if (this.persist.getDb().exists(Caches.LANGMODEL_WEIGHT.name())) {
        this.persist.getDb().getAtomicString(Caches.LANGMODEL_WEIGHT.name())
            .set(this.conf.
                getLangModelWeight().toString());
      } else {
        this.persist.getDb()
            .createAtomicString(Caches.LANGMODEL_WEIGHT.name(), this.conf.
                getLangModelWeight().toString());
      }
      persistence.clearMetaData();
      persistence.updateMetaData(this.dataProv.getDocumentFields(),
          this.dataProv.getStopwords());
    }

    this.defaultDocModels = this.persist.getDb()
        .createHashMap(Caches.DEFAULT_DOC_MODELS.name())
        .keySerializer(ByteArray.SERIALIZER)
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();

    this.extDocMan =
        new ExternalDocTermDataManager(this.persist.getDb(), IDENTIFIER);
    if (clearCache) {
      LOG.info("Clearing document model data cache.");
      this.extDocMan.clear();
    }
  }

  /**
   * Calculate or get the query model for a given term. Calculation is based on
   * a set of feedback documents and a list of query terms.
   *
   * @param term Term to calculate the model for
   * @param fbDocIds Feedback document ids
   * @param qTerms Query terms
   * @return Model value
   */
  double getQueryModel(final ByteArray term, final Collection<Integer> fbDocIds,
      final Set<ByteArray> qTerms) {
    assert null != term;
    assert null != fbDocIds && !fbDocIds.isEmpty();
    assert null != qTerms && !qTerms.isEmpty();
    checkClosed();

    double model = 0d;
    // throw all terms together
    final Collection<ByteArray> terms = new ArrayList<>(qTerms);
    terms.add(term);
    // document model map of a current document
    Map<ByteArray, Double> docModMap;

    // go through all documents
    for (final Integer docId : fbDocIds) {
      docModMap = getDocumentModel(docId);
      assert null != docModMap;
      double modelPart = 1d;
      for (final ByteArray aTerm : terms) {
        if (docModMap.containsKey(aTerm)) {
          assert 0d != docModMap.get(aTerm);
          modelPart *= docModMap.get(aTerm);
        } else {
          assert 0d != getDefaultDocumentModel(aTerm);
          modelPart *= getDefaultDocumentModel(aTerm);
        }
      }
      model += modelPart;
    }
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
          map = this.extDocMan.getData(docId, DataKeys.DOCMODEL.name());
          // build mapping, if needed
          if (null == map || map.isEmpty()) {
            final DocumentModel docModel = this.metrics.getDocumentModel(docId);
            final Set<ByteArray> terms = docModel.getTermFreqMap().keySet();

            map = new HashMap<>(terms.size());
            for (final ByteArray term : terms) {
              final Double model = (this.conf.getLangModelWeight()
                  * docModel.metrics().relTf(term))
                  + getDefaultDocumentModel(term);

              assert 0d != model;
              map.put(term, model);
            }
            // push data to persistent storage
            this.extDocMan.setData(docModel.id, DataKeys.DOCMODEL.name(), map);
          }
          // push to local cache
          this.docModelDataCache.put(docId, map);
        }
      }
    }
    return map;
  }

  /**
   * Calculate or get the default value, if the term is not contained in
   * document.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   */
  double getDefaultDocumentModel(final ByteArray term) {
    assert null != this.defaultDocModels; // must be initialized
    assert null != term;
    checkClosed();

    Double model = this.defaultDocModels.get(term);
    if (model == null) {
      // double checked locking
      synchronized (this.defaultDocModelCalcSync) {
        model = this.defaultDocModels.get(term);
        if (model == null) {
          model = ((1d - this.conf.getLangModelWeight()) *
              this.metrics.collection().relTf(term));
          this.defaultDocModels.put(new ByteArray(term), model);
        }
      }
    }
    return model;
  }

  /**
   * Pre-calculate all document models for all terms known from the index. This
   * simply runs the calculation for each term and stores it's value to a
   * persistent cache.
   *
   * @throws ProcessingException Thrown, if threaded calculation encountered an
   * error
   */
  public void preCalcDocumentModels()
      throws ProcessingException {
    checkClosed();
    final Atomic.Boolean hasData = this.persist.getDb().getAtomicBoolean(
        Caches.HAS_PRECALC_DATA.name());
    if (hasData.get()) {
      LOG.info("Pre-calculated models are current.");
    } else {
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
   * Checks, if this instance has been closed. Throws a runtime exception, if
   * the instance has already been closed.
   */
  private void checkClosed() {
    if (this.isClosed) {
      throw new IllegalStateException("Instance has been closed.");
    }
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
   * Calculate the clarity score. This method does only pre-checks. The real
   * calculation is done in {@link #calculateClarity(Set, Set)}.
   *
   * @param query Query used for term extraction
   * @return Clarity score result
   * @throws ClarityScoreCalculationException
   */
  @Override
  public Result calculateClarity(final String query)
      throws ClarityScoreCalculationException {
    checkClosed();
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final TryExactTermsQuery queryObj;
    try {
      queryObj = new TryExactTermsQuery(this.analyzer, query,
          this.dataProv.getDocumentFields());
    } catch (final ParseException e) {
      throw new ClarityScoreCalculationException(
          "Caught exception while building query.", e);
    }

    // get feedback documents
    final Set<Integer> feedbackDocuments;
    try {
      feedbackDocuments =
          Feedback.getFixed(this.idxReader, queryObj,
              this.conf.getFeedbackDocCount());
    } catch (final IOException | ParseException ex) {
      throw new ClarityScoreCalculationException(
          "Caught exception while preparing calculation.", ex);
    }

    if (feedbackDocuments.isEmpty()) {
      final Result result = new Result();
      result.setEmpty("No feedback documents.");
      return result;
    }

    final Set<ByteArray> queryTerms;
    try {
      // Get unique query terms
      queryTerms = QueryUtils.getUniqueQueryTerms(queryObj);
    } catch (final UnsupportedEncodingException e) {
      throw new ClarityScoreCalculationException(
          "Caught exception parsing query.", e);
    }

    if (null == queryTerms || queryTerms.isEmpty()) {
      final Result result = new Result();
      result.setEmpty("No query terms.");
      return result;
    }

    timeMeasure.stop();
    try {
      final Result r = calculateClarity(feedbackDocuments, queryTerms);

      LOG.debug("Calculating default clarity score for query '{}' "
              + "with {} document models took {}. {}", query,
          feedbackDocuments.size(), timeMeasure.getTimeString(), r.getScore()
      );
      return r;
    } catch (final ProcessingException e) {
      throw new ClarityScoreCalculationException(e);
    }
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  /**
   * Calculate the clarity score. Calculation is based on a set of feedback
   * documents and a list of query terms.
   *
   * @param feedbackDocuments Document-ids of feedback documents
   * @param queryTerms Query terms
   * @return Result of the calculation
   * @throws ProcessingException Thrown if any of the threaded calculations
   * encountered an error
   */
  Result calculateClarity(final Set<Integer> feedbackDocuments,
      final Set<ByteArray> queryTerms)
      throws ProcessingException {
    checkClosed();
    final Result result = new Result();
    result.setConf(this.conf);
    result.setFeedbackDocIds(feedbackDocuments);

    this.docModelDataCache = new ConcurrentHashMap<>(feedbackDocuments.size());

    // remove terms not in index, their value is zero
    final Iterator<ByteArray> queryTermsIt = queryTerms.iterator();
    ByteArray queryTerm;
    while (queryTermsIt.hasNext()) {
      queryTerm = queryTermsIt.next();
      if (0L == this.metrics.collection().tf(queryTerm)) {
        queryTermsIt.remove();
      }
    }

    // Short circuit, if no terms are left. The score will be zero.
    if (queryTerms.isEmpty()) {
      result.setEmpty("No query term matched in index. Result is 0.");
      return result;
    }

    // non existent (in collection) terms are now removed
    result.setQueryTerms(queryTerms);

    // calculate multi-threaded
    final Processing p = new Processing();

    // calculate the query model for each term in the query
    final ConcurrentMap<ByteArray, Double> queryModelMap =
        new ConcurrentHashMap<>(feedbackDocuments.size());
    p.setSourceAndTarget(
        new TargetFuncCall<>(
            new CollectionSource<>(queryTerms),
            new QueryModelCalculatorTarget(queryTerms, feedbackDocuments,
                queryModelMap)
        )
    ).process(queryTerms.size());

    // now calculate the score using all now calculated values
    final AtomicDouble score = new AtomicDouble(0d);
    p.setSourceAndTarget(
        new TargetFuncCall<>(
            new CollectionSource<>(queryTerms),
            new ScoreCalculatorTarget(queryModelMap, score)
        )
    ).process(queryTerms.size());

    result.setScore(score.doubleValue());
    return result;
  }

  /**
   * Get the provider for general index metrics.
   *
   * @return Metrics instance
   */
  Metrics getMetrics() {
    checkClosed();
    return this.metrics;
  }

  /**
   * Ids of temporary data caches held in the database.
   */
  private enum Caches {

    /**
     * Language model weighting factor.
     */
    LANGMODEL_WEIGHT,
    /**
     * Flag indicating, if pre-calculated models are available.
     */
    HAS_PRECALC_DATA,
    /**
     * Pre-calculated default document models.
     */
    DEFAULT_DOC_MODELS
  }

  /**
   * Keys to store calculation results in document models and access properties
   * stored in the {@link IndexDataProvider}.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum DataKeys {

    /**
     * Stores the document model for a specific term in a {@link
     * DocumentModel}.
     */
    DOCMODEL
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
     * Configuration prefix.
     */
    private static final String CONF_PREFIX =
        DefaultClarityScore.IDENTIFIER + "-result";
    /**
     * Number of documents in the index matching the query.
     */
    private Integer numberOfMatchingDocuments = null;
    /**
     * Configuration that was used.
     */
    private DefaultClarityScoreConfiguration conf = null;

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
     * Set the number of documents in the index matching the query.
     *
     * @param num Number of documents
     */
    void setNumberOfMatchingDocuments(final int num) {
      this.numberOfMatchingDocuments = num;
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

      // configuration
      if (this.conf != null) {
        xml.getLists().put("configuration", this.conf.entryList());
      }

      // number of feedback documents
      xml.getItems().put("feedbackDocuments",
          Integer.toString(this.feedbackDocIds.size()));

      // number of matching documents
      if (this.numberOfMatchingDocuments != null) {
        xml.getItems().put("matchingDocuments",
            Integer.toString(this.numberOfMatchingDocuments));
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

      return xml;
    }


  }

  /**
   * Builder to create a new {@link DefaultClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractClarityScoreCalculationBuilder<Builder> {
    /**
     * Configuration to use.
     */
    private DefaultClarityScoreConfiguration configuration = new
        DefaultClarityScoreConfiguration();

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
        final DefaultClarityScoreConfiguration newConf) {
      this.configuration = Objects.requireNonNull(newConf,
          "Configuration was null.");
      return this;
    }

    /**
     * Get the configuration to use.
     *
     * @return Configuration
     */
    DefaultClarityScoreConfiguration getConfiguration() {
      return this.configuration;
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
      super.validate();
      validatePersistenceBuilder();
    }

    @Override
    protected Builder getThis() {
      return this;
    }
  }

  /**
   * {@link Processing} {@link Target} calculating a portion of the final
   * clarity score. The current term is passed in from a {@link Source}.
   */
  private final class ScoreCalculatorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    /**
     * Map containing pre-calculated query model values. (Term -> Model)
     */
    private final ConcurrentMap<ByteArray, Double> qModMap;

    /**
     * Shared instance to store calculation results.
     */
    private final AtomicDouble target;

    /**
     * Initialize the calculation target.
     *
     * @param queryModelMap Map with pre-calculated query model values
     * @param score Atomic value to add up the score
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    ScoreCalculatorTarget(
        final ConcurrentMap<ByteArray, Double> queryModelMap,
        final AtomicDouble score) {
      this.qModMap = queryModelMap;
      this.target = score;
    }

    /**
     * Calculate the score portion for a given term using already calculated
     * query models.
     *
     * @param term Current term
     */
    @Override
    public void call(final ByteArray term) {
      if (null != term) {
        final Double pq = this.qModMap.get(term);
        if (0d == pq) {
          // query model may be zero.
          return;
        }

        assert null != pq;
        this.target.addAndGet(pq * MathUtils.log2(pq /
            DefaultClarityScore.this.getMetrics().collection().relTf(term)));
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for query model calculation based on
   * query terms and feedback documents. The current term is passed in from a
   * {@link Source}.
   */
  private final class QueryModelCalculatorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    /**
     * List of query terms used.
     */
    private final Set<ByteArray> queryTerms;

    /**
     * Feedback documents to use.
     */
    private final Set<Integer> fbDocIds;

    /**
     * Map to gather calculation results. (term -> model)
     */
    private final Map<ByteArray, Double> target;

    /**
     * Create a new target calculating query models for a list of query terms
     * and feedback documents.
     *
     * @param qTerms List of query terms
     * @param feedbackDocuments List of feedback document ids
     * @param queryModelMap Target map to store results
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    QueryModelCalculatorTarget(
        final Set<ByteArray> qTerms,
        final Set<Integer> feedbackDocuments,
        final Map<ByteArray, Double> queryModelMap) {
      this.queryTerms = qTerms;
      this.target = queryModelMap;
      this.fbDocIds = feedbackDocuments;
    }

    /**
     * Calculate the query model for a single term.
     *
     * @param term Term
     */
    @Override
    public void call(final ByteArray term) {
      if (null != term) {
        this.target.put(new ByteArray(term), getQueryModel(term, this.fbDocIds,
            this.queryTerms));
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for document model creation.
   */
  private final class DocumentModelCalculatorTarget
      extends TargetFuncCall.TargetFunc<Integer> {

    /**
     * Empty constructor for parent class access.
     */
    DocumentModelCalculatorTarget() {
    }

    /**
     * Calculate the document model.
     *
     * @param docId Current document-id
     */
    @Override
    public void call(final Integer docId) {
      if (docId != null) {
        getDocumentModel(docId);
      }
    }
  }
}
