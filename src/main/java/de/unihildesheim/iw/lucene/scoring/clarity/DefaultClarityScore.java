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
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.document.Feedback;
import de.unihildesheim.iw.lucene.index.ExternalDocTermDataManager;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.query.TermsQueryBuilder;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.AtomicDouble;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.mapdb.Atomic;
import org.mapdb.DB;
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
 * Yun Zhou, and W. Bruce Croft.
 * <p/>
 * Reference:
 * <p/>
 * “Predicting Query Performance.” In Proceedings of the 25th Annual
 * International ACM SIGIR Conference on Research and Development in Information
 * Retrieval, 299–306. SIGIR ’02. New York, NY, USA: ACM, 2002.
 * doi:10.1145/564376.564429.
 *
 * @author Jens Bertram
 */
public final class DefaultClarityScore
    implements ClarityScoreCalculation {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      DefaultClarityScore.class);

  /**
   * Prefix used to identify externally stored data.
   */
  static final String IDENTIFIER = "DCS";

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
    DM
  }

  /**
   * Cached storage of Document-id -> Term, model-value.
   */
  private Map<Integer, Map<ByteArray, Double>> docModelDataCache;

  /**
   * Cached mapping of document model values.
   */
  private Map<ByteArray, Double> defaultDocModels;

  /**
   * Configuration object used for all parameters of the calculation.
   */
  private DefaultClarityScoreConfiguration conf;

  /**
   * Flag indicating, if a cache is set.
   */
  private boolean hasCache;

  /**
   * Database to use.
   */
  private DB db;

  /**
   * Manager object for extended document data.
   */
  private ExternalDocTermDataManager extDocMan;

  /**
   * {@link IndexDataProvider} to use.
   */
  private IndexDataProvider dataProv;

  /**
   * Reader to access Lucene index.
   */
  private IndexReader idxReader;

  /**
   * Provider for general index metrics.
   */
  Metrics metrics;

  /**
   * Default constructor. Called from builder.
   */
  private DefaultClarityScore() {
    super();
  }

  /**
   * Builder method to create a new instance.
   *
   * @param builder Builder to use for constructing the instance
   * @return New instance
   */
  protected static DefaultClarityScore build(final Builder builder)
      throws IOException, Buildable.BuildableException {
    Objects.requireNonNull(builder);
    final DefaultClarityScore instance = new DefaultClarityScore();

    // set configuration
    instance.conf = builder.getConfiguration();
    instance.dataProv = builder.idxDataProvider;
    instance.idxReader = builder.idxReader;
    instance.metrics = new Metrics(builder.idxDataProvider);
    instance.docModelDataCache = new ConcurrentHashMap<>();

    instance.conf.debugDump();

    // initialize
    instance.initCache(builder);

    return instance;
  }

  /**
   * Initializes a cache.
   *
   * @param builder Builder instance
   * @throws IOException Thrown on low-level I/O errors
   */
  @SuppressWarnings("checkstyle:magicnumber")
  private void initCache(final Builder builder)
      throws IOException, Buildable.BuildableException {
    final Persistence.Builder psb = builder.persistenceBuilder;
    psb.setDbDefaults();

    final Persistence persistence;
    boolean createNew = false;
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

    this.db = persistence.db;

    if (!createNew) {
      if (this.dataProv.getLastIndexCommitGeneration() == null) {
        LOG.warn("Index commit generation not available. Assuming an " +
            "unchanged index!");
      } else {
        if (!persistence.getMetaData()
            .generationCurrent(this.dataProv.getLastIndexCommitGeneration())) {
          throw new IllegalStateException("Index changed since last caching.");
        }
      }
      if (!this.db.getAtomicString(Caches.LANGMODEL_WEIGHT.name()).get().
          equals(this.conf.getLangModelWeight().toString())) {
        throw new IllegalStateException(
            "Different language-model weight used in cache.");
      }
      if (!persistence.getMetaData()
          .fieldsCurrent(this.dataProv.getDocumentFields())) {
        throw new IllegalStateException(
            "Current fields are different from cached ones.");
      }
      if (!persistence.getMetaData().stopWordsCurrent(this.dataProv
          .getStopwords())) {
        throw new IllegalStateException(
            "Current stopwords are different from cached ones.");
      }
    } else {
      this.db.createAtomicString(Caches.LANGMODEL_WEIGHT.name(), this.conf.
          getLangModelWeight().toString());
      persistence.updateMetaData(this.dataProv.getDocumentFields(),
          this.dataProv.getStopwords());
    }

    this.defaultDocModels = this.db
        .createHashMap(Caches.DEFAULT_DOC_MODELS.name())
        .keySerializer(ByteArray.SERIALIZER)
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();
    this.extDocMan = new ExternalDocTermDataManager(this.db, IDENTIFIER);
    this.hasCache = true;
  }

  /**
   * Calculate or get the default value, if the term is not contained in
   * document.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   */
  double getDefaultDocumentModel(final ByteArray term) {
    assert (this.defaultDocModels != null); // must be initialized

    Double model = this.defaultDocModels.get(term);
    if (model == null) {
      model = ((1d - this.conf.getLangModelWeight()) *
          this.metrics.collection.relTf(term));
      this.defaultDocModels.put(term.clone(), model);
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
    Map<ByteArray, Double> map;
    if (this.docModelDataCache.containsKey(docId)) {
      // get local cached map
      map = this.docModelDataCache.get(docId);
    } else {
      // get external cached map
      map = this.extDocMan.getData(docId, DataKeys.DM.name());
      // build mapping
      if (map == null || map.isEmpty()) {
        final DocumentModel docModel = this.metrics.getDocumentModel(docId);
        final Set<ByteArray> terms = docModel.termFreqMap.keySet();

        map = new HashMap<>(terms.size());
        for (final ByteArray term : docModel.termFreqMap.keySet()) {
          final Double model = (this.conf.getLangModelWeight()
              * docModel.metrics().relTf(term))
              + getDefaultDocumentModel(term);

          map.put(term, model);
        }
        // push data to persistent storage
        this.extDocMan.setData(docModel.id, DataKeys.DM.name(), map);
      }
      // push to local cache
      this.docModelDataCache.put(docId, map);
    }
    return map;
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
  double getQueryModel(final ByteArray term, final Set<Integer> fbDocIds,
      final Set<ByteArray> qTerms) {
    double model = 1d;
    // throw all terms together
    final List<ByteArray> terms = new ArrayList<>(qTerms);
    terms.add(term);
    // document model map of a current document
    Map<ByteArray, Double> docModMap;

    // go through all documents
    for (final Integer docId : fbDocIds) {
      docModMap = getDocumentModel(docId);
      assert docModMap != null;

      for (final ByteArray aTerm : terms) {
        if (docModMap.containsKey(aTerm)) {
          model *= docModMap.get(aTerm);
        } else {
          model *= getDefaultDocumentModel(aTerm);
        }
      }
    }

    return model;
  }

  /**
   * Pre-calculate all document models for all terms known from the index. This
   * simpliy runs the calculation for each term and stores it's value to a
   * persistent cache.
   */
  public void preCalcDocumentModels()
      throws ProcessingException {
    if (!this.hasCache) {
      LOG.warn("Won't pre-calculate any values. Cache not set.");
      return;
    }
    final Atomic.Boolean hasData = this.db.getAtomicBoolean(
        Caches.HAS_PRECALC_DATA.name());
    if (hasData.get()) {
      LOG.info("Pre-calculated models are current.");
    } else {
      new Processing(
          new TargetFuncCall<>(
              this.dataProv.getDocumentIdSource(),
              new DocumentModelCalculatorTarget()
          )
      ).process(this.metrics.collection.numberOfDocuments().intValue());
      hasData.set(true);
    }
  }

  /**
   * Calculate the clarity score. Calculation is based on a set of feedback
   * documents and a list of query terms.
   *
   * @param feedbackDocuments Document-ids of feedback documents
   * @param queryTerms Query terms
   * @return Result of the calculation
   */
  private Result calculateClarity(final Set<Integer> feedbackDocuments,
      final Set<ByteArray> queryTerms)
      throws IOException, ProcessingException {
    // check if models are pre-calculated
    final Result result = new Result(this.getClass());
    result.setConf(this.conf);
    result.setFeedbackDocIds(feedbackDocuments);
    result.setQueryTerms(queryTerms);

    this.docModelDataCache = new ConcurrentHashMap<>(feedbackDocuments.size());

    // models generated for the query model calculation.
    final ConcurrentMap<Integer, Double> queryModels =
        new ConcurrentHashMap<>(feedbackDocuments.size());

    // remove terms not in index, their value is zero
    final Iterator<ByteArray> queryTermsIt = queryTerms.iterator();
    ByteArray queryTerm;
    while (queryTermsIt.hasNext()) {
      queryTerm = queryTermsIt.next();
      if (this.metrics.collection.tf(queryTerm) == 0L) {
        queryTermsIt.remove();
      }
    }

    // Short circuit, if no terms are left. The score will be zero.
    if (queryTerms.isEmpty()) {
      LOG.warn("No query term matched in index. Result is 0.");
      result.setScore(0d);
      return result;
    }

    // calculate multi-threaded
    final Processing p = new Processing();

    // get a unique list of terms from all feedback documents
    final Set<ByteArray> termSet = this.dataProv.getDocumentsTermSet(
        feedbackDocuments);

    // calculate the query model for each term from the feedback documents
    final ConcurrentMap<ByteArray, Double> queryModelMap =
        new ConcurrentHashMap<>(feedbackDocuments.size());
    p.setSourceAndTarget(
        new TargetFuncCall<>(
            new CollectionSource<>(termSet),
            new QueryModelCalculatorTarget(queryTerms, feedbackDocuments,
                queryModelMap)
        )
    ).process(termSet.size());

    // now calculate the score using all now calculated values
    final AtomicDouble score = new AtomicDouble(0);
    p.setSourceAndTarget(
        new TargetFuncCall<>(
            new CollectionSource<>(termSet),
            new ScoreCalculatorTarget(queryModelMap, score)
        )
    ).process(termSet.size());

    result.setScore(score.doubleValue());
    return result;
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
    if (Objects.requireNonNull(query).trim().isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final QueryUtils queryUtils =
        new QueryUtils(this.idxReader, this.dataProv.getDocumentFields());

    final Query queryObj;
    try {
      queryObj = new TermsQueryBuilder(this.idxReader,
          this.dataProv.getDocumentFields()).query(query).build();
    } catch (Buildable.BuildableException e) {
      throw new ClarityScoreCalculationException(
          "Caught exception while building query.", e);
    }

    // get feedback documents
    final Set<Integer> feedbackDocuments;
    try {
      feedbackDocuments = Feedback.getFixed(this.idxReader, queryObj,
          this.conf.getFeedbackDocCount());

    } catch (IOException ex) {
      LOG.error("Caught exception while preparing calculation.", ex);
      return null;
    }
    if (feedbackDocuments == null || feedbackDocuments.isEmpty()) {
      throw new IllegalStateException("No feedback documents.");
    }

    final Set<ByteArray> queryTerms;
    try {
      // Get unique query terms
      queryTerms = queryUtils.getUniqueQueryTerms(query);
    } catch (UnsupportedEncodingException | ParseException e) {
      LOG.error("Caught exception parsing query.", e);
      return null;
    } catch (Buildable.BuildableException e) {
      LOG.error("Caught exception building query.", e);
      return null;
    }

    if (queryTerms == null || queryTerms.isEmpty()) {
      throw new IllegalStateException("No query terms.");
    }

    timeMeasure.stop();
    try {
      final Result r = calculateClarity(feedbackDocuments, queryTerms);
      LOG.debug("Calculating default clarity score for query '{}' "
              + "with {} document models took {}. {}", query,
          feedbackDocuments.size(), timeMeasure.getTimeString(), r.getScore()
      );
      return r;
    } catch (IOException | ProcessingException e) {
      throw new ClarityScoreCalculationException(e);
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
     */
    public ScoreCalculatorTarget(
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
      if (term != null) {
        final Double pq = this.qModMap.get(term);
        assert pq != null;
        this.target.addAndGet(pq * MathUtils.log2(pq / metrics.collection
            .relTf(term)));
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
    QueryModelCalculatorTarget(
        final Set<ByteArray> qTerms,
        final Set<Integer> feedbackDocuments,
        final ConcurrentMap<ByteArray, Double> queryModelMap) {
      super();
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
      if (term != null) {
        this.target.put(term.clone(), getQueryModel(term, this.fbDocIds,
            this.queryTerms));
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for document model creation.
   */
  private final class DocumentModelCalculatorTarget
      extends
      TargetFuncCall.TargetFunc<Integer> {

    /**
     * @param docId
     */
    @Override
    public void call(final Integer docId) {
      if (docId != null) {
        getDocumentModel(docId);
      }
    }
  }

  /**
   * Extended result object containing additional meta information about what
   * values were actually used for calculation.
   */
  public static final class Result
      extends ClarityScoreResult {

    /**
     * Query terms used for calculation.
     */
    private Collection<ByteArray> queryTerms;
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
     *
     * @param cscType Type of the calculation class
     */
    protected Result(final Class<? extends ClarityScoreCalculation> cscType) {
      super(Objects.requireNonNull(cscType));

      this.feedbackDocIds = Collections.<Integer>emptyList();
      this.queryTerms = Collections.<ByteArray>emptyList();
    }

    /**
     * Set the list of feedback documents used.
     *
     * @param fbDocIds List of feedback documents
     */
    protected void setFeedbackDocIds(final Collection<Integer> fbDocIds) {
      Objects.requireNonNull(fbDocIds);

      this.feedbackDocIds = new ArrayList<>(fbDocIds.size());
      this.feedbackDocIds.addAll(fbDocIds);
    }

    /**
     * Set the query terms used.
     *
     * @param qTerms Query terms
     */
    protected void setQueryTerms(final Collection<ByteArray> qTerms) {
      Objects.requireNonNull(qTerms);
      this.queryTerms = new ArrayList<>(qTerms.size());
      this.queryTerms.addAll(qTerms);
    }

    /**
     * Set the value of the calculated score.
     *
     * @param score Calculated score
     */
    protected void setScore(final double score) {
      _setScore(score);
    }

    /**
     * Set the configuration that was used.
     *
     * @param newConf Configuration used
     */
    protected void setConf(final DefaultClarityScoreConfiguration newConf) {
      this.conf = Objects.requireNonNull(newConf);
    }

    /**
     * Get the query terms used for calculation.
     *
     * @return Query terms
     */
    public Collection<ByteArray> getQueryTerms() {
      return Collections.unmodifiableCollection(this.queryTerms);
    }

    /**
     * Get the collection of feedback documents used for calculation.
     *
     * @return Feedback documents used for calculation
     */
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
  }

  /**
   * Builder to create a new {@link DefaultClarityScore} instance.
   */
  public static final class Builder
      extends AbstractClarityScoreCalculationBuilder<Builder> {
    /**
     * Configuration to use.
     */
    private DefaultClarityScoreConfiguration configuration = new
        DefaultClarityScoreConfiguration();

    public Builder() {
      super(IDENTIFIER);
    }

    protected Builder getThis() {
      return this;
    }

    /**
     * Set the configuration to use.
     *
     * @param conf Configuration
     * @return Self reference
     */
    public Builder configuration(final DefaultClarityScoreConfiguration conf) {
      this.configuration = Objects.requireNonNull(conf);
      return this;
    }

    /**
     * Get the configuration to use.
     *
     * @return Configuration
     */
    protected DefaultClarityScoreConfiguration getConfiguration() {
      return this.configuration;
    }

    @Override
    public DefaultClarityScore build()
        throws BuildableException {
      validate();
      try {
        return DefaultClarityScore.build(this);
      } catch (IOException e) {
        throw new BuildException(e);
      }
    }

    @Override
    public void validate()
        throws ConfigurationException {
      super.validate();
      super.validatePersistenceBuilder();
    }
  }
}
