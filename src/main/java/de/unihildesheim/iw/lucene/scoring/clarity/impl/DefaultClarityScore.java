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
package de.unihildesheim.iw.lucene.scoring.clarity.impl;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.SupportsPersistence;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.lucene.Environment;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.document.Feedback;
import de.unihildesheim.iw.lucene.index.ExternalDocTermDataManager;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.metrics.CollectionMetrics;
import de.unihildesheim.iw.lucene.metrics.DocumentMetrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.query.TermsQueryBuilder;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.iw.util.ByteArrayUtil;
import de.unihildesheim.iw.util.Configuration;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.AtomicDouble;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default Clarity Score implementation as described by Cronen-Townsend, Steve,
 * Yun Zhou, and W. Bruce Croft.
 * <p>
 * Reference:
 * <p>
 * “Predicting Query Performance.” In Proceedings of the 25th Annual
 * International ACM SIGIR Conference on Research and Development in Information
 * Retrieval, 299–306. SIGIR ’02. New York, NY, USA: ACM, 2002.
 * doi:10.1145/564376.564429.
 *
 * @author Jens Bertram
 */
public final class DefaultClarityScore
    implements ClarityScoreCalculation,
               SupportsPersistence {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      DefaultClarityScore.class);

  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link IndexDataProvider}.
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
    DM,
    /**
     * Flag to indicate, if all document-models have already been
     * pre-calculated. Stored in the {@link IndexDataProvider}.
     */
    DOCMODELS_PRECALCULATED
  }

  /**
   * Cached storage of Document-id -> Term, model-value.
   */
  private Map<Integer, Map<ByteArray, Object>> docModelDataCache;

  /**
   * Cached mapping of document model values.
   */
  private Map<ByteArray, Double> defaultDocModels = null;

  /**
   * Configuration object used for all parameters of the calculation.
   */
  private DefaultClarityScoreConfiguration conf;

  /**
   * Flag indicating, if caches are temporary.
   */
  private boolean cacheTemporary = false;
  /**
   * Flag indicating, if a cache is set.
   */
  private boolean hasCache = false;

  /**
   * Database to use.
   */
  private DB db;
  /**
   * Manager object for extended document data.
   */
  private ExternalDocTermDataManager extDocMan;

  /**
   * Create a new scoring instance with the default parameter set.
   */
  public DefaultClarityScore() {
    this(new DefaultClarityScoreConfiguration());
  }

  /**
   * Create a new scoring instance with the parameters set in the given
   * configuration.
   *
   * @param newConf Configuration
   */
  public DefaultClarityScore(final Configuration newConf) {
    super();
    setConfiguration(newConf);
  }

  /**
   * Tries to load a persistent cache and creates a new one, if none found.
   *
   * @param name Cache name
   * @throws IOException Thrown on low-level I/O errors
   * @throws Environment.NoIndexException Thrown, if no index is provided in the
   * {@link Environment}
   */
  @Override
  public void loadOrCreateCache(final String name)
      throws IOException,
             Environment.NoIndexException {
    initCache(name, false, true);
  }

  /**
   * Creates a new persistent cache. Throws an exception if one with the given
   * name already exist.
   *
   * @param name Cache name
   * @throws IOException Thrown on low-level I/O errors
   * @throws Environment.NoIndexException Thrown, if no index is provided in the
   * {@link Environment}
   */
  @Override
  public void createCache(final String name)
      throws IOException,
             Environment.NoIndexException {
    initCache(name, true, true);
  }

  /**
   * Tries to load a persistent cache. Throws an exception if none with the
   * given name exist.
   *
   * @param name Cache name
   * @throws IOException Thrown on low-level I/O errors
   * @throws Environment.NoIndexException Thrown, if no index is provided in the
   * {@link Environment}
   */
  @Override
  public void loadCache(final String name)
      throws IOException,
             Environment.NoIndexException {
    initCache(name, false, false);
  }

  /**
   * Debug method. Get the local manager for external document data.
   *
   * @return Document data manager
   */
  protected ExternalDocTermDataManager testGetExtDocMan() {
    return this.extDocMan;
  }

  /**
   * Initializes a cache.
   *
   * @param name Cache name
   * @param createNew If true, a new cache will be created. Throws an exception
   * if one with the given name already exist.
   * @param createIfNeeded If true, creates a new cache if none found
   * @throws IOException Thrown on low-level I/O errors
   * @throws Environment.NoIndexException Thrown, if no index is provided in the
   * {@link Environment}
   */
  @SuppressWarnings("checkstyle:magicnumber")
  private void initCache(final String name, boolean createNew,
      final boolean createIfNeeded)
      throws IOException,
             Environment.NoIndexException {
    final Persistence.Builder psb;
    if (Environment.isTestRun() || this.cacheTemporary) {
      psb = new Persistence.Builder(IDENTIFIER
                                    + "_" + name + "_" +
                                    RandomValue.getString(6)).temporary();
    } else {
      psb = new Persistence.Builder(IDENTIFIER + "_" + name);
    }
    psb.setDbDefaults();

    if (!psb.exists() && createIfNeeded) {
      createNew = true;
    }

    // wrapper for persistent data storage.
    final Persistence pData;
    if (createNew) {
      pData = psb.make();
    } else if (!createIfNeeded) {
      pData = psb.get();
    } else {
      pData = psb.makeOrGet();
    }
    this.db = pData.db;

    if (!createNew) {
      if (!pData.getMetaData().generationCurrent()) {
        throw new IllegalStateException(
            "Index changed since last caching.");
      }
      if (!this.db.getAtomicString(Caches.LANGMODEL_WEIGHT.name()).get().
          equals(this.conf.getLangModelWeight().toString())) {
        throw new IllegalStateException(
            "Different language-model weight used in cache.");
      }
      if (!pData.getMetaData().fieldsCurrent()) {
        throw new IllegalStateException(
            "Current fields are different from cached ones.");
      }
      if (!pData.getMetaData().stopWordsCurrent()) {
        throw new IllegalStateException(
            "Current stopwords are different from cached ones.");
      }
    } else {
      this.db.createAtomicString(Caches.LANGMODEL_WEIGHT.name(), this.conf.
          getLangModelWeight().toString());
      pData.updateMetaData();
    }

    this.defaultDocModels = this.db
        .createHashMap(Caches.DEFAULT_DOC_MODELS.name())
        .keySerializer(ByteArray.SERIALIZER)
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();
    this.extDocMan = new ExternalDocTermDataManager(this.db, IDENTIFIER);
    this.hasCache = true;
  }

  @Override
  public DefaultClarityScore setConfiguration(final Configuration newConf) {
    if (!(newConf instanceof DefaultClarityScoreConfiguration)) {
      throw new IllegalArgumentException("Wrong configuration type.");
    }
    this.conf = (DefaultClarityScoreConfiguration) newConf;
    this.conf.debugDump();
    return this;
  }

  /**
   * Calculates the default value, if the term is not contained in document.
   * This value is also part of the regular calculation formula.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   */
  private double calcDefaultDocumentModel(final ByteArray term) {
    assert (this.defaultDocModels != null); // must be initialized
    Double model = this.defaultDocModels.get(term);
    if (model == null) {
      model = ((1 - this.conf.getLangModelWeight()) * CollectionMetrics.relTf(
          term));
      this.defaultDocModels.put(term.clone(), model);
    }
    return model;
  }

  /**
   * Calculate the document distribution of a term, document model (pD).
   *
   * @param docModel Document model to do the calculation for
   */
  void calcDocumentModel(final DocumentModel docModel) {
    for (ByteArray term : docModel.termFreqMap.keySet()) {
      final Double model = (this.conf.getLangModelWeight()
                            * docModel.metrics().relTf(term))
                           + calcDefaultDocumentModel(term);

      this.extDocMan.setData(docModel.id, term.clone(), DataKeys.DM.name(),
          model);
    }
  }

  /**
   * Get the document language model for a given term.
   *
   * @param docId Document model to do the calculation for
   * @param term Term to do the calculation for
   * @return Calculated language model for the given document and term or
   * <tt>null</tt> if the term was not found in the document
   */
  private Tuple.Tuple2<Double, Boolean> getDocumentModel(
      final Integer docId, final ByteArray term) {
    Double model;
    boolean isDefault = false;
    if (this.docModelDataCache.containsKey(docId)) {
      model = (Double) this.docModelDataCache.get(docId).get(term);
    } else {
      Map<ByteArray, Object> td = this.extDocMan.getData(docId, DataKeys.DM.
          name());
      if (td == null || td.isEmpty()) {
        calcDocumentModel(DocumentMetrics.getModel(docId));
        td = this.extDocMan.getData(docId, DataKeys.DM.name());
      }
      model = (Double) td.get(term);
      this.docModelDataCache.put(docId, td);
    }

    if (model == null) {
      isDefault = true;
      model = calcDefaultDocumentModel(term);
    }
    return Tuple.tuple2(model, isDefault);
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   */
  public void preCalcDocumentModels() {
    if (!this.hasCache) {
      LOG.warn("Won't pre-calculate any values. Cache not set.");
      return;
    }
    final Atomic.Boolean hasData = this.db.getAtomicBoolean(
        Caches.HAS_PRECALC_DATA.name());
    if (hasData.get()) {
      LOG.info("Precalculated models are current.");
    } else {
      new Processing(
          new Target.TargetFuncCall<>(
              Environment.getDataProvider().getDocumentIdSource(),
              new DocumentModelCalculatorTarget()
          )
      ).process(CollectionMetrics.numberOfDocuments().intValue());
      hasData.set(true);
    }
  }

  /**
   * Calculate the clarity score.
   *
   * @param feedbackDocuments Document ids of feedback documents
   * @param queryTerms Query terms
   * @return Result of the calculation
   */
  private Result calculateClarity(final Collection<Integer> feedbackDocuments,
      final Collection<ByteArray> queryTerms)
      throws IOException,
             Environment.NoIndexException {
    // check if models are pre-calculated
    final Result result = new Result(this.getClass());
    result.setConf(this.conf);
    result.setFeedbackDocIds(feedbackDocuments);
    result.setQueryTerms(queryTerms);

    this.docModelDataCache = new ConcurrentHashMap<>(feedbackDocuments.size());
    // models generated for the query model calculation.
    final Map<Integer, Double> queryModels =
        new ConcurrentHashMap<>(feedbackDocuments.size());

    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // convert query to readable string for logging output
    @SuppressWarnings("StringBufferWithoutInitialCapacity")
    StringBuilder queryStr = new StringBuilder();
    for (ByteArray ba : queryTerms) {
      queryStr.append(ByteArrayUtil.utf8ToString(ba)).append(' ');
    }
    LOG.info("Calculating clarity score. query={}", queryStr);

    // remove terms not in index, their value is zero
    final Iterator<ByteArray> queryTermsIt = queryTerms.iterator();
    ByteArray queryTerm;
    while (queryTermsIt.hasNext()) {
      queryTerm = queryTermsIt.next();
      if (CollectionMetrics.tf(queryTerm) == 0L) {
        queryTermsIt.remove();
      }
    }

    // short circuit, if no terms are left
    if (queryTerms.isEmpty()) {
      LOG.warn("No query term matched in index. Result is 0.");
      result.setScore(0d);
      return result;
    }

    final Processing p = new Processing();

    // calculate query models for feedback documents.
    LOG.info("Calculating query models for {} feedback documents.",
        feedbackDocuments.size());

    p.setSourceAndTarget(
        new Target.TargetFuncCall<>(
            new CollectionSource<>(feedbackDocuments),
            new QueryModelCalulatorTarget(queryTerms, queryModels)
        )
    ).process(feedbackDocuments.size());

    final AtomicDouble score = new AtomicDouble(0);
//    final Source<ByteArray> sourceCollection;

    // calculation with all terms from collection
//  LOG.info("Calculating query probability values "
//      + "for {} unique terms in collection.", this.dataProv.
//      getUniqueTermsCount());
//  sourceCollection = this.dataProv.getTermsSource();
    // collection with query terms only
//  LOG.info("Calculating query probability values for {} query terms.",
//      this.queryTerms.size());
//  sourceCollection = new CollectionSource<>(this.queryTerms);
    // terms from feedback documents only
    final Collection<ByteArray> termSet = Environment.getDataProvider().
        getDocumentsTermSet(feedbackDocuments);

    LOG.info("Calculating query probability values "
             + "for {} feedback documents.", feedbackDocuments.size());
    p.setSourceAndTarget(
        new Target.TargetFuncCall<>(
            new CollectionSource<>(termSet),
            new QueryProbabilityCalculatorTarget(
                this.conf.getLangModelWeight(),
                queryModels,
                feedbackDocuments, score)
        )
    ).process(termSet.size());

    result.setScore(score.doubleValue());

    timeMeasure.stop();
    LOG.debug("Calculating default clarity score for query {} "
              + "with {} document models took {}. {}", queryStr,
        feedbackDocuments.size(), timeMeasure.getTimeString(), score
    );

    return result;
  }

  @Override
  public Result calculateClarity(final String query)
      throws
      ParseException, Environment.NoIndexException, IOException {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }

    final Query queryObj = TermsQueryBuilder.buildFromEnvironment(query);

    // get feedback documents
    final Collection<Integer> feedbackDocuments;
    try {
      feedbackDocuments = Feedback.getFixed(queryObj, this.conf.
          getFeedbackDocCount());

    } catch (IOException ex) {
      LOG.error("Caught exception while preparing calculation.", ex);
      return null;
    }
    if (feedbackDocuments == null || feedbackDocuments.isEmpty()) {
      throw new IllegalStateException("No feedback documents.");
    }

    if (!this.hasCache) {
      this.cacheTemporary = true;
      try {
        initCache("temp", true, true);
      } catch (IOException ex) {
        throw new IllegalStateException("Error creating cache.", ex);
      }
    }

    final Collection<ByteArray> queryTerms;
    try {
      // Get unique query terms
      queryTerms = QueryUtils.getUniqueQueryTerms(query);
    } catch (UnsupportedEncodingException | ParseException ex) {
      LOG.error("Caught exception parsing query.", ex);
      return null;
    }

    if (queryTerms == null || queryTerms.isEmpty()) {
      throw new IllegalStateException("No query terms.");
    }

    return calculateClarity(feedbackDocuments, queryTerms);
  }

  /**
   * {@link Processing} {@link Target} for query model calculation.
   */
  private final class QueryModelCalulatorTarget
      extends
      Target.TargetFunc<Integer> {

    /**
     * List of query terms used.
     */
    private final Collection<ByteArray> queryTerms;
    /**
     * Map to gather calculation results. (Document-id -> model)
     */
    private final Map<Integer, Double> map;

    QueryModelCalulatorTarget(final Collection<ByteArray> qTerms,
        final Map<Integer, Double> targetMap) {
      this.queryTerms = qTerms;
      this.map = targetMap;
    }

    @Override
    public void call(final Integer docId) {
      if (docId != null) {
        double modelWeight = 1d;
        for (ByteArray term : this.queryTerms) {
          modelWeight *= getDocumentModel(docId, term).a;
        }
        this.map.put(docId, modelWeight);
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for query model calculation.
   */
  private class QueryProbabilityCalculatorTarget
      extends Target.TargetFunc<ByteArray> {

    /**
     * List of feedback document-ids.
     */
    private final Collection<Integer> fbDocIds;
    /**
     * Shared results of calculation.
     */
    private final AtomicDouble result;
    /**
     * Weighting factor.
     */
    private final double weightFactor;
    /**
     * Pre-calculated query models.
     */
    private final Map<Integer, Double> queryModels;

    /**
     * @param langModelWeight Language model weighting factor to use
     * @param qModelsMap Map with pre-calculated query models
     * @param feedbackDocuments Feedback documents to use for calculation
     * @param newResult Shared value of calculation results
     */
    QueryProbabilityCalculatorTarget(
        final double langModelWeight,
        final Map<Integer, Double> qModelsMap,
        final Collection<Integer> feedbackDocuments,
        final AtomicDouble newResult) {
      super();
      this.weightFactor = 1 - langModelWeight;
      this.fbDocIds = feedbackDocuments;
      this.result = newResult;
      this.queryModels = qModelsMap;
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        // calculate the query probability of the current term
        double qLangMod = 0d;
        final double termRelFreq = CollectionMetrics.relTf(term);
        Tuple.Tuple2<Double, Boolean> model;

        for (Integer docId : this.fbDocIds) {
          model = getDocumentModel(docId, term);
          if (model.b) {
            qLangMod += this.weightFactor * termRelFreq * this.queryModels.
                get(docId);
          } else {
            qLangMod += model.a * this.queryModels.get(docId);
          }
        }

        // calculate logarithmic part of the formulary
        final double log = (Math.log(qLangMod) / Math.log(2)) - (Math.log(
            termRelFreq) / Math.log(2));
        // add up final score for each term
        this.result.addAndGet(qLangMod * log);
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for document model creation.
   */
  private final class DocumentModelCalculatorTarget
      extends
      Target.TargetFunc<Integer> {

    @Override
    public void call(final Integer docId) {
      if (docId != null) {
        final DocumentModel docModel = DocumentMetrics.getModel(docId);
        if (docModel == null) {
          LOG.warn("({}) Model for document-id {} was null.", this.
              getName(), docId);
        } else {
          // call the calculation method of the main class for each
          // document and term that is available for processing
          calcDocumentModel(docModel);
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
    public Result(final Class<? extends ClarityScoreCalculation> cscType) {
      super(cscType);
      this.feedbackDocIds = Collections.<Integer>emptyList();
      this.queryTerms = Collections.<ByteArray>emptyList();
    }

    /**
     * Set the list of feedback documents used.
     *
     * @param fbDocIds List of feedback documents
     */
    protected void setFeedbackDocIds(final Collection<Integer> fbDocIds) {
      this.feedbackDocIds = new ArrayList<>(fbDocIds.size());
      this.feedbackDocIds.addAll(fbDocIds);
    }

    /**
     * Set the query terms used.
     *
     * @param qTerms Query terms
     */
    protected void setQueryTerms(final Collection<ByteArray> qTerms) {
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
      this.conf = newConf;
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
}