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
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.document.Feedback;
import de.unihildesheim.iw.lucene.index.ExternalDocTermDataManager;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.query.TryExactTermsQuery;
import de.unihildesheim.iw.mapdb.DBMakerUtils;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.AtomicDouble;
import de.unihildesheim.iw.util.concurrent.processing.IteratorSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.mapdb.DB;
import org.mapdb.DBMaker;
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
   * How long to wait for a model currently being calculated. If the timeout is
   * hit the model will be calculated, regardless if it's already done in
   * another thread.
   */
  private static final long DOC_MODEL_CALC_WAIT_TIMEOUT = 1000L;
  /**
   * Provider for general index metrics.
   */
  protected final Metrics metrics;
  /**
   * {@link IndexDataProvider} to use.
   */
  final IndexDataProvider dataProv;
  /**
   * Configuration object used for all parameters of the calculation.
   */
  private final DefaultClarityScoreConfiguration conf;
  /**
   * Reader to access Lucene index.
   */
  private final IndexReader idxReader;
  /**
   * Analyzer for parsing queries.
   */
  private final Analyzer analyzer;
  /**
   * Set of feedback documents to use for calculation.
   */
  protected Set<Integer> feedbackDocIds;
  /**
   * Manager object for extended document data.
   */
  ExternalDocTermDataManager extDocMan;
  /**
   * Flag indicating, if this instance has been closed.
   */
  private volatile boolean isClosed;
  /**
   * Database to use.
   */
  private Persistence persist;
  /**
   * List of query terms provided.
   */
  private Map<ByteArray, Integer> queryTerms;

  private DB cache = DBMaker.newMemoryDirectDB()
      .transactionDisable()
      .make();
  private Map<Integer, Double> queryModelPartCache = cache
      .createHashMap("qmCache")
      .keySerializer(Serializer.INTEGER)
      .valueSerializer(Serializer.BASIC)
      .expireStoreSize(0.5) // size in GB
      .make();
  private Map<ByteArray, Double> defaultDocModelCache = cache
      .createTreeMap("ddmCache")
      .keySerializer(ByteArray.SERIALIZER_BTREE)
      .valueSerializer(Serializer.BASIC)
      .make();

  /**
   * Create a new instance using a builder.
   *
   * @param builder Builder to use for constructing the instance
   * @throws Buildable.BuildableException Thrown, if building the persistent
   * cache failed
   */
  private DefaultClarityScore(final Builder builder)
      throws Buildable.BuildableException {
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    this.conf = builder.getConfiguration();
    this.dataProv = builder.idxDataProvider;
    this.idxReader = builder.idxReader;
    this.metrics = new Metrics(builder.idxDataProvider);
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
    psb.dbMkr.compressionDisable()
        .noChecksum();

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

    this.extDocMan =
        new ExternalDocTermDataManager(this.persist.getDb(), IDENTIFIER);
    if (clearCache) {
      LOG.info("Clearing document model data cache.");
      this.extDocMan.clear();
    }
  }

  void testSetValues(final Set<Integer> testFbDocIds,
      final Map<ByteArray, Integer> testQueryTerms) {
    this.feedbackDocIds = testFbDocIds;
    this.queryTerms = testQueryTerms;
    this.defaultDocModelCache.clear();
    this.queryModelPartCache.clear();
    this.extDocMan.clear();
    cacheDocumentModels();
  }

  @SuppressWarnings("ObjectAllocationInLoop")
  private void cacheDocumentModels() {
    Map<ByteArray, Double> map;
    final double langModelWeight = this.conf.getLangModelWeight();

    final DB memDb = DBMakerUtils.newCompressedMemoryDirectDB().make();
    final Set<ByteArray> fbTerms = memDb.createTreeSet("fbTerms").make();

    // get an iterator over all feedback terms
    final Iterator<Map.Entry<ByteArray, Long>> fbTermIt = this.dataProv
        .getDocumentsTerms(this.feedbackDocIds);

    while (fbTermIt.hasNext()) {
      fbTerms.add(fbTermIt.next().getKey());
    }

    // empty caches
    this.defaultDocModelCache.clear();
    this.queryModelPartCache.clear();

    // calculate default models for documents that do not contain a
    // specific feedback term
    for (final ByteArray term : fbTerms) {
      this.defaultDocModelCache.put(term, getDefaultDocumentModel(term));
    }

    // calculate default models for documents that do not contain a
    // specific query term
    for (final ByteArray term : this.queryTerms.keySet()) {
      this.defaultDocModelCache.put(term, getDefaultDocumentModel(term));
    }

    // run pre-calculation for all feedback documents
    for (final Integer docId : this.feedbackDocIds) {
      final DocumentModel docModel = this.metrics.getDocumentModel(docId);
      map = this.extDocMan.getData(docId, DataKeys.DOCMODEL.id());
      if (map == null || map.isEmpty()) {
        map = new HashMap<>((int) ((double) this.queryTerms.size() * 1.5));
      }

      // calculate model values for all unique feedback terms
      for (final ByteArray term : fbTerms) {
        // skip, if term is not in document - ad-hoc calculation is cheap
        if (docModel.contains(term) && !map.containsKey(term)) {
          final Double model = (
              langModelWeight * docModel.metrics().relTf(term))
              + this.defaultDocModelCache.get(term);
          map.put(term, model);
        }
      }

      // calculate model values for all unique query terms
      for (final ByteArray term : this.queryTerms.keySet()) {
        if (!map.containsKey(term)) {
          if (docModel.contains(term)) {
            map.put(term, (langModelWeight
                * docModel.metrics().relTf(term))
                + this.defaultDocModelCache.get(term));
          }
        }
      }

      if (!map.isEmpty()) {
        this.extDocMan.setData(docId, DataKeys.DOCMODEL.id(), map);
      }
    }

    memDb.close();
    this.persist.getDb().compact();
  }

//  /**
//   * Calculate or get the document model (pd) for a specific document and
// term.
//   *
//   * @param docId Id of the document whose model to get
//   * @param term to calculate for
//   * @return Model value
//   */
//  @SuppressWarnings("ReuseOfLocalVariable")
//  double getDocumentModel(final int docId, final ByteArray term) {
//    // try to get pre-calculated values
//    Map<ByteArray, Double> map =
//        this.extDocMan.getData(docId, DataKeys.DOCMODEL.name());
//    Double model = 0d;
//
//    if (map == null || map.isEmpty()) {
//      map = new HashMap<>((int) ((double) this.queryTerms.size() * 1.5));
//      final DocumentModel docModel = this.metrics.getDocumentModel(docId);
//
//      if (docModel.contains(term)) {
//        model = (this.conf.getLangModelWeight()
//            * this.metrics.getDocumentModel(docId).metrics().relTf(term))
//            + getDefaultDocumentModel(term);
//        map.put(term, model);
//      }
//
//      // push data to persistent storage
//      this.extDocMan.setData(docId, DataKeys.DOCMODEL.name(), map);
//    } else {
//      model = map.get(term);
//    }
//
//    if (model == null || model == 0d) {
//      return getDefaultDocumentModel(term);
//    }
//    return model;
//  }

  /**
   * Calculate or get the default value, if the term is not contained in
   * document.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   */
  double getDefaultDocumentModel(final ByteArray term) {
    return ((1d - this.conf.getLangModelWeight()) *
        this.metrics.collection().relTf(term));
  }

//  /**
//   * Calculate or get the document model (pd) for a specific document. This
// will
//   * pre-calculate all models for all query terms.
//   *
//   * @param docId Id of the document whose model to get
//   * @return Mapping of each term in the document to it's model value
//   */
//  Map<ByteArray, Double> getDocumentModel(final int docId) {
//    // try to get pre-calculated values
//    Map<ByteArray, Double> map =
//        this.extDocMan.getData(docId, DataKeys.DOCMODEL.name());
//
//    if (map == null || map.isEmpty()) {
//      LOG.debug("empty");
//      // store results for all query terms
//      map = new HashMap<>((int) (this.queryTerms.size() * 1.5));
//
//      double langModelWeight = this.conf.getLangModelWeight();
//      final DocumentModel docModel = this.metrics.getDocumentModel(docId);
//
//      // calculate model values for all unique query terms
//      for (final ByteArray term : this.queryTerms.keySet()) {
//        final Double model;
//        if (docModel.contains(term)) {
//          model = (langModelWeight
//              * docModel.metrics().relTf(term))
//              + getDefaultDocumentModel(term);
//        } else {
//          model = getDefaultDocumentModel(term);
//        }
//
//        assert 0d != model;
//        map.put(term, model);
//      }
//
//      // calculate model values for all unique feedback terms
//      final Iterator<Map.Entry<ByteArray, Long>> fbTermIt = this.dataProv
//          .getDocumentsTerms(this.feedbackDocIds);
//
//      while (fbTermIt.hasNext()) {
//        final ByteArray term = fbTermIt.next().getKey();
//        if (docModel.contains(term)) {
//          final Double model = (langModelWeight
//              * docModel.metrics().relTf(term))
//              + getDefaultDocumentModel(term);
//          map.put(term, model);
//        }
//      }
//
//      // push data to persistent storage
//      this.extDocMan.setData(docId, DataKeys.DOCMODEL.name(), map);
//    }
//    return map;
//  }

  /**
   * Calculate or get the query model for a given term. Calculation is based on
   * a set of feedback documents and a list of query terms.
   *
   * @param term Feedback document term
   * @return Model value
   */
  double getQueryModel(final ByteArray term) {
    // cache for query terms set
    Set<Map.Entry<ByteArray, Integer>> qtSet = null;
    double modelPart;
    Double currentDocValue;
    double model = 0d;
    // go through all documents
    for (final Integer docId : this.feedbackDocIds) {
      // calculate for all query terms (try cache first)
      try {
        modelPart = this.queryModelPartCache.get(docId);
      } catch (final NullPointerException e) {
        if (qtSet == null) {
          qtSet = this.queryTerms.entrySet();
        }
        modelPart = 1d;
        for (final Map.Entry<ByteArray, Integer> qTermEntry : qtSet) {
          currentDocValue = this.extDocMan.getDirect(
              DataKeys.DOCMODEL.id(), docId, qTermEntry.getKey());
          if (currentDocValue == null) {
            // term not in document
            currentDocValue =
                this.defaultDocModelCache.get(qTermEntry.getKey());
          }
          modelPart *= (currentDocValue * qTermEntry.getValue());
        }
        this.queryModelPartCache.put(docId, modelPart);
      }

      // .. and for the current feedback term
      try {
        model = (modelPart * this.defaultDocModelCache.get(term));
      } catch (final NullPointerException e) {
        model = (modelPart * this.defaultDocModelCache.get(term));
      }
    }
    return model;
  }

  /**
   * Close this instance and release any resources (mainly the database
   * backend).
   */
  @Override
  public void close() {
    if (!this.isClosed) {
      this.cache.close();
      this.persist.closeDb();
      this.isClosed = true;
    }
  }

  /**
   * Calculate the clarity score. This method does only pre-checks. The real
   * calculation is done in {@link #calculateClarity()}.
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
      queryObj =
          new TryExactTermsQuery(this.analyzer, QueryParser.escape(query),
              this.dataProv.getDocumentFields());
    } catch (final ParseException e) {
      final String msg = "Caught exception while building query.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    // get feedback documents
    try {
      this.feedbackDocIds =
          Feedback.getFixed(this.idxReader, queryObj,
              this.conf.getFeedbackDocCount());
    } catch (final IOException | ParseException e) {// | ParseException e) {
      final String msg = "Caught exception while preparing calculation.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    if (this.feedbackDocIds.isEmpty()) {
      final Result result = new Result();
      result.setEmpty("No feedback documents.");
      return result;
    }

    // get a normalized map of query terms
    try {
      final List<String> queryTermsStr = QueryUtils.tokenizeQueryString(
          query, this.analyzer);
      this.queryTerms = new HashMap<>(queryTermsStr.size());
      for (final String qTerm : queryTermsStr) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final ByteArray qTermBa = new ByteArray(qTerm.getBytes("UTF-8"));
        if (this.metrics.collection().tf(qTermBa) == 0) {
          LOG.info("Removing query term '{}'. Not in collection.", qTerm);
        } else {
          if (this.queryTerms.containsKey(qTermBa)) {
            this.queryTerms.put(qTermBa, this.queryTerms.get(qTermBa) + 1);
          } else {
            this.queryTerms.put(qTermBa, 1);
          }
        }
      }
    } catch (final UnsupportedEncodingException e) {
      final String msg = "Caught exception while parsing query.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    // check query term extraction result
    if (this.queryTerms == null || this.queryTerms.isEmpty()) {
      final Result result = new Result();
      result.setEmpty("No query terms.");
      return result;
    }

    try {
      final Result r = calculateClarity();

      LOG.debug("Calculating default clarity score for query '{}' "
              + "with {} document models took {}. {}", query,
          this.feedbackDocIds.size(), timeMeasure.stop().getTimeString(),
          r.getScore()
      );
      return r;
    } catch (final ProcessingException e) {
      timeMeasure.stop();
      final String msg = "Caught exception while calculating score.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
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
   * Calculate the clarity score. Calculation is based on a set of feedback
   * documents and a list of query terms.
   *
   * @return Result of the calculation
   * @throws ProcessingException Thrown if any of the threaded calculations
   * encountered an error
   */
  Result calculateClarity()
      throws ProcessingException {
    checkClosed();
    final Result result = new Result();
    result.setConf(this.conf);
    result.setFeedbackDocIds(this.feedbackDocIds);

    // short circuit, if no terms are left. The score will be zero.
    if (this.queryTerms.isEmpty()) {
      result.setEmpty("No query term matched in index. Result is 0.");
      return result;
    }

    // cache document models
    LOG.info("Caching {} document models.", this.feedbackDocIds.size());
    final TimeMeasure tm = new TimeMeasure().start();
    cacheDocumentModels();
    LOG.debug("Caching document models took {}.", tm.stop().getTimeString());

    // non existent (in collection) terms are now removed
    result.setQueryTerms(this.queryTerms);

    LOG.info("Calculating query language model using feedback vocabulary.");
    // now calculate the score using all feedback terms
    // collect from feedback documents
    final AtomicDouble score = new AtomicDouble(0d);
    new Processing().setSourceAndTarget(
        new TargetFuncCall<>(
            new IteratorSource<>(
                this.dataProv.getDocumentsTerms(this.feedbackDocIds)),
            new ScoreCalculatorTarget(score)
        )
    ).process();

    result.setScore(score.doubleValue());
    return result;
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
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
  enum DataKeys {

    /**
     * Stores the document model for a specific term in a {@link
     * DocumentModel}.
     */
    DOCMODEL((byte) 0);

    final SerializableByte id;

    DataKeys(byte itemId) {
      this.id = new SerializableByte(itemId);
    }

    SerializableByte id() {
      return this.id;
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
     * Ids of feedback documents used for calculation.
     */
    private Collection<Integer> feedbackDocIds;
    /**
     * Number of documents in the index matching the query.
     */
    private Integer numberOfMatchingDocuments = null;
    /**
     * Configuration that was used.
     */
    private DefaultClarityScoreConfiguration conf = null;
    /**
     * Configuration prefix.
     */
    private static final String CONF_PREFIX = IDENTIFIER + "-result";

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
      extends TargetFuncCall.TargetFunc<Map.Entry<ByteArray, Long>> {
    /**
     * Shared instance to store calculation results.
     */
    private final AtomicDouble target;

    /**
     * Initialize the calculation target.
     *
     * @param score Atomic value to add up the score
     */
    ScoreCalculatorTarget(final AtomicDouble score) {
      this.target = Objects.requireNonNull(score,
          "Calculation result target was null.");
    }

    /**
     * Calculate the score portion for a given term using already calculated
     * query models.
     */
    @Override
    public void call(final Map.Entry<ByteArray, Long> termEntry) {
      if (termEntry != null) {
        final double pq = getQueryModel(termEntry.getKey());
        if (pq <= 0d) {
          LOG.warn("Query model <= 0 ({}) for term '{}'.", pq,
              ByteArrayUtils.utf8ToString(termEntry.getKey()));
        } else {
          final double value = pq * MathUtils.log2(pq /
              DefaultClarityScore.this.metrics.collection()
                  .relTf(termEntry.getKey()));
          // add the value n-times the terms is in the feedback list
          this.target.addAndGet(termEntry.getValue() * value);
        }
      }
    }
  }

//  /**
//   * {@link Processing} {@link Target} for document model creation.
//   */
//  private final class DocumentModelCalculatorTarget
//      extends TargetFuncCall.TargetFunc<Integer> {
//    /**
//     * Calculate the document model.
//     *
//     * @param docId Current document-id
//     */
//    @Override
//    public void call(final Integer docId) {
//      if (docId != null) {
//        getDocumentModel(docId);
//      }
//    }
//  }
}
