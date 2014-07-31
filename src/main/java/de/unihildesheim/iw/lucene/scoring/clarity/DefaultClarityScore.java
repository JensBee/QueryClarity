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
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.scoring.ScoringBuilder;
import de.unihildesheim.iw.lucene.scoring.data.DefaultFeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultVocabularyProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.mapdb.DBMakerUtils;
import de.unihildesheim.iw.util.BigDecimalCache;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.IteratorSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

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
  @SuppressWarnings("PackageVisibleField")
  final Metrics metrics; // accessed from inner class
  /**
   * Caches values of document and query models. Those need to be normalized
   * before calculating the score.
   */
  // accessed from multiple threads
  // accessed from inner class
  @SuppressWarnings("PackageVisibleField")
  final Collection<Fun.Tuple2<BigDecimal, BigDecimal>> dataSet;
  /**
   * {@link IndexDataProvider} to use.
   */
  @SuppressWarnings("PackageVisibleField")
  final IndexDataProvider dataProv; // accessed by unit test
  /**
   * Reader to access Lucene index.
   */
  private final IndexReader idxReader;
  /**
   * Analyzer for parsing queries.
   */
  private final Analyzer analyzer;
  /**
   * On disk database for creating temporary caches.
   */
  private final DB cache;
  /**
   * In memory database for creating temporary caches.
   */
  private final DB memCache;
  /**
   * Caching a part of the query model calculation formula.
   */
  private final Map<Integer, BigDecimal> queryModelPartCache;
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
   * On disk storage of default document models that get used, if a term is not
   * in a specific document.
   */
  @SuppressWarnings("PackageVisibleField") // accessed from inner classes
      Map<ByteArray, BigDecimal> defaultDocModelStore;
  /**
   * List of query terms provided.
   */
  @SuppressWarnings("PackageVisibleField") // accessed from inner classes
      Set<ByteArray> queryTerms;
  /**
   * Disk based storage of document models. Containing only models for
   * document-term combinations.
   */
  @SuppressWarnings("PackageVisibleField") // accessed from inner classes
      Map<Fun.Tuple2<Integer, ByteArray>, BigDecimal> docModelStore;
  /**
   * Set of feedback documents to use for calculation.
   */
  private Set<Integer> feedbackDocIds;
  /**
   * Flag indicating, if this instance has been closed.
   */
  private volatile boolean isClosed;
  /**
   * Database to use.
   */
  private Persistence persist;
  /**
   * Flag indicating, if a unit-test is running. This should prevent overwriting
   * test-data with real values.
   */
  private boolean isTest;

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
    // localize some values for time critical calculations
    this.docLangModelWeight = BigDecimalCache.get(this.conf
        .getLangModelWeight());
    this.docLangModelWeight1Sub = BigDecimalCache.get(1d - this.conf
        .getLangModelWeight());
    this.conf.debugDump();

    this.dataProv = builder.getIndexDataProvider();
    this.idxReader = builder.getIndexReader();
    this.metrics = new Metrics(builder.getIndexDataProvider());
    this.analyzer = builder.getAnalyzer();

    if (builder.getVocabularyProvider() != null) {
      this.vocProvider = builder.getVocabularyProvider();
    } else {
      this.vocProvider = new DefaultVocabularyProvider();
    }

    if (builder.getFeedbackProvider() != null) {
      this.fbProvider = builder.getFeedbackProvider();
    } else {
      this.fbProvider = new DefaultFeedbackProvider();
    }

    this.memCache = DBMaker.newMemoryDirectDB()
        .transactionDisable()
        .make();
    this.cache = DBMakerUtils.newTempFileDB().make();
    this.dataSet = new ConcurrentLinkedQueue<>();
    this.queryModelPartCache = this.memCache
        .createTreeMap("qmCache")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
        .valueSerializer(Serializer.BASIC)
        .make();

    // initialize
    try {
      this.initCache(builder);
    } catch (DataProviderException e) {
      throw new Buildable.BuildException(
          "Error while initializing cache.", e);
    }

    if (this.docModelStore.isEmpty() || (
        this.persist.getDb().exists(DataKeys.HAS_PRECALC_MODELS.name())
            && !this.persist.getDb().getAtomicBoolean(DataKeys
            .HAS_PRECALC_MODELS.name()).get())) {
      try {
        createDocumentModels();
        this.persist.getDb().compact();
      } catch (final DataProviderException | ProcessingException e) {
        throw new Buildable.BuildException(
            "Error while pre-calculating document models.", e);
      }
    }
  }

  /**
   * Initializes a cache.
   *
   * @param builder Builder instance
   * @throws Buildable.BuildableException Thrown, if building the persistent
   * cache failed
   */
  private void initCache(final ScoringBuilder builder)
      throws Buildable.BuildableException, DataProviderException {
    final Persistence.Builder psb = builder.getCache();
    psb.dbMkr.compressionDisable()
        .noChecksum()
        .cacheSize(65536)
        .transactionDisable();

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
            .set(this.conf.getLangModelWeight().toString());
      } else {
        this.persist.getDb()
            .createAtomicString(Caches.LANGMODEL_WEIGHT.name(),
                this.conf.getLangModelWeight().toString());
      }
      persistence.clearMetaData();
      persistence.updateMetaData(this.dataProv.getDocumentFields(),
          this.dataProv.getStopwords());
    }

    this.docModelStore = this.persist.getDb()
        .createTreeMap(DataKeys.DOCMODEL.name())
        .keySerializer(new BTreeKeySerializer.Tuple2KeySerializer<>(
            null, Serializer.INTEGER, ByteArray.SERIALIZER))
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();
    this.defaultDocModelStore = this.persist.getDb()
        .createTreeMap(DataKeys.DOCMODEL_DEFAULT.name())
        .keySerializer(ByteArray.SERIALIZER_BTREE)
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();

    if (clearCache) {
      LOG.info("Clearing document model data cache.");
      clearStore();
    }
  }

  /**
   * Pre-calculate model values for all terms in the index.
   *
   * @throws ProcessingException Thrown on errors in the calculation target
   * @throws DataProviderException Thrown on errors accessing the Lucene index
   */
  private void createDocumentModels()
      throws ProcessingException, DataProviderException {
    final Processing p = new Processing();

    LOG.info("Cache init: "
        + "Calculating default document models for all collection terms.");
    p.setSourceAndTarget(new TargetFuncCall<>(
        new IteratorSource<>(this.dataProv.getTermsIterator()),
        new TargetFuncCall.TargetFunc<ByteArray>() {

          @Override
          public void call(final ByteArray term)
              throws Exception {
            if (term != null) {
              DefaultClarityScore.this.defaultDocModelStore.put(
                  term, getDefaultDocumentModel(term));
            }
          }
        }
    )).setSourceDataCount(this.dataProv.getUniqueTermsCount())
        .process((int) this.dataProv.getUniqueTermsCount());

    LOG.info("Cache init: "
        + "Calculating document models for all collection documents.");
    p.setSourceAndTarget(new TargetFuncCall<>(
        new IteratorSource<>(this.dataProv.getDocumentIds()),
        new TargetFuncCall.TargetFunc<Integer>() {
          @Override
          public void call(final Integer docId)
              throws Exception {
            if (docId != null) {
              final DocumentModel docModel = DefaultClarityScore.this.metrics
                  .getDocumentModel(docId);
              for (final ByteArray term : docModel.getTermFreqMap().keySet()) {
                DefaultClarityScore.this.docModelStore.put(Fun.t2(docId, term),
                    calcDocumentModel(docModel, term));
              }
            }
          }
        }
    )).setSourceDataCount(this.dataProv.getDocumentCount())
        .process((int) this.dataProv.getDocumentCount());

    if (this.persist.getDb().exists(DataKeys.HAS_PRECALC_MODELS.name())) {
      this.persist.getDb().getAtomicBoolean(DataKeys.HAS_PRECALC_MODELS.name())
          .getAndSet(true);
    } else {
      this.persist.getDb()
          .createAtomicBoolean(DataKeys.HAS_PRECALC_MODELS.name(),
              true);
    }
  }

  /**
   * Clears all cached and stored values.
   */
  private void clearStore() {
    // clear cache
    this.queryModelPartCache.clear();
    // clear store
    this.docModelStore.clear();
    this.defaultDocModelStore.clear();
  }

  /**
   * Calculate or get the default value, if the term is not contained in
   * document.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   * @throws NullPointerException Thrown, if term is not known
   */
  BigDecimal getDefaultDocumentModel(final ByteArray term)
      throws DataProviderException {
    return this.docLangModelWeight1Sub
        .multiply(this.metrics.collection().relTf(term));
  }

  /**
   * Calculate a single document model value.
   *
   * @param docModel Document data model
   * @param term Current term
   * @return Model value
   */
  private BigDecimal calcDocumentModel(final DocumentModel docModel,
      final ByteArray term)
      throws DataProviderException {
    if (this.defaultDocModelStore.containsKey(term)) {
      return this.docLangModelWeight
          .multiply(docModel.metrics().relTf(term))
          .add(this.defaultDocModelStore.get(term));
    }
    // term not in collection - unknown
    return getDefaultDocumentModel(term);
  }

  /**
   * Set values used for unit testing. Also triggers pre-calculation of model
   * values.
   *
   * @param testFbDocIds List of feedback documents to use
   * @param testQueryTerms List of query terms to use
   * @throws ProcessingException Thrown on errors in the calculation target
   * @throws DataProviderException Thrown on errors accessing the Lucene index
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  void testSetValues(final Set<Integer> testFbDocIds,
      final Map<ByteArray, Integer> testQueryTerms)
      throws ProcessingException, DataProviderException {
    this.feedbackDocIds = testFbDocIds;
    this.queryTerms = testQueryTerms.keySet();

    clearStore();
    createDocumentModels();
    cacheDocumentModels();
    this.isTest = true;
  }

  VocabularyProvider testGetVocabularyProvider() {
    return this.vocProvider;
  }

  /**
   * Calculate or get the query model for a given term. Calculation is based on
   * a set of feedback documents and a list of query terms.
   *
   * @param term Feedback document term
   * @return Model value
   */
  BigDecimal getQueryModel(final ByteArray term)
      throws DataProviderException {
    BigDecimal modelPart;
    BigDecimal model = BigDecimal.ZERO;
    BigDecimal value = null;
    Double valueDouble;
    // go through all documents
    for (final Integer docId : this.feedbackDocIds) {
      // get the static part for all query terms (pre-calculated)
      modelPart = this.queryModelPartCache.get(docId);
      if (modelPart == null) {
        throw new IllegalStateException(
            "Pre-calculated model part value not found.");
      }
//      if (modelPart == 0d) {
//        // won't add anything to the final model value
//        continue;
//      }

      // get the value for the current document & feedback term
      // if the term is in the document it must be available here, since we have
      // pre-calculated data
      value = this.docModelStore.get(Fun.t2(docId, term));
      if (value == null) {
        // term not in document - get default model
        value = this.defaultDocModelStore.get(term);
        if (value == null) {
          // default model not calculated, calculate ad-hoc
          value = getDefaultDocumentModel(term);
        }
      }

      // add values together
      model = model.add(modelPart.multiply(value));

      if (model.compareTo(BigDecimal.ZERO) == 0) {
        // value is too low
        return BigDecimal.ZERO;
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
      if (!this.cache.isClosed()) {
        LOG.info("Shutdown: closing cache");
        this.cache.close();
      }
      LOG.info("Shutdown: compacting & closing store");
      this.persist.getDb().compact();
      this.persist.closeDb();
      if (!this.memCache.isClosed()) {
        LOG.info("Shutdown: closing memory cache");
        this.memCache.close();
      }
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
      throws ClarityScoreCalculationException, DataProviderException {
    checkClosed();
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    if (!this.isTest) { // test sets value manually
      try {
        this.feedbackDocIds = this.fbProvider
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
    }

    if (this.feedbackDocIds.isEmpty()) {
      final Result result = new Result();
      result.setEmpty("No feedback documents.");
      return result;
    }

    if (!this.isTest) { // test sets value manually
      // get a normalized list of query terms
      // skips stopwords and removes unknown terms
      this.queryTerms = new HashSet<>(QueryUtils.tokenizeQuery(query,
          this.analyzer, this.metrics.collection()));
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
    } catch (final DataProviderException | ProcessingException e) {
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
      throws ProcessingException, DataProviderException {
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

    LOG.info("Calculating language model values using feedback vocabulary.");
    // now calculate the score using all feedback terms
    // collected from feedback documents
    new Processing().setSourceAndTarget(
        new TargetFuncCall<>(
            new IteratorSource<>(
                this.vocProvider
                    .indexDataProvider(this.dataProv)
                    .documentIds(this.feedbackDocIds)
                    .get()),
            new ScoreCalculatorTarget()
        )
    ).process();

    LOG.info("Calculating final score.");
    result.setScore(
        MathUtils.KlDivergence.calcBig(
            this.dataSet, MathUtils.KlDivergence.sumBigValues(this.dataSet)
        ).doubleValue());
    return result;
  }

  /**
   * Caches all values that can be pre-calculated and are used for scoring.
   *
   * @throws ProcessingException Thrown on concurrent calculation errors
   */
  private void cacheDocumentModels()
      throws ProcessingException {
    // empty caches
    LOG.info("Clearing temporary caches.");
    this.queryModelPartCache.clear();

    new Processing().setSourceAndTarget(new TargetFuncCall<>(
        new CollectionSource<>(this.feedbackDocIds),
        new TargetFuncCall.TargetFunc<Integer>() {

          @Override
          public void call(final Integer docId)
              throws Exception {
            if (docId == null) {
              return;
            }

            final DocumentModel docModel = DefaultClarityScore.this.metrics
                .getDocumentModel(docId);

            // calculate model values for all unique query terms
            for (final ByteArray term :
                DefaultClarityScore.this.queryTerms) {
              final Fun.Tuple2<Integer, ByteArray> mapKey = Fun.t2(docId,
                  term);
              if (!DefaultClarityScore.this
                  .docModelStore.containsKey(mapKey)) {
                if (docModel.contains(term)) {
                  DefaultClarityScore.this.docModelStore.put(
                      mapKey, calcDocumentModel(docModel, term));
                }
              }
            }

            // calculate the static portion of the query model for all query
            // terms. Calculate for all query terms (try cache first)
            BigDecimal modelPart = BigDecimal.ONE;
            Double valueDouble;
            BigDecimal value;
            for (final ByteArray qTerm :
                DefaultClarityScore.this.queryTerms) {
              value = DefaultClarityScore.this
                  .docModelStore.get(Fun.t2(docId, qTerm));

              if (value == null) {
                // term not in document
                value = DefaultClarityScore.this
                    .defaultDocModelStore.get(qTerm);
                if (value == null) {
                  // term not known
                  value = getDefaultDocumentModel(qTerm);
                  if (value.compareTo(BigDecimal.ZERO) <= 0) {
                    LOG.debug("qmPart<={} ddm-value={}",
                        modelPart.doubleValue(),
                        value.doubleValue());
                  }
                }
              }

              modelPart = modelPart.multiply(value);

              if (modelPart.compareTo(BigDecimal.ZERO) == 0) {
                break;
              }
            }
            DefaultClarityScore.this
                .queryModelPartCache.put(docId, modelPart);
          }
        }
    )).process(this.feedbackDocIds.size());
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
  @SuppressWarnings({"PublicInnerClass", "PackageVisibleInnerClass"})
  enum DataKeys { // needs access for unit testing

    /**
     * Stores the document model for a specific term in a {@link
     * DocumentModel}.
     */
    DOCMODEL,
    /**
     * Stores the default document model for a specific term.
     */
    DOCMODEL_DEFAULT,
    /**
     * Flag indicating, if pre-calculated model values are available for all
     * terms in the index
     */
    HAS_PRECALC_MODELS
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
      xml.getItems().put("feedbackDocuments",
          Integer.toString(this.feedbackDocIds.size()));

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

  /**
   * {@link Processing} {@link Target} calculating a portion of the final
   * clarity score. The current term is passed in from a {@link Source}.
   */
  private final class ScoreCalculatorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {
    /**
     * Calculate the score portion for a given term using already calculated
     * query models.
     */
    @Override
    public void call(final ByteArray term)
        throws DataProviderException {
      if (term != null) {
        DefaultClarityScore.this.dataSet.add(
            Fun.t2(
                getQueryModel(term),
                DefaultClarityScore.this.metrics.collection()
                    .relTf(term)));
      }
    }
  }
}
