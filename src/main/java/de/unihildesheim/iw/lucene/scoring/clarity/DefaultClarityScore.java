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
import org.mapdb.Atomic;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
  // accessed from inner class
  @SuppressWarnings("PackageVisibleField")
  final Metrics metrics;
  /**
   * Caches values of document and query models. Those need to be normalized
   * before calculating the score.
   */
  // accessed from multiple threads
  // accessed from inner class
  @SuppressWarnings("PackageVisibleField")
  Map<Long, Fun.Tuple2<BigDecimal, BigDecimal>> c_dataSet;
  /**
   * Counter for entries in {@link #c_dataSet}. Gets used as map key.
   */
  private AtomicLong dataSetCounter;
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
   * Caching a part of the query model calculation formula.
   */
  // accessed from inner classes
  @SuppressWarnings("PackageVisibleField")
  final Map<Integer, BigDecimal> c_queryModelParts;
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
  // accessed from inner classes
  @SuppressWarnings("PackageVisibleField")
  Map<Long, BigDecimal> s_defaultDocModels;
  /**
   * Mapping of term to an unique id. Used to store term related data.
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private BTreeMap<byte[], Long> s_termIds;
  /**
   * Counter for {@link #s_termIds} term ids.
   */
  private Atomic.Long termIdIdx;
  /**
   * List of query terms provided.
   */
  // accessed from inner classes
  @SuppressWarnings("PackageVisibleField")
  Collection<ByteArray> queryTerms;
  /**
   * Disk based storage of document models. Containing only models for
   * document-term combinations.
   */
  // accessed from inner classes
  @SuppressWarnings("PackageVisibleField")
  Map<Fun.Tuple2<Integer, Long>, BigDecimal> s_docModels;
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
   * Flag indicating, if all models needed for calculations have been
   * pre-calculated already. This may save some calculation time.
   */
  private final boolean hasAllPrecalculatedModels;
  /**
   * Counter values for inserts to persistent storage maps to shedule compaction
   * runs.
   */
  private final DBInsertCounter dbInsertTracker;

  /**
   * Counter for inserts to persistent storage maps to shedule compaction runs.
   */
  private static final class DBInsertCounter {
    /**
     * After how many inserts to a store a compaction should be triggered.
     */
    private static final int COMPACT_AFTER = 1000;

    /**
     * Targets for increasing counters.
     */
    private enum Target {
      MODELS, MODELS_DEFAULT
    }

    /**
     * Counter for inserts to {@link #s_defaultDocModels}.
     */
    private final AtomicInteger defaultDocModels = new AtomicInteger(0);
    /**
     * Counter for inserts to {@link #s_docModels}.
     */
    private final AtomicInteger docModels = new AtomicInteger(0);
    /**
     * Target Database.
     */
    private final DB db;

    DBInsertCounter(final DB newDb) {
      this.db = newDb;
    }

    void addTo(final Target target) {
      if (target == Target.MODELS) {
        if (this.docModels.get() >= COMPACT_AFTER) {
          runCommit();
        } else {
          this.docModels.incrementAndGet();
        }
      } else if (target == Target.MODELS_DEFAULT) {
        if (this.defaultDocModels.get() >= COMPACT_AFTER) {
          runCommit();
        } else {
          this.defaultDocModels.incrementAndGet();
        }
      }
    }

    private void runCommit() {
      this.db.commit();
      this.docModels.getAndSet(0);
      this.defaultDocModels.getAndSet(0);
    }
  }

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

    this.cache = DBMakerUtils.newTempFileDB().make();
    this.c_queryModelParts = this.cache
        .createTreeMap("qmCache")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
        .valueSerializer(Serializer.BASIC)
        .make();
    this.c_dataSet = this.cache
        .createTreeMap("dataSet")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
        .valueSerializer(Serializer.BASIC)
        .make();

    // initialize
    try {
      this.initCache(builder);
    } catch (final DataProviderException e) {
      throw new Buildable.BuildException(
          "Error while initializing cache.", e);
    }

    this.dbInsertTracker = new DBInsertCounter(this.persist.getDb());

    this.hasAllPrecalculatedModels =
        this.persist.getDb().exists(DataKeys.HAS_PRECALC_MODELS.name())
            && this.persist.getDb().getAtomicBoolean(
            DataKeys.HAS_PRECALC_MODELS.name()).get();

    LOG.debug("hasAllPrecalculatedModels: {}", this.hasAllPrecalculatedModels);

    if (builder.shouldPrecalculate() && !this.hasAllPrecalculatedModels) {
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
   * @throws DataProviderException Thrown on low-level errors
   */
  private void initCache(final ScoringBuilder builder)
      throws Buildable.BuildableException, DataProviderException {
    final Persistence.Builder psb = builder.getCache();
    psb.dbMkr
        .noChecksum()
        .transactionEnable()
        .cacheSize(65536);

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

    this.s_docModels = this.persist.getDb()
        .createTreeMap(DataKeys.DOCMODEL.name())
        .keySerializer(new BTreeKeySerializer.Tuple2KeySerializer<>(
            null, Serializer.INTEGER, Serializer.LONG))
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();
    this.s_defaultDocModels = this.persist.getDb()
        .createTreeMap(DataKeys.DOCMODEL_DEFAULT.name())
        .keySerializer(BTreeKeySerializer.BASIC)
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();
    this.s_termIds = this.persist.getDb()
        .createTreeMap(DataKeys.TERM_IDS.name())
        .keySerializerWrap(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.LONG)
        .comparator(Fun.BYTE_ARRAY_COMPARATOR)
        .counterEnable()
        .makeOrGet();
    if (this.persist.getDb().exists(DataKeys.TERM_IDS_IDX.name())) {
      this.termIdIdx = this.persist.getDb()
          .getAtomicLong(DataKeys.TERM_IDS_IDX.name());
    } else {
      if (!this.s_termIds.isEmpty()) {
        throw new IllegalStateException("TermIndex corruption.");
      }
      this.termIdIdx = this.persist.getDb()
          .createAtomicLong(DataKeys.TERM_IDS_IDX.name(), 0);
    }

    if (clearCache) {
      LOG.info("Clearing document model data cache.");
      clearStore();
    }
  }

  /**
   * Get the id for a known term. Creates a new term-id, if the term was not
   * known. Terms are cached in a global map. Only references to those terms
   * should be used to minimize storage size.
   *
   * @param term Term whose id to get.
   * @return Term id
   */
  @SuppressWarnings("SynchronizeOnNonFinalField")
  private Long getTermId(final ByteArray term) {
    Long termId = this.s_termIds.get(term.bytes);
    if (termId == null) {
      termId = this.termIdIdx.incrementAndGet();
      final Long oldId = this.s_termIds.putIfAbsent(term.bytes, termId);
      if (oldId != null) {
        termId = oldId;
      }
    }
    return termId;
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
              DefaultClarityScore.this.s_defaultDocModels.put(
                  getTermId(term), getDefaultDocumentModel(term));
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
                DefaultClarityScore.this
                    .s_docModels.put(Fun.t2(docId, getTermId(term)),
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
    LOG.info("Clearing cached data");
    // clear cache
    this.c_queryModelParts.clear();
    // clear store
    this.s_docModels.clear();
    this.s_defaultDocModels.clear();
    this.s_termIds.clear();
  }

  /**
   * Calculate or get the default value, if the term is not contained in
   * document.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   * @throws NullPointerException Thrown, if term is not known
   * @throws DataProviderException Thrown on low-level errors
   */
  BigDecimal getDefaultDocumentModel(final ByteArray term)
      throws DataProviderException {
    return BigDecimalCache.getMul(this.metrics.collection().relTf(term),
        this.docLangModelWeight1Sub);
  }

  /**
   * Calculate a single document model value.
   *
   * @param docModel Document data model
   * @param term Current term
   * @return Model value
   * @throws DataProviderException Thrown on low-level errors
   */
  private BigDecimal calcDocumentModel(final DocumentModel
      docModel, final ByteArray term)
      throws DataProviderException {
    final BigDecimal value;
    if (this.metrics.collection().tf(term) > 0L) {
      final long termId = getTermId(term);
      if (!this.s_defaultDocModels.containsKey(termId)) {
        this.s_defaultDocModels.put(
            termId, getDefaultDocumentModel(term));
      }
      if (docModel != null && docModel.contains(term)) {
        value = BigDecimalCache
            .getMul(docModel.metrics().relTf(term), this.docLangModelWeight)
            .add(this.s_defaultDocModels.get(termId));
      } else {
        value = this.s_defaultDocModels.get(termId);
      }
    } else {
      // term not in collection - unknown
      value = getDefaultDocumentModel(term);
    }
    return value;
  }

  /**
   * Get the {@link VocabularyProvider} in use by this instance. Used for
   * testing.
   *
   * @return Provider currently in use
   */
  VocabularyProvider testGetVocabularyProvider() {
    return this.vocProvider;
  }

  /**
   * Get the document model value for a document specified by it's id.
   *
   * @param docId Document id
   * @param model Already retrieved data model of the same document. If null, a
   * new data model will be retrieved, if needed.
   * @param term Term to calculate for
   * @return Calculated model value
   * @throws DataProviderException Thrown on low-level errors
   */
  BigDecimal getDocumentModel(final int docId, final DocumentModel model,
      final ByteArray term)
      throws DataProviderException {
    final long termId = getTermId(term);
    final Fun.Tuple2<Integer, Long> mapKey = Fun.t2(docId, termId);

    BigDecimal value = this.s_docModels.get(mapKey);
    if (value == null) {
      // term not in document - get default model
      // we tried to get the value for the current document & feedback term
      // if the term is in the document it must be available here,
      // since we have
      value = this.s_defaultDocModels.get(termId);
      if (value == null) {
        // not cached (non-index term?) - calculate it
        value = getDefaultDocumentModel(term);
      }

//        // check, if term is in document
//        if ((model != null && model.contains(term))
//            || this.dataProv.documentContains(docId, term)) {
//          // in document, so store calculated value
//          if (model == null) {
//            value = calcDocumentModel(
//                this.metrics.getDocumentModel(docId), term);
//          } else {
//            value = calcDocumentModel(model, term);
//          }
//          this.s_docModels.put(mapKey, value);
//        } else {
//          // not in document, check if value is cached
//          value = this.s_defaultDocModels.get(termId);
//          if (value == null) {
//            // not cached - calculate it
//            value = getDefaultDocumentModel(term);
//            if (this.metrics.collection().tf(term) > 0L) {
//              // store, if collection term
//              this.s_defaultDocModels.put(termId, value);
//            }
//          }
//        }
    }
    return value;
  }

  /**
   * Calculate or get the query model for a given term. Calculation is based on
   * a set of feedback documents and a list of query terms.
   *
   * @param term Feedback document term
   * @return Model value
   * @throws DataProviderException Thrown on low-level errors
   */
  BigDecimal getQueryModel(final ByteArray term)
      throws DataProviderException {
    BigDecimal modelPart;
    BigDecimal model = BigDecimal.ZERO;

    // go through all documents
    for (final Integer docId : this.feedbackDocIds) {
      // get the static part for all query terms (pre-calculated)
      modelPart = this.c_queryModelParts.get(docId);
      if (modelPart == null) {
        throw new IllegalStateException(
            "Pre-calculated model part value not found.");
      }

      // add values together
      model = model.add(modelPart.multiply(
          getDocumentModel(docId, null, term)));
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
      LOG.info("Shutdown: compacting & closing store");
      this.persist.getDb().commit();
      this.persist.getDb().compact();
      this.persist.closeDb();
      if (!this.cache.isClosed()) {
        LOG.info("Shutdown: closing cache");
        this.cache.close();
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

    if (this.feedbackDocIds.isEmpty()) {
      final Result result = new Result();
      result.setEmpty("No feedback documents.");
      return result;
    }

    // get a normalized list of query terms
    // skips stopwords and removes unknown terms
//    this.queryTerms = new HashSet<>(QueryUtils.tokenizeQuery(query,
//        this.analyzer, this.metrics.collection()));
    this.queryTerms = QueryUtils.tokenizeQuery(query,
        this.analyzer, this.metrics.collection());

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
   * @throws DataProviderException Thrown on low-level errors
   * @throws ClarityScoreCalculationException Thrown on model calculation
   * errors
   */
  Result calculateClarity()
      throws ProcessingException, DataProviderException,
             ClarityScoreCalculationException {
    checkClosed();
    final Result result = new Result();
    result.setConf(this.conf);
    result.setFeedbackDocIds(this.feedbackDocIds);

    // short circuit, if no terms are left. The score will be zero.
    if (this.queryTerms.isEmpty()) {
      result.setEmpty("No query term matched in index. Result is 0.");
      return result;
    }

    // empty caches
    LOG.debug("Clearing temporary caches.");
    this.c_queryModelParts.clear();

    // cache document models
    LOG.info("Caching {} document models.", this.feedbackDocIds.size());
    final TimeMeasure tm = new TimeMeasure().start();
    cacheDocumentModels(this.queryTerms, true);
    LOG.debug("Caching document models took {}.", tm.stop().getTimeString());

    // non existent (in collection) terms are now removed
    result.setQueryTerms(this.queryTerms);

    LOG.info("Collecting feedback vocabulary.");
    final DB fbTermsDb = DBMakerUtils.newTempFileDB().make();
    final Set<ByteArray> fbTerms = fbTermsDb.createTreeSet("fbTerms")
        .counterEnable()
        .serializer(ByteArray.SERIALIZER_BTREE)
        .make();
    final Iterator<ByteArray> fbTermsIt;
    fbTermsIt = this.vocProvider
        .indexDataProvider(this.dataProv)
        .documentIds(this.feedbackDocIds)
        .get();

    while (fbTermsIt.hasNext()) {
      final ByteArray fbTerm = fbTermsIt.next();
      if (fbTerm == null) {
        throw new IllegalStateException("Null term!");
      }
      fbTerms.add(fbTerm);
    }
    LOG.info("{} terms in feedback vocabulary.", fbTerms.size());

    LOG.info("Pre-calculating {} language model values " +
            "from feedback vocabulary.",
        fbTerms.size() * this.feedbackDocIds.size());
    try {
      if (!this.hasAllPrecalculatedModels) {
        cacheDocumentModels(fbTerms, false);
      }
    } catch (final ProcessingException e) {
      final String msg = "Caught exception while pre-calculating models.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    // clear any leftover values
    this.c_dataSet.clear();
    this.dataSetCounter = new AtomicLong(0L);

    LOG.info("Calculating {} language models using feedback vocabulary.",
        fbTerms.size());
    // now calculate the score using all feedback terms
    // collected from feedback documents
    new Processing().setSourceAndTarget(
        new TargetFuncCall<>(
            new CollectionSource<>(fbTerms),
            new ScoreCalculatorTarget()
        )
    ).process();

    LOG.info("Calculating final score.");
    result.setScore(
        MathUtils.KlDivergence.calc(
            this.c_dataSet.values(),
            MathUtils.KlDivergence.sumValues(this.c_dataSet.values())
        ).doubleValue());

    fbTermsDb.close(); // delete
    return result;
  }

  /**
   * Caches all values that can be pre-calculated and are used for scoring.
   *
   * @param terms Terms to use for caching
   * @param calcQPortion If true, a static portion of the query model will also
   * be calculated. This should only be true, if the passed in terms are query
   * terms. Else the calculation results may be wrong.
   * @throws ProcessingException Thrown on concurrent calculation errors
   */
  private void cacheDocumentModels(final Collection<ByteArray> terms,
      final boolean calcQPortion)
      throws ProcessingException {
    final Processing p = new Processing();
    // count adds to s_defaultDocModels and s_docModels for debugging
    final AtomicLong[] dbg_itemCount = {new AtomicLong(0L), new AtomicLong(0L)};

    LOG.info("Calculating default document models from vocabulary.");
    p.setSourceAndTarget(new TargetFuncCall<>(
        new CollectionSource<>(terms),
        new TargetFuncCall.TargetFunc<ByteArray>() {

          @Override
          public void call(final ByteArray term)
              throws Exception {
            if (term != null && DefaultClarityScore.this
                .metrics.collection().tf(term) <= 0L) {
              final long termId = getTermId(term);
              if (!DefaultClarityScore.this
                  .s_defaultDocModels.containsKey(termId)) {
                // not cached - calculate and store it, if collection term
                DefaultClarityScore.this.s_defaultDocModels.put(
                    termId, getDefaultDocumentModel(term));
                if (LOG.isDebugEnabled()) {
                  dbg_itemCount[0].incrementAndGet();
                }
                DefaultClarityScore.this.dbInsertTracker.addTo(
                    DBInsertCounter.Target.MODELS_DEFAULT);
              }
            }
          }
        })).process(terms.size());

    if (calcQPortion) {
      LOG.info("Calculating document models and " +
          "static query model values for feedback documents.");
    } else {
      LOG.info("Calculating document models for feedback documents.");
    }

    p.setSourceAndTarget(new TargetFuncCall<>(
        new CollectionSource<>(this.feedbackDocIds),
        new TargetFuncCall.TargetFunc<Integer>() {

          @SuppressWarnings("ReuseOfLocalVariable")
          @Override
          public void call(final Integer docId)
              throws Exception {
            if (docId == null) {
              return;
            }

            // data model of the current document
            final DocumentModel docModel = DefaultClarityScore.this
                .metrics.getDocumentModel(docId);

            // choose minimum number of terms to check
            if (terms.size() > docModel.getTermFreqMap().size()) {
              for (final ByteArray docTerm :
                  docModel.getTermFreqMap().keySet()) {
                final Long termId = getTermId(docTerm);
                final Fun.Tuple2<Integer, Long> mapKey =
                    Fun.t2(docId, termId);
                if (terms.contains(docTerm)
                    && !DefaultClarityScore.this
                    .s_docModels.containsKey(mapKey)) {
                  DefaultClarityScore.this.s_docModels.put(
                      mapKey, calcDocumentModel(docModel, docTerm));
                  if (LOG.isDebugEnabled()) {
                    dbg_itemCount[1].incrementAndGet();
                  }
                  DefaultClarityScore.this.dbInsertTracker.addTo(
                      DBInsertCounter.Target.MODELS);
                }
              }
            } else {
              for (final ByteArray term : terms) {
                if (DefaultClarityScore.this
                    .metrics.collection().tf(term) > 0L) {
                  final Long termId = getTermId(term);
                  final Fun.Tuple2<Integer, Long> mapKey =
                      Fun.t2(docId, termId);
                  if (docModel.contains(term)
                      && !DefaultClarityScore.this
                      .s_docModels.containsKey(mapKey)) {
                    DefaultClarityScore.this.s_docModels.put(
                        mapKey, calcDocumentModel(docModel, term));
                    if (LOG.isDebugEnabled()) {
                      dbg_itemCount[1].incrementAndGet();
                    }
                    DefaultClarityScore.this.dbInsertTracker.addTo(
                        DBInsertCounter.Target.MODELS);
                  }
                }
              }
            }

            // calculate the static portion of the query model for all (query)
            // terms
            if (calcQPortion) {
              BigDecimal modelPart = BigDecimal.ONE;
              for (final ByteArray term : terms) {
                modelPart = modelPart.multiply(
                    getDocumentModel(docModel.id, docModel, term));
              }
              DefaultClarityScore.this
                  .c_queryModelParts.put(docId, modelPart);
            }
          }
        }
    )).process(this.feedbackDocIds.size());

    if (LOG.isDebugEnabled()
        && (dbg_itemCount[0].get() > 0L || dbg_itemCount[1].get() > 0L)) {
      LOG.debug("New entries: dmDefault={} dm={}",
          dbg_itemCount[0].get(), dbg_itemCount[1].get());
    }
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
     * terms in the index.
     */
    HAS_PRECALC_MODELS,
    TERM_IDS_IDX, DATASET, /**
     * Store ids for terms to not store terms all over again.
     */
    TERM_IDS
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
          ScoringResultXml.Keys.FEEDBACK_DOCUMENTS.toString(),
          Integer.toString(this.feedbackDocIds.size()));

      // feedback documents
      if (GlobalConfiguration.conf()
          .getAndAddBoolean(CONF_PREFIX + "ListFeedbackDocuments",
              Boolean.TRUE)) {
        final List<Tuple.Tuple2<String, String>> fbDocsList = new ArrayList<>
            (this.feedbackDocIds.size());
        for (final Integer docId : this.feedbackDocIds) {
          fbDocsList.add(Tuple.tuple2(
              ScoringResultXml.Keys.FEEDBACK_DOCUMENT_KEY.toString(),
              docId.toString()));
        }
        xml.getLists().put(
            ScoringResultXml.Keys.FEEDBACK_DOCUMENTS.toString(), fbDocsList);
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
        DefaultClarityScore.this.c_dataSet.put(
            DefaultClarityScore.this.dataSetCounter.incrementAndGet(),
            Fun.t2(getQueryModel(term), DefaultClarityScore.this
                .dataProv.getRelativeTermFrequency(term)));
      }
    }
  }
}
