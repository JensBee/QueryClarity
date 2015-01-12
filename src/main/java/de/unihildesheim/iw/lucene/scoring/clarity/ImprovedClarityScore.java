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
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.IteratorSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
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
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
   * Default math context for model calculations.
   */
  private static final MathContext MATH_CONTEXT = MathContext.DECIMAL128;
  /**
   * Provider for general index metrics.
   */
  @SuppressWarnings("PackageVisibleField")
  final Metrics metrics; // accessed from inner class
  /**
   * {@link IndexDataProvider} to use.
   */
  // accessed by unit test
  @SuppressWarnings("PackageVisibleField")
  final IndexDataProvider dataProv;
  /**
   * On disk caching the values of default document models that get used, if a
   * term is not in a specific document.
   */
  // accessed from inner classes
  @SuppressWarnings("PackageVisibleField")
  final BTreeMap<
      Fun.Tuple2<Integer, Long>, BigDecimal> c_tempDocModels;
  /**
   * Reader to access the Lucene index.
   */
  private final IndexReader idxReader;
  /**
   * Lucene query analyzer.
   */
  private final Analyzer analyzer;
  /**
   * On disk database for creating temporary caches.
   */
  private final DB cache;
  /**
   * Caching a part of the query model calculation formula.
   */
  private final Map<Integer, BigDecimal> c_queryModelParts;
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
   * Flag indicating, if all models needed for calculations have been
   * pre-calculated already. This may save some calculation time.
   */
  private final boolean hasAllPrecalculatedModels;
  /**
   * Counter values for inserts to persistent storage maps to shedule compaction
   * runs.
   */
  private final DBInsertCounter dbInsertTracker;
  private final Calculation calculation;
  /**
   * Caches values of document and query models. Those need to be normalized
   * before calculating the score.
   */
  // accessed from multiple threads
  // accessed from inner class
  @SuppressWarnings("PackageVisibleField")
  Map<Long, Fun.Tuple2<BigDecimal, BigDecimal>> c_dataSet;
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
  BTreeMap<Fun.Tuple2<Integer, Long>, BigDecimal> s_docModels;
  /**
   * Counter for entries in {@link #c_dataSet}. Gets used as map key.
   */
  private AtomicLong dataSetCounter;
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
   * Static portion of a document model (lower fraction part).
   */
  private BTreeMap<Integer, BigDecimal> s_docModelStatic;
  /**
   * Set of feedback documents to use for calculation.
   */
  private Set<Integer> feedbackDocIds;
  /**
   * Database instance.
   */
  private Persistence persist;
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
  private ImprovedClarityScore(final Builder builder)
      throws Buildable.BuildableException {
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    this.dataProv = builder.getIndexDataProvider();
    this.idxReader = builder.getIndexReader();
    this.metrics = new Metrics(builder.getIndexDataProvider());

    this.conf = builder.getConfiguration();

    // check config
    try {
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
    } catch (final DataProviderException e) {
      throw new Buildable.BuildException("Error while initializing.", e);
    }
    this.conf.debugDump();

    // localize some values for time critical calculations
    this.calculation = new Calculation(
        BigDecimalCache.get(
            this.conf.getDocumentModelSmoothingParameter()),
        BigDecimalCache.get(this.conf.getDocumentModelParamBeta()),
        BigDecimalCache.get(this.conf.getDocumentModelParamLambda())
    );

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
    this.c_dataSet = this.cache
        .createTreeMap("dataSet")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
        .valueSerializer(Serializer.BASIC)
        .make();
    this.c_queryModelParts = this.cache
        .createTreeMap("qmCache")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
        .valueSerializer(Serializer.BASIC)
        .make();
    this.c_tempDocModels = this.cache
        .createTreeMap("ddmCache")
        .keySerializer(new BTreeKeySerializer.Tuple2KeySerializer<>(
            null, Serializer.INTEGER, Serializer.LONG))
        .valueSerializer(Serializer.BASIC)
        .make();

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
   * Initializes a cache using the {@link Builder}.
   *
   * @param builder Builder instance
   * @throws Buildable.BuildableException Thrown, if building the cache instance
   * failed
   * @throws DataProviderException Thrown on low-level errors
   */
  private void initCache(final ScoringBuilder builder)
      throws Buildable.BuildableException, DataProviderException {
    final Persistence.Builder psb = builder.getCache();
    psb.dbMkr
        .transactionEnable()
        .noChecksum();

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

    this.s_docModels = this.persist.getDb()
        .createTreeMap(DataKeys.DOCMODEL.name())
        .keySerializer(new BTreeKeySerializer.Tuple2KeySerializer<>(
            null, Serializer.INTEGER, Serializer.LONG))
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();
    this.s_docModelStatic = this.persist.getDb()
        .createTreeMap(DataKeys.DOCMODEL_STATIC.name())
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();
    this.s_termIds = this.persist.getDb()
        .createTreeMap(DataKeys.TERM_IDS.name())
        .keySerializerWrap(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.LONG)
        .comparator(Fun.BYTE_ARRAY_COMPARATOR)
        .makeOrGet();
    if (this.persist.getDb().exists(DataKeys.TERM_IDS_IDX.name())) {
      this.termIdIdx = this.persist.getDb()
          .getAtomicLong(DataKeys.TERM_IDS_IDX.name());
    } else {
      if (!this.s_termIds.isEmpty()) {
        throw new IllegalStateException("TermIndex corruption.");
      }
      this.termIdIdx = this.persist.getDb()
          .createAtomicLong(DataKeys.TERM_IDS_IDX.name(), 0L);
    }

    if (rebuild) {
      LOG.warn("Cache flagged as being invalid due to changes in parameters. " +
          "Clearing cache.");
      this.persist.getDb().delete(Caches.LAMBDA.name());
      this.persist.getDb().delete(Caches.BETA.name());
      clearStore();
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
  }

  /**
   * Pre-calculate model values for all terms in the index.
   *
   * @throws ProcessingException Thrown, if concurrent processing of documents
   * has failed
   * @throws DataProviderException Thrown on low-level errors
   */
  private void createDocumentModels()
      throws ProcessingException, DataProviderException {
    final Processing p = new Processing();
    LOG.info("Cache init: "
        + "Calculating default document models for all collection documents.");
    p.setSourceAndTarget(new TargetFuncCall<>(
        new IteratorSource<>(this.dataProv.getDocumentIds()),
        new TargetFuncCall.TargetFunc<Integer>() {

          @Override
          public void call(final Integer docId)
              throws Exception {
            if (docId == null) {
              return;
            }
            final DocumentModel docModel = ImprovedClarityScore.this.metrics
                .getDocumentModel(docId);

            // calculate model values for all document terms
            for (final ByteArray term : docModel.getTermFreqMap().keySet()) {
              // calculate also the default value,
              // if term is not in document
              ImprovedClarityScore.this
                  .s_docModels.put(Fun.t2(docId, getTermId(term)),
                  ImprovedClarityScore.this.calculation.calcDocumentModel(
                      docId, docModel.metrics().tf(term), term));
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
    this.c_queryModelParts.clear();
    this.c_dataSet.clear();
    this.c_tempDocModels.clear();
    // clear store
    this.s_docModels.clear();
    this.s_docModelStatic.clear();
    this.s_termIds.clear();
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
      this.persist.getDb().commit();
      this.persist.getDb().compact();
      this.persist.closeDb();
      this.isClosed = true;
    }
  }

  /**
   * Calculate the query model.
   *
   * @param term Feedback term
   * @return Query model for the current term and set of feedback documents
   * @throws DataProviderException Thrown on low-level errors
   */
  BigDecimal getQueryModel(final ByteArray term) {
    BigDecimal modelPart;
    BigDecimal model = BigDecimal.ZERO;
    final Long termId = getTermId(term);

    for (final Integer docId : this.feedbackDocIds) {
      // get the static part for all query terms (pre-calculated)
      modelPart = this.c_queryModelParts.get(docId);

      final Fun.Tuple2<Integer, Long> mapKey = Fun.t2(docId, termId);
      BigDecimal docModelValue = this.s_docModels.get(mapKey);
      if (docModelValue == null) {
        // term not in document - get default model
        docModelValue = this.c_tempDocModels.get(mapKey);

        // still null, term not in document and no pre-cached value available
        // something went wrong!
        if (docModelValue == null) {
          throw new IllegalStateException("Pre-calculated value missing " +
              "doc=" + docId + " term=" + termId + ".");
        }
      }

      // get the value for the current document & feedback term
      // add values together
      model = model.add(modelPart.multiply(docModelValue));
    }
    return model;
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
   * Calculates the improved clarity score for a given query.
   *
   * @param query Query to calculate for
   * @return Clarity score result object
   */
  @Override
  public Result calculateClarity(final String query)
      throws ClarityScoreCalculationException, DataProviderException {
    checkClosed();
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // empty caches
    LOG.debug("Clearing temporary caches.");
    this.c_queryModelParts.clear();

    // result object
    final Result result = new Result(this.getClass());

    // get a normalized unique list of query terms
    // skips stopwords and removes unknown terms
    this.queryTerms = QueryUtils.tokenizeQuery(query,
        this.analyzer, this.metrics.collection());

    // check query term extraction result
    if (this.queryTerms.isEmpty()) {
      result.setEmpty("No query terms.");
      return result;
    }

    // save base data to result object
    result.setConf(this.conf);
    result.setQueryTerms(this.queryTerms);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    try {
      this.feedbackDocIds = this.fbProvider
          .indexReader(this.idxReader)
          .analyzer(this.analyzer)
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

    if (this.feedbackDocIds.isEmpty()) {
      result.setEmpty("No feedback documents.");
      return result;
    } else if (this.feedbackDocIds.size() < this.conf
        .getMinFeedbackDocumentsCount()) {
      result.setEmpty("Not enough feedback documents. " +
          this.conf.getMinFeedbackDocumentsCount() +
          " requested, " + this.feedbackDocIds.size() + " retrieved.");
      return result;
    }
    result.setFeedbackDocIds(this.feedbackDocIds);

    if (LOG.isDebugEnabled()) {
      final Set<ByteArray> matchingQTerms = new HashSet<>(
          this.queryTerms.size());
      for (final ByteArray qTerm : this.queryTerms) {
        boolean match = true;
        for (final Integer docId : this.feedbackDocIds) {
          if (this.metrics.getDocumentModel(docId).tf(qTerm) == 0) {
            match = false;
            break; // skip term, not a 'all documents match'
          }
        }
        if (match) {
          matchingQTerms.add(qTerm);
        }
      }

      final Set<String> matchingQTermsStr = new HashSet<>(matchingQTerms
          .size());
      for (final ByteArray ba : matchingQTerms) {
        matchingQTermsStr.add(ByteArrayUtils.utf8ToString(ba));
      }
      LOG.debug("{} of {} query terms matching all documents. q={} t={}",
          matchingQTerms.size(), this.queryTerms.size(), query,
          matchingQTermsStr);
    }

    // get document frequency threshold - allowed terms must be in bounds
    final BigDecimal minFreq = BigDecimal.valueOf(this.conf
        .getMinFeedbackTermSelectionThreshold());
    final BigDecimal maxFreq = BigDecimal.valueOf(this.conf
        .getMaxFeedbackTermSelectionThreshold());
    LOG.debug("Feedback term Document frequency threshold: {}%-{}%",
        minFreq.doubleValue() * 100d, maxFreq.doubleValue() * 100d);

    // cache document models
    LOG.info("Caching {} document models.", this.feedbackDocIds.size());
    final TimeMeasure tm = new TimeMeasure().start();
    try {
//      cacheDocumentModels(this.queryTerms, true);
      this.calculation.cacheDocModels(this.queryTerms, true);
    } catch (final ProcessingException e) {
      final String msg = "Caught exception while caching models.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }
    LOG.debug("Caching document models took {}.", tm.stop().getTimeString());

    // do the final calculation for all feedback terms
    if (LOG.isTraceEnabled()) {
      LOG.trace("Using {} feedback documents for calculation. {}",
          this.feedbackDocIds.size(), this.feedbackDocIds);
    } else {
      LOG.debug("Using {} feedback documents for calculation.",
          this.feedbackDocIds.size());
    }

    LOG.info("Collecting feedback vocabulary.");
    final DB fbTermsDb = DBMakerUtils.newTempFileDB().make();
    final Set<ByteArray> fbTerms = fbTermsDb.createTreeSet("fbTerms")
        .counterEnable()
        .serializer(ByteArray.SERIALIZER_BTREE)
        .make();
    final Iterator<ByteArray> fbTermsIt;
    final AtomicLong dbg_filteredTerms = new AtomicLong(0L);
    fbTermsIt = this.vocProvider
        .indexDataProvider(this.dataProv)
        .documentIds(this.feedbackDocIds)
        .filter(new VocabularyProvider.Filter() {

          @SuppressWarnings("ReturnOfNull")
          @Override
          public ByteArray filter(final ByteArray term)
              throws DataProviderException {
            final BigDecimal relDf = ImprovedClarityScore.this
                .metrics.collection().relDf(term);
            if (relDf.compareTo(minFreq) >= 0
                && relDf.compareTo(maxFreq) <= 0) {
              return term;
            }
            if (LOG.isDebugEnabled()) {
              dbg_filteredTerms.incrementAndGet();
            }
            return null;
          }
        })
        .get();
    if (LOG.isDebugEnabled() && dbg_filteredTerms.get() > 0) {
      LOG.debug("Filtered terms: {}", dbg_filteredTerms.get());
    }

    while (fbTermsIt.hasNext()) {
      final ByteArray fbTerm = fbTermsIt.next();
      if (fbTerm == null) {
        throw new IllegalStateException("Null term!");
      }
      fbTerms.add(fbTerm);
    }
    LOG.info("{} terms in feedback vocabulary.", fbTerms.size());

    LOG.info("Pre-calculating {} language model values " +
            "from feedback vocabulary ({} documents).",
        fbTerms.size() * this.feedbackDocIds.size(),
        this.feedbackDocIds.size());
    try {
      if (!this.hasAllPrecalculatedModels) {
//        cacheDocumentModels(fbTerms, false);
        this.calculation.cacheDocModels(fbTerms, false);
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
    try {
      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(fbTerms),
              new ModelCalculatorTarget()
          )).process(fbTerms.size());
    } catch (final ProcessingException e) {
      final String msg = "Caught exception while calculating score.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    fbTermsDb.close(); // delete

    LOG.info("Calculating final score.");
    try {
      result.setScore(
          MathUtils.KlDivergence.calc(
              this.c_dataSet.values(),
              MathUtils.KlDivergence.sumValues(this.c_dataSet.values())
          ).doubleValue());
    } catch (final ProcessingException e) {
      final String msg = "Caught exception while calculating score.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    timeMeasure.stop();

    LOG.debug("Calculating improved clarity score for query {} "
            + "with {} document models took {}. {}",
        query, this.feedbackDocIds.size(),
        timeMeasure.getTimeString(), result.getScore()
    );

    return result;
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

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
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
     * Flag indicating, if pre-calculated model values are available for all
     * terms in the index
     */
    HAS_PRECALC_MODELS,
    /**
     * Term to id mapping.
     */
    TERM_IDS_IDX,
    /**
     * Static document model values.
     */
    DOCMODEL_STATIC,
    /**
     * Store ids for terms to not store terms all over again.
     */
    TERM_IDS
  }

  /**
   * Counter for inserts to persistent storage maps to shedule compaction runs.
   */
  private static final class DBInsertCounter {
    /**
     * After how many inserts to a store a compaction should be triggered.
     */
    private static final int COMPACT_AFTER = 1000;
    /**
     * Counter for inserts to {@link #s_docModelStatic}.
     */
    private final AtomicInteger docModelStatic = new AtomicInteger(0);
    /**
     * Counter for inserts to {@link #s_docModels}.
     */
    private final AtomicInteger docModels = new AtomicInteger(0);
    /**
     * Target Database.
     */
    private final DB db;
    /**
     * Counts values stored to the database.
     *
     * @param newDb Database
     */
    DBInsertCounter(final DB newDb) {
      this.db = newDb;
    }

    /**
     * Increments an insert counter.
     *
     * @param target Counter
     */
    void addTo(final Target target) {
      if (target == Target.MODELS) {
        if (this.docModels.get() >= COMPACT_AFTER) {
          runCommit();
        } else {
          this.docModels.incrementAndGet();
        }
      } else if (target == Target.MODELS_STATIC) {
        if (this.docModelStatic.get() >= COMPACT_AFTER) {
          runCommit();
        } else {
          this.docModelStatic.incrementAndGet();
        }
      }
    }

    /**
     * Executes a commit to the store.
     */
    private void runCommit() {
      this.db.commit();
      this.docModels.getAndSet(0);
      this.docModelStatic.getAndSet(0);
    }

    /**
     * Targets for increasing counters.
     */
    private enum Target {
      /**
       * Document model values.
       */
      MODELS,
      /**
       * Static document model values.
       */
      MODELS_STATIC
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
    }    /**
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

  private final class Calculation {
    /**
     * Document model smoothing parameter value.
     */
    // accessed from inner class
    @SuppressWarnings("PackageVisibleField")
    final BigDecimal docModSmoothing;
    /**
     * Document model beta parameter value.
     */
    private final BigDecimal docModBeta;
    /**
     * One (1) minus document model beta parameter value.
     */
    private final BigDecimal docModBeta1Sub;
    /**
     * Document model lambda parameter value.
     */
    private final BigDecimal docModLambda;
    /**
     * One (1) minus document model lambda parameter value.
     */
    private final BigDecimal docModLambda1Sub;

    Calculation(final BigDecimal sm, BigDecimal beta, BigDecimal lambda) {
      this.docModSmoothing = sm;
      this.docModBeta = beta;
      this.docModLambda = lambda;
      this.docModBeta1Sub = BigDecimal.ONE.subtract(this.docModBeta);
      this.docModLambda1Sub = BigDecimal.ONE.subtract(this.docModLambda);
    }

    /**
     * Caches all values that can be pre-calculated and are used for scoring.
     *
     * @param terms Terms to calculate values for
     * @param calcQPortion If true: calculate the static portion of the query
     * model for all query terms. This should only be true, if the passed in
     * terms are query terms. Else the calculation results may be wrong.
     * @throws ProcessingException Thrown on concurrent calculation errors
     */
    public void cacheDocModels(final Iterable<ByteArray> terms,
        final boolean calcQPortion)
        throws ProcessingException, DataProviderException {

      final Processing p = new Processing();
      // count adds to s_docModelStatic and s_docModels for debugging
      final AtomicLong[] dbg_itemCount =
          {new AtomicLong(0L), new AtomicLong(0L)};

      final DB tempTermsDb = DBMakerUtils.newTempFileDB().make();
      final Set<ByteArray> idxTerms = tempTermsDb.createTreeSet("idxTerms")
          .serializer(ByteArray.SERIALIZER_BTREE)
          .make();
      final Set<ByteArray> nonIdxTerms =
          tempTermsDb.createTreeSet("nonIdxTerms")
              .serializer(ByteArray.SERIALIZER_BTREE)
              .make();

      for (final ByteArray term : terms) {
        if (ImprovedClarityScore.this.dataProv.getTermFrequency(term) > 0L) {
          idxTerms.add(term);
        } else {
          nonIdxTerms.add(term);
        }
      }

      p.setSourceAndTarget(new TargetFuncCall<>(
          new CollectionSource<>(ImprovedClarityScore.this.feedbackDocIds),
          new TargetFuncCall.TargetFunc<Integer>() {
            @SuppressWarnings("ReuseOfLocalVariable")
            @Override
            public void call(final Integer docId)
                throws Exception {
              if (docId == null) {
                return;
              }

              // data-model of current document
              final DocumentModel docModel = ImprovedClarityScore.this
                  .metrics.getDocumentModel(docId);
              // partial query model value
              BigDecimal qModelPart = BigDecimal.ONE;

              // cache static document model values
              if (!ImprovedClarityScore.this
                  .s_docModelStatic.containsKey(docId)) {
                ImprovedClarityScore.this
                    .s_docModelStatic.put(docId,
                    BigDecimalCache.get(docModel.metrics().tf())
                        .add(BigDecimalCache.getMul(
                            BigDecimalCache.get(
                                docModel.metrics().uniqueTermCount()),
                            Calculation.this.docModSmoothing))
                );
                if (LOG.isDebugEnabled()) {
                  dbg_itemCount[0].incrementAndGet();
                }
                ImprovedClarityScore.this.dbInsertTracker.addTo(
                    DBInsertCounter.Target.MODELS_STATIC);
              }

              // go through all index terms
              for (final ByteArray term : idxTerms) {
                // storage key
                final Fun.Tuple2<Integer, Long> mapKey = Fun.t2(
                    docId, getTermId(term));
                // calculate and store model value, if needed
                final BigDecimal termModel = _cacheModel(docId, term,
                    mapKey, docModel, ImprovedClarityScore.this.s_docModels);

                if (LOG.isDebugEnabled() && termModel != null) {
                  dbg_itemCount[1].incrementAndGet();
                }

                // calculate the static portion of the query model
                if (calcQPortion) {
                  qModelPart = qModelPart.multiply(
                      _getTermModel(termModel, mapKey));
                }
              }

              // go through all non-index terms
              for (final ByteArray term : nonIdxTerms) {
                // storage key
                final Fun.Tuple2<Integer, Long> mapKey = Fun.t2(
                    docId, getTermId(term));

                // calculate and store model value, if needed
                final BigDecimal termModel = _cacheModel(
                    docId, term, mapKey, docModel,
                    ImprovedClarityScore.this.c_tempDocModels);

                // calculate the static portion of the query model
                if (calcQPortion) {
                  qModelPart = qModelPart.multiply(
                      _getTermModel(termModel, mapKey));
                }
              }

              // store the static portion of the query model
              if (calcQPortion) {
                ImprovedClarityScore.this
                    .c_queryModelParts.put(docId, qModelPart);
              }
            }
          }
      )).process(ImprovedClarityScore.this.feedbackDocIds.size());

      if (LOG.isDebugEnabled()
          && (dbg_itemCount[0].get() > 0L || dbg_itemCount[1].get() > 0L)) {
        LOG.debug("New entries: dmStatic={} dm={}",
            dbg_itemCount[0].get(), dbg_itemCount[1].get());
      }
      tempTermsDb.close();
    }

    /**
     * Cache a document model value.
     *
     * @param docId Document id
     * @param term Term
     * @param mapKey Storage key
     * @param docModel Document data model
     * @param target Target map
     * @return Model value, if value was not already stored or {@code null} if
     * it was already stored
     * @throws DataProviderException
     */
    BigDecimal _cacheModel(final Integer docId, final ByteArray term,
        final Fun.Tuple2<Integer, Long> mapKey,
        final DocumentModel docModel,
        final Map<Fun.Tuple2<Integer, Long>, BigDecimal> target)
        throws DataProviderException {
      // skip terms already calculated
      if (!target.containsKey(mapKey)) {
        // store model value - term in collection
        final BigDecimal termModel = calcDocumentModel(
            docId, docModel.tf(term), term);
        target.put(mapKey, termModel);
        return termModel;
      }
      return null;
    }

    /**
     * Get the model value for a term, if not already defined.
     *
     * @param termModel Current value, may be null
     * @param mapKey Key to retrieve model
     * @return model value
     */
    BigDecimal _getTermModel(BigDecimal termModel,
        final Fun.Tuple2<Integer, Long> mapKey) {
      // null, if not calculated previously, because it was cached
      // already
      if (termModel == null) {
        termModel = ImprovedClarityScore.this
            .s_docModels.get(mapKey);
        if (termModel == null) {
          termModel = ImprovedClarityScore.this
              .c_tempDocModels.get(mapKey);
        }
      }
      return termModel;
    }

    /**
     * Calculate the document model for a given document and term. All values
     * for the current feedback documents and feedback vocabulary must be
     * pre-calculated if this method is called.
     *
     * @param docId Document id
     * @param termInDocFreq Frequency of current term in document
     * @param term Term to calculate
     * @return Model for document and term
     * @throws DataProviderException Thrown on low-level errors
     * @see Calculation#cacheDocModels(Iterable, boolean)
     */
    private BigDecimal calcDocumentModel(final Integer docId,
        final long termInDocFreq, final ByteArray term)
        throws DataProviderException {
      // relative frequency of the term in the whole collection
      final BigDecimal termRelIdxFreq = ImprovedClarityScore.this
          .dataProv.getRelativeTermFrequency(term);
      // final value
      BigDecimal result;

      // calculate smoothed document model s(t,D)
      if (termInDocFreq == 0L) {
        // (mu * pc(t)) / (fD + (mu * f_T,D))
        result =
            BigDecimalCache.getMul(termRelIdxFreq, this.docModSmoothing)
                .divide(ImprovedClarityScore.this
                    .s_docModelStatic.get(docId), MATH_CONTEXT);
      } else {
        // (f_t,d + (mu * pc(t))) / (fD + (mu * f_T,D))
        result =
            BigDecimalCache.get(termInDocFreq)
                .add(BigDecimalCache
                    .getMul(termRelIdxFreq, this.docModSmoothing))
                .divide(ImprovedClarityScore.this
                    .s_docModelStatic.get(docId), MATH_CONTEXT);
      }

      // (beta * s(t,d)) + (1- beta) * pc(t)
      result = this.docModBeta.multiply(result).add(
          BigDecimalCache.getMul(termRelIdxFreq, this.docModBeta1Sub));

      // (lambda * result) + (1 - lambda) * pc(t)
      if (this.docModLambda.compareTo(BigDecimal.ONE) == 0) {
        // result + 0
        return result;
      } else {
        return this.docModLambda.multiply(result).add(
            BigDecimalCache.getMul(termRelIdxFreq, this.docModLambda1Sub));
      }
    }
  }

  /**
   * {@link Processing} {@link Target} to calculate document models.
   */
  private final class ModelCalculatorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    @Override
    public void call(final ByteArray term)
        throws DataProviderException {
      if (term != null) {
        ImprovedClarityScore.this.c_dataSet.put(
            ImprovedClarityScore.this.dataSetCounter.incrementAndGet(),
            Fun.t2(getQueryModel(term), ImprovedClarityScore.this
                .dataProv.getRelativeTermFrequency(term)));
      }
    }
  }
}
