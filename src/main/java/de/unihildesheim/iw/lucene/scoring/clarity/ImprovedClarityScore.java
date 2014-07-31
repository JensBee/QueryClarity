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
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

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
   * Caches values of document and query models. Those need to be normalized
   * before calculating the score.
   */
  // accessed from multiple threads
  // accessed from inner class
  @SuppressWarnings("PackageVisibleField")
  final Collection<Fun.Tuple2<BigDecimal, BigDecimal>> dataSet;
  /**
   * On disk caching the values of default document models that get used, if a
   * term is not in a specific document.
   */
  // accessed from inner classes
  @SuppressWarnings("PackageVisibleField")
  final ConcurrentMap<
      Fun.Tuple2<Integer, ByteArray>, BigDecimal> defaultDocModelCache;
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
  private final ImprovedClarityScoreConfiguration conf;
  /**
   * Document model smoothing parameter value.
   */
  private final BigDecimal docModSmoothing;
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
  /**
   * List of query terms provided.
   */
  // accessed from inner classes
  @SuppressWarnings("PackageVisibleField")
  Set<ByteArray> queryTerms;
  /**
   * Disk based storage of document models. Containing only models for
   * document-term combinations.
   */
  // accessed from inner classes
  @SuppressWarnings("PackageVisibleField")
  ConcurrentMap<Fun.Tuple2<Integer, ByteArray>, BigDecimal> docModelStore;
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
   * Flag indicating, if a unit-test is running. This should prevent overwriting
   * test-data with real values.
   */
  private boolean isTest;

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
    } catch (DataProviderException e) {
      throw new Buildable.BuildException("Error while initializing.", e);
    }
    this.conf.debugDump();

    // localize some values for time critical calculations
    this.docModSmoothing = BigDecimalCache.get(
        this.conf.getDocumentModelSmoothingParameter());
    this.docModBeta =
        BigDecimalCache.get(this.conf.getDocumentModelParamBeta());
    this.docModBeta1Sub = BigDecimal.ONE.subtract(this.docModBeta);
    this.docModLambda = BigDecimalCache.get(this.conf
        .getDocumentModelParamLambda());
    this.docModLambda1Sub = BigDecimal.ONE.subtract(this.docModLambda);

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
        .compressionEnable()
        .make();
    this.cache = DBMakerUtils.newTempFileDB().make();
    this.dataSet = new ConcurrentLinkedQueue<>();
    this.queryModelPartCache = this.memCache
        .createTreeMap("qmCache")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
        .valueSerializer(Serializer.BASIC)
        .make();
    this.defaultDocModelCache = this.cache
        .createTreeMap("ddmCache")
        .keySerializer(new BTreeKeySerializer.Tuple2KeySerializer<>(
            null, Serializer.INTEGER, ByteArray.SERIALIZER))
        .valueSerializer(Serializer.BASIC)
        .make();

    try {
      this.initCache(builder);
    } catch (DataProviderException e) {
      throw new Buildable.BuildException(
          "Error while initializing cache.", e);
    }

    if (this.persist.getDb().exists(DataKeys.HAS_PRECALC_MODELS.name())) {
      this.persist.getDb().getAtomicBoolean(DataKeys.HAS_PRECALC_MODELS.name())
          .getAndSet(true);
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
   * Initializes a cache using the {@link Builder}.
   *
   * @param builder Builder instance
   * @throws Buildable.BuildableException Thrown, if building the cache instance
   * failed
   */
  private void initCache(final ScoringBuilder builder)
      throws Buildable.BuildableException, DataProviderException {
    final Persistence.Builder psb = builder.getCache();
    psb.dbMkr
        .noChecksum()
        .cacheSize(65536)
        .transactionDisable();

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

    this.docModelStore = this.persist.getDb()
        .createTreeMap(DataKeys.DOCMODEL.name())
        .keySerializer(new BTreeKeySerializer.Tuple2KeySerializer<>(
            null, Serializer.INTEGER, ByteArray.SERIALIZER))
        .valueSerializer(Serializer.BASIC)
        .nodeSize(6)
        .valuesOutsideNodesEnable()
        .makeOrGet();

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
   * @throws ProcessingException
   * @throws DataProviderException
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

            // frequency of term in document
            final BigDecimal docTermFreq = BigDecimalCache.get(
                docModel.metrics().tf());
            // number of unique terms in document
            final BigDecimal termsInDoc = BigDecimalCache.get(
                docModel.metrics().uniqueTermCount());

            // calculate model values for all document terms
            for (final ByteArray term : docModel.getTermFreqMap().keySet()) {
              // calculate also the default value,
              // if term is not in document
              ImprovedClarityScore.this.docModelStore.put(Fun.t2(docId, term),
                  calcDocumentModel(docTermFreq, termsInDoc,
                      docModel.metrics().tf(term), term));
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
  }

  /**
   * Calculate the document model for a given document and term.
   *
   * @param docTermFreq Frequency of all terms in document
   * @param termsInDoc Number of unique terms in document
   * @param termInDocFreq Frequency of current term in document
   * @param term Term to calculate
   * @return Model for document and term
   */
  private BigDecimal calcDocumentModel(
      final BigDecimal docTermFreq, final BigDecimal termsInDoc,
      final long termInDocFreq, final ByteArray term)
      throws DataProviderException {
    // relative frequency of the term in the whole collection
    final BigDecimal termRelIdxFreq = this.metrics.collection().relTf(term);

    final BigDecimal smoothedTerm;
    if (termInDocFreq == 0L) {
      smoothedTerm =
          this.docModSmoothing.multiply(termRelIdxFreq)
              .divide(docTermFreq.add(
                  this.docModSmoothing.multiply(termsInDoc)), MATH_CONTEXT);
    } else {
      smoothedTerm =
          BigDecimalCache.get(termInDocFreq)
              .add(this.docModSmoothing.multiply(termRelIdxFreq))
              .divide(docTermFreq
                      .add(this.docModSmoothing.multiply(termsInDoc)),
                  MATH_CONTEXT);
    }

    final BigDecimal coreResult =
        this.docModBeta.multiply(smoothedTerm).add(
            this.docModBeta1Sub.multiply(termRelIdxFreq));

    if (this.docModLambda.compareTo(BigDecimal.ONE) == 0) {
      return coreResult;
    } else {
      return this.docModLambda.multiply(coreResult).add(
          this.docModLambda1Sub.multiply(termRelIdxFreq));
    }
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
   * Calculate the query model.
   *
   * @param term Feedback term
   * @return Query model for the current term and set of feedback documents
   */
  BigDecimal getQueryModel(final ByteArray term)
      throws DataProviderException {
    BigDecimal modelPart;
    BigDecimal model = BigDecimal.ZERO;
    BigDecimal value;
    Metrics.DocumentMetrics docMetrics;

    for (final Integer docId : this.feedbackDocIds) {
      // get the static part for all query terms (pre-calculated)
      modelPart = this.queryModelPartCache.get(docId);
      if (modelPart == null) {
        throw new IllegalStateException(
            "Pre-calculated model part value not found.");
      }

      // get the value for the current document & feedback term
      final Fun.Tuple2<Integer, ByteArray> mapKey = Fun.t2(docId, term);

      // get the value for the current document & feedback term
      // if the term is in the document it must be available here, since we have
      // pre-calculated data
      value = this.docModelStore.get(mapKey);
      if (value == null) {
        // term not in document - get default model
        value = this.defaultDocModelCache.get(mapKey);

        if (value == null) {
          // worst case: we have to calculate a missing value ad-hoc
          docMetrics = this.metrics.getDocumentModel(docId).metrics();
          value = calcDocumentModel(
              docMetrics.tf().doubleValue(),
              docMetrics.uniqueTermCount().doubleValue(),
              term, 0L);
          if (this.metrics.collection().tf(term) > 0) {
            this.docModelStore.put(mapKey, value);
          } else {
            this.defaultDocModelCache.put(mapKey, value);
          }
        }
      }

      // add values together
      model = model.add(modelPart.multiply(value));
    }
    return model;
  }

  /**
   * Calculates a single document model value.
   *
   * @param docTermFreq Term frequency in document
   * @param termsInDoc Number of unique terms in document
   * @param term Current term
   * @param termInDocFreq Frequency of current term in document
   * @return Model value
   * @see #calcDocumentModel(BigDecimal, BigDecimal, long, ByteArray)
   */
  private BigDecimal calcDocumentModel(
      final double docTermFreq, final double termsInDoc,
      final ByteArray term, final long termInDocFreq)
      throws DataProviderException {

    return calcDocumentModel(
        BigDecimalCache.get(docTermFreq),
        BigDecimalCache.get(termsInDoc),
        termInDocFreq,
        term
    );

//    // relative frequency of the term in the whole collection
//    final BigDecimal termRelIdxFreq = BigDecimalCache.get(
//        this.metrics.collection().relTf(term));
//
//    final BigDecimal smoothedTerm =
//        BigDecimalCache.get(termInDocFreq)
//            .add(this.docModSmoothing.multiply(termRelIdxFreq))
//            .divide(BigDecimalCache.get(docTermFreq)
//                .add(this.docModSmoothing
//                        .multiply(BigDecimalCache.get(termsInDoc))
//                ), MATH_CONTEXT);
//
//    final BigDecimal coreResult =
//        this.docModBeta.multiply(smoothedTerm).add(
//            this.docModBeta1Sub.multiply(termRelIdxFreq));
//
//    if (this.docModLambda.compareTo(BigDecimal.ONE) == 0) {
//      return coreResult;
//    } else {
//      return this.docModLambda.multiply(coreResult).add(
//          this.docModLambda1Sub.multiply(termRelIdxFreq));
//    }
  }

  /**
   * Set values used for unit testing.
   *
   * @param testFbDocIds List of feedback documents to use
   * @param testQueryTerms List of query terms to use TODO: document frequency
   * values ignored for feedback terms
   * @throws ProcessingException
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  void testSetValues(final Set<Integer> testFbDocIds,
      final Map<ByteArray, Integer> testQueryTerms)
      throws ProcessingException, DataProviderException {
    this.feedbackDocIds = testFbDocIds;
    this.queryTerms = testQueryTerms.keySet();

    clearStore();
    createDocumentModels();
    cacheDocumentModels(this.queryTerms, true);
    this.isTest = true;
  }

  /**
   * Caches all values that can be pre-calculated and are used for scoring.
   *
   * @throws ProcessingException Thrown on concurrent calculation errors
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  private void cacheDocumentModels(final Set<ByteArray> terms,
      final boolean calcQPortion)
      throws ProcessingException {
    new Processing().setSourceAndTarget(new TargetFuncCall<>(
        new CollectionSource<>(this.feedbackDocIds),
        new TargetFuncCall.TargetFunc<Integer>() {
          @Override
          public void call(final Integer docId)
              throws Exception {
            if (docId == null) {
              return;
            }
            final DocumentModel docModel = ImprovedClarityScore.this.metrics
                .getDocumentModel(docId);
            // frequency of term in document
            final BigDecimal docTermFreq = BigDecimalCache.get(
                docModel.metrics().tf());
            // number of unique terms in document
            final BigDecimal termsInDoc = BigDecimalCache.get(
                docModel.metrics().uniqueTermCount());

            // calculate model values for all unique query terms
            for (final ByteArray term : terms) {
              // check, if already calculated
              final Fun.Tuple2<Integer, ByteArray> mapKey = Fun.t2(docId, term);
              if (docModel.contains(term) || ImprovedClarityScore.this
                  .metrics.collection().tf(term) > 0L) {
                if (!ImprovedClarityScore.this
                    .docModelStore.containsKey(mapKey)) {
                  // Store term data. Term in document or a collection term
                  ImprovedClarityScore.this.docModelStore.put(mapKey,
                      calcDocumentModel(docTermFreq, termsInDoc,
                          docModel.metrics().tf(term), term));
                }
              } else if (!ImprovedClarityScore.this
                  .defaultDocModelCache.containsKey(mapKey)) {
                // cache term data - not a collection term
                ImprovedClarityScore.this.defaultDocModelCache.put(mapKey,
                    calcDocumentModel(docTermFreq, termsInDoc, 0L, term));
              }
            }

            // calculate the static portion of the query model for all query
            // terms, if desired
            if (calcQPortion) {
              Fun.Tuple2<Integer, ByteArray> mapKey;
              BigDecimal modelPart = BigDecimal.ONE;

              BigDecimal value;
              for (final ByteArray qTerm :
                  ImprovedClarityScore.this.queryTerms) {
                mapKey = Fun.t2(docId, qTerm);
                value = ImprovedClarityScore.this
                    .docModelStore.get(mapKey);

                if (value == null) {
                  value = ImprovedClarityScore.this
                      .defaultDocModelCache.get(mapKey);
                }

                modelPart = modelPart.multiply(value);

                if (modelPart.compareTo(BigDecimal.ZERO) == 0) {
                  // stays zero
                  break;
                }
              }
              ImprovedClarityScore.this
                  .queryModelPartCache.put(docId, modelPart);
            }
          }
        }
    )).process(this.feedbackDocIds.size());
  }

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
    LOG.info("Clearing temporary caches.");
    this.queryModelPartCache.clear();

    // result object
    final Result result = new Result(this.getClass());

    if (!this.isTest) { // test sets value manually
      // get a normalized map of query terms
      // skips stopwords and removes unknown terms
      this.queryTerms = new HashSet<>(QueryUtils.tokenizeQuery(query,
          this.analyzer, this.metrics.collection()));
    }

    // check query term extraction result
    if (this.queryTerms == null || this.queryTerms.isEmpty()) {
      result.setEmpty("No query terms.");
      return result;
    }

    // save base data to result object
    result.setConf(this.conf);
    result.setQueryTerms(this.queryTerms);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    if (!this.isTest) { // test sets value manually
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

//    // get document frequency threshold - allowed terms must be in bounds
//    double minFreq = (double) (this.metrics.collection().numberOfDocuments()
//        * this.conf.getMinFeedbackTermSelectionThreshold());
//    if (minFreq < 0) {
//      minFreq = 0;
//    }
//    final double maxFreq = (double)
//        (this.metrics.collection().numberOfDocuments() *
//            this.conf.getMaxFeedbackTermSelectionThreshold());
//    LOG.debug("Feedback term Document frequency threshold: low=({} = {}) hi=
// ({} = {})",
//        minFreq, this.conf.getMinFeedbackTermSelectionThreshold(),
//        maxFreq, this.conf.getMaxFeedbackTermSelectionThreshold()
//    );
    final double minFreq = this.conf.getMinFeedbackTermSelectionThreshold();
    final double maxFreq = this.conf.getMaxFeedbackTermSelectionThreshold();
    LOG.debug("Feedback term Document frequency threshold: {}%-{}%",
        minFreq * 100d, maxFreq * 100d);

    // cache document models
    LOG.info("Caching {} document models.", this.feedbackDocIds.size());
    final TimeMeasure tm = new TimeMeasure().start();
    try {
      cacheDocumentModels(this.queryTerms, true);
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
      cacheDocumentModels(fbTerms, false);
    } catch (final ProcessingException e) {
      final String msg = "Caught exception while pre-calculating models.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    LOG.info("Calculating {} language models using feedback vocabulary.",
        fbTerms.size());
    try {
      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(fbTerms),
//              new IteratorSource<>(
//                  this.vocProvider
//                      .indexDataProvider(this.dataProv)
//                      .documentIds(this.feedbackDocIds)
//                      .get()),
              new ModelCalculatorTarget(minFreq, maxFreq)
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
          MathUtils.KlDivergence.calcBig(
              this.dataSet, MathUtils.KlDivergence.sumBigValues(this.dataSet)
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
    HAS_PRECALC_MODELS;
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
     * Configuration prefix.
     */
    private static final String CONF_PREFIX = IDENTIFIER + "-result";

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

    @Override
    public ScoringResultXml getXml() {
      final ScoringResultXml xml = new ScoringResultXml();

      getXml(xml);

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

  /**
   * {@link Processing} {@link Target} to calculate document models.
   */
  private final class ModelCalculatorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {
    /**
     * Minimum frequency a term must match.
     */
    private final BigDecimal minFreq;
    /**
     * Maximum frequency a term may reach.
     */
    private final BigDecimal maxFreq;

    ModelCalculatorTarget(final double newMinFreq, final double newMaxFreq) {
      this.minFreq = BigDecimalCache.get(newMinFreq);
      this.maxFreq = BigDecimalCache.get(newMaxFreq);
    }

    @Override
    public void call(final ByteArray term)
        throws DataProviderException {
      if (term != null) {
        final BigDecimal tfc = ImprovedClarityScore.this.
            metrics.collection().relTf(term);
        if (tfc.compareTo(this.minFreq) >= 0
            && tfc.compareTo(this.maxFreq) <= 0) {
          ImprovedClarityScore.this.dataSet.add(
              Fun.t2(getQueryModel(term), tfc));
        }
      }
    }
  }
}
