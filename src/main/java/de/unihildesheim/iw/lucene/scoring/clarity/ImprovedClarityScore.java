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
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
   * Reader to access the Lucene index.
   */
  private final IndexReader idxReader;
  /**
   * Lucene query analyzer.
   */
  private final Analyzer analyzer;
  /**
   * Set of feedback documents to use for calculation.
   */
  protected Set<Integer> feedbackDocIds;
  /**
   * List of query terms provided.
   */
  protected Map<ByteArray, Integer> queryTerms;
  /**
   * Manager for extended document meta-data.
   */
  ExternalDocTermDataManager extDocMan;
  /**
   * Configuration object used for all parameters of the calculation.
   */
  private ImprovedClarityScoreConfiguration conf;
  /**
   * Database instance.
   */
  private Persistence persist;
  /**
   * Flag indicating, if this instance has been closed.
   */
  private volatile boolean isClosed;
  private DB cache = DBMaker.newMemoryDirectDB()
      .transactionDisable()
      .compressionEnable()
      .make();
  private Map<Integer, Double> queryModelPartCache = cache
      .createHashMap("qmCache")
      .expireStoreSize(0.5) // 500mb cache
      .make();
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
    psb.dbMkr.compressionDisable()
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
      this.cache.close();
      this.persist.closeDb();
      this.isClosed = true;
    }
  }

  /**
   * Calculate the query model.
   *
   * @param fbTerm Feedback term
   * @return Query model for the current term and set of feedback documents
   */
  double getQueryModel(final ByteArray fbTerm) {
    Set<Map.Entry<ByteArray, Integer>> qtSet = null;
    double docModelQtProduct;
    double model = 0d;
    for (final Integer docId : this.feedbackDocIds) {
      // document model for the given term pD(t)
      final double docModel = this.extDocMan.getDirect(
          DataKeys.DOCMODEL.id(), docId, fbTerm);

      // calculate the product of the document models for all query terms
      // given the current document
      try {
        model += (docModel * this.queryModelPartCache.get(docId));
      } catch (final NullPointerException e) {
        if (qtSet == null) {
          qtSet = this.queryTerms.entrySet();
        }
        docModelQtProduct = 1d;
        for (final Map.Entry<ByteArray, Integer> qTermEntry : qtSet) {
          docModelQtProduct *=
              (this.extDocMan.getDirect(
                  DataKeys.DOCMODEL.id(), docId, qTermEntry.getKey())
                  * qTermEntry.getValue());

          if (docModelQtProduct <= 0d) {
            // short circuit, if model is already too low
            break;
          }
        }
        this.queryModelPartCache.put(docId, docModelQtProduct);
        model += docModel * docModelQtProduct;
      }
    }
    return model;
  }

  void testSetValues(final Set<Integer> testFbDocIds,
      final Map<ByteArray, Integer> testQueryTerms) {
    this.feedbackDocIds = testFbDocIds;
    this.queryTerms = testQueryTerms;
    cacheDocumentModels();
    isTest = true;
  }

//  /**
//   * Calculate or get the document model (pd) for a specific document.
//   *
//   * @param docId Id of the document whose model to get
//   * @return Mapping of each term in the document to it's model value
// available
//   * was interrupted
//   */
//  Map<ByteArray, Double> getDocumentModel(final int docId) {
//    checkClosed();
//
//    // try to get pre-calculated values
//    Map<ByteArray, Double> map =
//        this.extDocMan.getData(docId, DataKeys.DOCMODEL.name());
//    final boolean calculate;
//
//    if (map == null || map.isEmpty()) {
//      // document data model
//      final DocumentModel docModel = this.metrics.getDocumentModel(docId);
//      // frequency of term in document
//      final double docTermFreq = docModel.metrics().tf().doubleValue();
//      // number of unique terms in document
//      final double termsInDoc =
//          docModel.metrics().uniqueTermCount().doubleValue();
//      // unique list of all terms in document
//      final Set<ByteArray> terms = docModel.getTermFreqMap().keySet();
//      // store results for all terms in document
//      map = new HashMap<>(terms.size());
//
//      // calculate model values for all terms in document
//      for (final ByteArray term : terms) {
//        map.put(term,
//            calcDocumentModel(docModel, docTermFreq, termsInDoc, term));
//      }
//      // push data to persistent storage
//      this.extDocMan.setData(docId, DataKeys.DOCMODEL.name(), map);
//    } else {
//      map = this.extDocMan.getData(docId, DataKeys.DOCMODEL.name());
//    }
//
//    assert map != null && !map.isEmpty() : "Model map was empty.";
//    return map;
//  }

//  /**
//   * Safe method to get the document model value for a document term. This
//   * method will calculate an additional value, if the term in question is not
//   * contained in the document. <br> A call to this method should only needed,
//   * if {@code lambda} in {@link #conf} is lesser than {@code 1}.
//   *
//   * @param docId Id of the document whose model to get
//   * @param term Term to calculate the model for
//   * @return Model value for document & term
//   */
//  double getDocumentModel(final int docId, final ByteArray term) {
//    assert term != null;
//    checkClosed();
//    Double model = getDocumentModel(docId).get(term);
//    // if term not in document, calculate new value
//    if (model == null) {
//      // document data model
//      final DocumentModel docModel = this.metrics.getDocumentModel(docId);
//      // frequency of term in document
//      final double docTermFreq = docModel.metrics().tf().doubleValue();
//      // number of unique terms in document
//      final double termsInDoc =
//          docModel.metrics().uniqueTermCount().doubleValue();
//
//      model = calcDocumentModel(docModel, docTermFreq, termsInDoc, term);
//    }
//    assert model > 0d;
//    return model;
//  }

  @SuppressWarnings("ObjectAllocationInLoop")
  private void cacheDocumentModels() {
    Map<ByteArray, Double> map;

    final DB memDb = DBMakerUtils.newCompressedMemoryDirectDB().make();
    final Set<ByteArray> fbTerms = memDb.createTreeSet("fbTerms").make();

    // get an iterator over all feedback terms
    final Iterator<Map.Entry<ByteArray, Long>> fbTermIt = this.dataProv
        .getDocumentsTerms(this.feedbackDocIds);

    while (fbTermIt.hasNext()) {
      fbTerms.add(fbTermIt.next().getKey());
    }

    // run pre-calculation for all feedback documents
    for (final Integer docId : this.feedbackDocIds) {
      final DocumentModel docModel = this.metrics.getDocumentModel(docId);

      map = this.extDocMan.getData(docId, DataKeys.DOCMODEL.id());
      if (map == null || map.isEmpty()) {
        map = new HashMap<>((int) (this.queryTerms.size() * 1.5));
      }

      // frequency of term in document
      final double docTermFreq = docModel.metrics().tf().doubleValue();
      // number of unique terms in document
      final double termsInDoc =
          docModel.metrics().uniqueTermCount().doubleValue();

      // calculate model values for all unique feedback terms
      for (final ByteArray term : fbTerms) {
        // calculate also the default value, if term is not in document
        if (!map.containsKey(term)) {
          map.put(term, calcDocumentModel(
              docModel, docTermFreq, termsInDoc, term));
        }
      }

      // calculate model values for all unique query terms
      for (final ByteArray term : this.queryTerms.keySet()) {
        if (!map.containsKey(term)) {
          map.put(term, calcDocumentModel(
              docModel, docTermFreq, termsInDoc, term));
        }
      }

      if (!map.isEmpty()) {
        this.extDocMan.setData(docId, DataKeys.DOCMODEL.id(), map);
      }
    }
    memDb.close();
    this.persist.getDb().compact();
  }

  /**
   * Calculate the document model for a given document and term.
   *
   * @param docModel Document data model
   * @param docTermFreq Frequency of all terms in document
   * @param termsInDoc Number of unique terms in document
   * @param term Term to calculate
   * @return Model for document and term
   */
  private double calcDocumentModel(final DocumentModel docModel,
      final double docTermFreq, final double termsInDoc,
      final ByteArray term) {
    // relative frequency of the term in the whole collection
    final double termRelIdxFreq = this.metrics.collection().relTf(term);
    // term frequency given the document
    final double termInDocFreq = docModel.metrics().tf(term).doubleValue();

    final double smoothedTerm =
        (termInDocFreq +
            (this.conf.getDocumentModelSmoothingParameter() *
                termRelIdxFreq)) /
            (docTermFreq +
                (this.conf.getDocumentModelSmoothingParameter() *
                    termsInDoc));
    return (this.conf.getDocumentModelParamLambda() *
        ((this.conf.getDocumentModelParamBeta() * smoothedTerm) +
            ((1d - this.conf.getDocumentModelParamBeta()) *
                termRelIdxFreq))
    ) + ((1d - this.conf.getDocumentModelParamLambda()) *
        termRelIdxFreq);
  }

  /**
   * Calculates the improved clarity score for a given query.
   *
   * @param query Query to calculate for
   * @return Clarity score result object
   */
  @Override
  public Result calculateClarity(final String query)
      throws ClarityScoreCalculationException {
    checkClosed();
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // result object
    final Result result = new Result(this.getClass());
    // final clarity score
    final AtomicDouble score = new AtomicDouble(0d);

    if (!isTest) {
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

    final TryExactTermsQuery relaxableQuery;
    try {
      relaxableQuery = new TryExactTermsQuery(
          this.analyzer, QueryParser.escape(query),
          this.dataProv.getDocumentFields());
    } catch (final ParseException e) {
      final String msg = "Caught exception while building query.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    // collect of feedback document ids
    if (!isTest) {
      try {
        this.feedbackDocIds = Feedback.getFixed(this.idxReader, relaxableQuery,
            this.conf.getMaxFeedbackDocumentsCount());
      } catch (final IOException | ParseException e) {
        final String msg =
            "Caught exception while retrieving feedback documents.";
        LOG.error(msg, e);
        throw new ClarityScoreCalculationException(msg, e);
      }
    }
    if (this.feedbackDocIds.isEmpty()) {
      result.setEmpty("No feedback documents.");
      return result;
    }
    result.setFeedbackDocIds(this.feedbackDocIds);

    // cache document models
    LOG.info("Caching {} document models.", this.feedbackDocIds.size());
    final TimeMeasure tm = new TimeMeasure().start();
    cacheDocumentModels();
    LOG.debug("Caching document models took {}.", tm.stop().getTimeString());

//    // collect all terms from feedback documents
//    final Iterator<Map.Entry<ByteArray, Long>> fbTermsMapIt = this.dataProv
//        .getDocumentsTerms(this.feedbackDocIds);

    // get document frequency threshold - allowed terms must be in bounds
    int minDf = (int) (this.metrics.collection().numberOfDocuments()
        * this.conf.getMinFeedbackTermSelectionThreshold());
    if (minDf <= 0) {
      LOG.debug("Document frequency threshold was {} setting to 1", minDf);
      minDf = 1;
    }
    final int maxDf = (int) (this.metrics.collection().numberOfDocuments()
        * this.conf.getMaxFeedbackTermSelectionThreshold());
    LOG.debug("Document frequency threshold: low=({} = {}) hi=({} = {})",
        minDf, this.conf.getMinFeedbackTermSelectionThreshold(),
        maxDf, this.conf.getMaxFeedbackTermSelectionThreshold()
    );

//    // keep results of concurrent term eliminations
//    final ConcurrentLinkedQueue<ByteArray> reducedFbTerms
//        = new ConcurrentLinkedQueue<>();
//
//    // remove all terms whose threshold is too low
//    try {
//      new Processing(
//          new TargetFuncCall<>(
//              new IteratorSource<>(fbTermsMapIt),
//              new FbTermReducerTarget(this.metrics.collection(), minDf, maxDf,
//                  reducedFbTerms)
//          )
//      ).process();
//    } catch (final ProcessingException e) {
//      final String msg = "Failed to reduce terms by threshold.";
//      LOG.error(msg, e);
//      throw new ClarityScoreCalculationException(msg, e);
//    }
//
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Reduced term set size {}.", reducedFbTerms.size());
//
//      final Collection<String> sTerms = new ArrayList<>(reducedFbTerms.size
// ());
//      for (final ByteArray bTerm : reducedFbTerms) {
//        sTerms.add(ByteArrayUtils.utf8ToString(bTerm));
//      }
//      LOG.debug("Feedback terms {}", sTerms);
//    }
//
//    if (reducedFbTerms.isEmpty()) {
//      result.setEmpty("No terms remaining after elimination based " +
//          "on document frequency threshold (" + minDf + ").");
//      return result;
//    }
//
//    result.setFeedbackTerms(reducedFbTerms);

    // do the final calculation for all remaining feedback terms
    if (LOG.isTraceEnabled()) {
      LOG.trace("Using {} feedback documents for calculation. {}",
          this.feedbackDocIds.size(), this.feedbackDocIds);
    } else {
      LOG.debug("Using {} feedback documents for calculation.",
          this.feedbackDocIds.size());
    }
    try {
      new Processing(
          new TargetFuncCall<>(
              new IteratorSource<>(
                  this.dataProv.getDocumentsTerms(this.feedbackDocIds)),
              new ModelCalculatorTarget(score, minDf, maxDf)
          )
      ).process();
    } catch (final ProcessingException e) {
      final String msg = "Caught exception while calculating score.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    result.setScore(score.get());

    timeMeasure.stop();
    LOG.debug("Calculating improved clarity score for query {} "
            + "with {} document models took {}. {}",
        query, this.feedbackDocIds.size(),
        timeMeasure.getTimeString(), score
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

//  /**
//   * Pre-calculate all document models for all terms known from the index.
//   *
//   * @throws ProcessingException Thrown, if threaded calculation
// encountered an
//   * error
//   */
//  public void preCalcDocumentModels()
//      throws ProcessingException {
//    if (!this.hasCache) {
//      LOG.warn("Won't pre-calculate any values. Cache not set.");
//    }
//    final Atomic.Boolean hasData = this.persist.getDb().getAtomicBoolean(
//        Caches.HAS_PRECALC_DATA.name());
//    if (hasData.get()) {
//      LOG.info("Pre-calculated models are current.");
//    } else {
//      LOG.info("Pre-calculating models.");
//      new Processing(
//          new TargetFuncCall<>(
//              new IteratorSource<>(this.dataProv.getDocumentIds()),
//              new DocumentModelCalculatorTarget()
//          )
//      ).process(this.metrics.collection().numberOfDocuments().intValue());
//      hasData.set(true);
//    }
//  }

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
  enum DataKeys {

    /**
     * Document-models.
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

//  /**
//   * {@link Processing} {@link Target} to reduce feedback terms.
//   */
//  @SuppressWarnings("ProtectedInnerClass")
//  protected static final class FbTermReducerTarget
//      extends TargetFuncCall.TargetFunc<Map.Entry<ByteArray, Long>> {
//
//    /**
//     * Target to store terms passing through the reducing process.
//     */
//    private final Queue<ByteArray> reducedTermsTarget;
//    /**
//     * Minimum document frequency for a term to pass.
//     */
//    private final int minDf;
//    /**
//     * Maximum document frequency for a term to pass.
//     */
//    private final int maxDf;
//    /**
//     * Access collection metrics.
//     */
//    private final Metrics.CollectionMetrics collectionMetrics;
//
//    /**
//     * Creates a new {@link Processing} {@link Target} for reducing query
//     * terms.
//     *
//     * @param newMetrics Metrics instance
//     * @param minDocFreq Minimum document frequency
//     * @param maxDocFreq Maximum document frequency
//     * @param reducedFbTerms Target for reduced terms
//     */
//    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
//    FbTermReducerTarget(
//        final Metrics.CollectionMetrics newMetrics,
//        final int minDocFreq, final int maxDocFreq,
//        final Queue<ByteArray> reducedFbTerms) {
//      assert newMetrics != null;
//      assert reducedFbTerms != null;
//
//      this.reducedTermsTarget = reducedFbTerms;
//      this.minDf = minDocFreq;
//      this.maxDf = maxDocFreq;
//      this.collectionMetrics = newMetrics;
//    }
//
//    @Override
//    public void call(final Map.Entry<ByteArray, Long> termEntry) {
//      if (termEntry != null) {
//        final int df = this.collectionMetrics.df(termEntry.getKey());
//        // check, if term meets the document-frequency value requirement
//        if (df >= this.minDf && df <= this.maxDf) {
//          // add term 'frequency' times to the target collection
//          for (long cnt = 1L; cnt <= termEntry.getValue(); cnt++) {
//            this.reducedTermsTarget.add(termEntry.getKey());
//          }
//        }
//      }
//    }
//  }

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
     * Configuration prefix.
     */
    private static final String CONF_PREFIX = ImprovedClarityScore.IDENTIFIER
        + "-result";

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
    }

    /**
     * Get the collection of feedback documents used for calculation.
     *
     * @return Feedback documents used for calculation
     */
    @SuppressWarnings("TypeMayBeWeakened")
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
      extends TargetFuncCall.TargetFunc<Map.Entry<ByteArray, Long>> {
    /**
     * Final score to add calculation results to.
     */
    private final AtomicDouble score;
    private final Metrics.CollectionMetrics cMetrics = ImprovedClarityScore
        .this.metrics.collection();
    private final double minDf;
    private final double maxDf;

    /**
     * Create a new calculator for document models.
     *
     * @param result Result to add to
     */
    ModelCalculatorTarget(final AtomicDouble result,
        final double newMinDf, final double newMaxDf) {
      this.score = Objects.requireNonNull(result,
          "Calculation result target was null.");
      this.minDf = newMinDf;
      this.maxDf = newMaxDf;
    }

    @Override
    public void call(final Map.Entry<ByteArray, Long> termEntry) {
      if (termEntry != null) {
        final int df = this.cMetrics.df(termEntry.getKey());
        // check, if term meets the document-frequency value requirement
        if (df >= this.minDf && df <= this.maxDf) {
          final double queryModel = getQueryModel(termEntry.getKey());
          if (queryModel <= 0d) {
            LOG.warn("Query model <= 0 ({}) for term '{}'.", queryModel,
                ByteArrayUtils.utf8ToString(termEntry.getKey()));
          } else {
            double value = queryModel * MathUtils.log2(queryModel
                / this.cMetrics.relTf(termEntry.getKey()));
            this.score.addAndGet(value * termEntry.getValue());
          }
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
//     * Constructor to allow parent class access.
//     */
//    DocumentModelCalculatorTarget() {
//    }
//
//    @Override
//    public void call(final Integer docId) {
//      if (docId != null) {
//        // call the calculation method of the main class for each
//        // document and term that is available for processing
//        getDocumentModel(docId);
//      }
//    }
//  }
}
