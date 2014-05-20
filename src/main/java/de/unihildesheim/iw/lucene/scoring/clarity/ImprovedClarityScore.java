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
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.AtomicDouble;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.mapdb.Atomic;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
 * Baeza-Yates.
 * <p/>
 * Reference
 * <p/>
 * Hauff, Claudia, Vanessa Murdock, and Ricardo Baeza-Yates. “Improved Query
 * Difficulty Prediction for the Web.” In Proceedings of the 17th ACM Conference
 * on Information and Knowledge Management, 439–448. CIKM ’08. New York, NY,
 * USA: ACM, 2008. doi:10.1145/1458082.1458142.
 *
 * @author Jens Bertram
 */
public final class ImprovedClarityScore
    implements ClarityScoreCalculation {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      ImprovedClarityScore.class);
  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link de.unihildesheim.iw.lucene.index
   * .IndexDataProvider}.
   */
  static final String IDENTIFIER = "ICS";

  /**
   * {@link IndexDataProvider} to use.
   */
  private IndexDataProvider dataProv;

  /**
   * Reader to access the Lucene index.
   */
  private IndexReader idxReader;

  /**
   * Configuration object used for all parameters of the calculation.
   */
  private ImprovedClarityScoreConfiguration.Conf conf;

  /**
   * Database instance.
   */
  private DB db;
  /**
   * Manager for extended document meta-data.
   */
  private ExternalDocTermDataManager extDocMan;

  /**
   * Flag indicating, if a cache is available.
   */
  private boolean hasCache;

  /**
   * Cache of default document models.
   */
  private Map<Fun.Tuple2<Integer, ByteArray>, Double> defaultDocModels;

  /**
   * Utility working with queries.
   */
  private QueryUtils queryUtils;

  /**
   * Policy to use to simplify a query, if no document matches all terms in the
   * initial query.
   * <p/>
   * If multiple terms match the same criteria a random one out of those will be
   * chosen.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum QuerySimplifyPolicy {

    /**
     * Removes the first term.
     */
    FIRST,
    /**
     * Removes the term with the highest document-frequency.
     */
    HIGHEST_DOCFREQ,
    /**
     * Removes the term with the highest index-frequency.
     */
    HIGHEST_TERMFREQ,
    /**
     * Removes the last term.
     */
    LAST,
    /**
     * Removes a randomly chosen term.
     */
    RANDOM
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

  private enum DataKeys {

    /**
     * Document-models.
     */
    DM
  }

  /**
   * Cached storage of Document-id -> Term, model-value.
   */
  private Map<Integer, Map<ByteArray, Double>> docModelDataCache;

  /**
   * Provider for general index metrics.
   */
  Metrics metrics;

  /**
   * Default constructor. Called from builder.
   */
  private ImprovedClarityScore() {
    super();
  }

  /**
   * Builder method to create a new instance.
   *
   * @param builder Builder to use for constructing the instance
   * @return New instance
   */
  protected static ImprovedClarityScore build(
      final Builder builder)
      throws Buildable.BuildableException, IOException {
    Objects.requireNonNull(builder, "Builder was null.");

    final ImprovedClarityScore instance = new ImprovedClarityScore();
    // set configuration
    instance.dataProv = builder.idxDataProvider;
    instance.idxReader = builder.idxReader;
    instance.metrics = new Metrics(builder.idxDataProvider);
    instance.setConfiguration(builder.configuration);

    // initialize
    instance.queryUtils =
        new QueryUtils(builder.idxReader, builder.idxDataProvider
            .getDocumentFields());

    instance.initCache(builder);

    return instance;
  }

  /**
   * Initializes a cache.
   */
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

    final Double smoothing = this.conf.smoothing;
    final Double lambda = this.conf.lambda;
    final Double beta = this.conf.beta;

    if (!createNew) {
      if (this.dataProv.getLastIndexCommitGeneration() == null) {
        LOG.warn("Index commit generation not available. Assuming an " +
            "unchanged index!");
      } else {
        if (!persistence.getMetaData()
            .generationCurrent(this.dataProv.getLastIndexCommitGeneration())) {
          throw new IllegalStateException(
              "Index changed since last caching.");
        }
      }
      if (!this.db.getAtomicString(Caches.SMOOTHING.name()).get().equals(
          smoothing.toString())) {
        throw new IllegalStateException(
            "Different smoothing parameter value used in cache.");
      }
      if (!this.db.getAtomicString(Caches.LAMBDA.name()).get().equals(
          lambda.toString())) {
        throw new IllegalStateException(
            "Different lambda parameter value used in cache.");
      }
      if (!this.db.getAtomicString(Caches.BETA.name()).get().equals(
          beta.toString())) {
        throw new IllegalStateException(
            "Different beta parameter value used in cache.");
      }
      if (!persistence.getMetaData()
          .fieldsCurrent(this.dataProv.getDocumentFields())) {
        throw new IllegalStateException(
            "Current fields are different from cached ones.");
      }
      if (!persistence.getMetaData()
          .stopWordsCurrent(this.dataProv.getStopwords())) {
        throw new IllegalStateException(
            "Current stopwords are different from cached ones.");
      }
    } else {
      this.db.
          createAtomicString(Caches.SMOOTHING.name(), smoothing.toString());
      this.db.createAtomicString(Caches.LAMBDA.name(), lambda.toString());
      this.db.createAtomicString(Caches.BETA.name(), beta.toString());
      persistence.updateMetaData(this.dataProv.getDocumentFields(),
          this.dataProv.getStopwords());
    }

    this.defaultDocModels = this.db
        .createTreeMap(Caches.DEFAULT_DOC_MODELS.name())
        .keySerializer(BTreeKeySerializer.TUPLE2)
        .valueSerializer(Serializer.BASIC)
        .makeOrGet();

    this.extDocMan = new ExternalDocTermDataManager(this.db, IDENTIFIER);
    this.hasCache = true;
  }

  /**
   * Set the configuration to use by this instance.
   *
   * @param newConf Configuration
   */
  private ImprovedClarityScore setConfiguration(
      final ImprovedClarityScoreConfiguration newConf) {
    this.conf = newConf.compile();
    newConf.debugDump();
    this.docModelDataCache = new ConcurrentHashMap<>(this.conf.fbMax);
    parseConfig();
    return this;
  }

  /**
   * Parse the configuration and do some simple pre-checks.
   */
  private void parseConfig() {
    if (this.conf.fbMin > this.metrics.collection.numberOfDocuments()) {
      throw new IllegalStateException(
          "Required minimum number of feedback documents ("
              + this.conf.fbMin + ") is larger "
              + "or equal compared to the total amount of indexed documents "
              + "(" + this.metrics.collection.numberOfDocuments()
              + "). Unable to provide feedback."
      );
    }
    this.conf.debugDump();
  }

  /**
   * Reduce the query by removing a term based on a specific policy.
   *
   * @param query Query to reduce
   * @param policy Policy to use for choosing which term to remove
   * @return Simplified query string
   * @throws ParseException Thrown, if query string could not be parsed
   * @throws IOException Thrown on low-level i/O errors or if a term could not
   * be parsed to UTF-8
   */
  private String simplifyQuery(final String query,
      final QuerySimplifyPolicy policy)
      throws IOException, ParseException,
             Buildable.ConfigurationException, Buildable.BuildException {
    Collection<ByteArray> qTerms = new ArrayList<>(this.queryUtils.
        getAllQueryTerms(query));
    ByteArray termToRemove = null;
    if (new HashSet<>(qTerms).size() == 1) {
      LOG.debug("Return empty string from one term query.");
      return "";
    }

    switch (policy) {
      case FIRST:
        termToRemove = ((List<ByteArray>) qTerms).get(0);
        break;
      case HIGHEST_DOCFREQ:
        long docFreq = 0;
        qTerms = new HashSet<>(qTerms);
        for (final ByteArray term : qTerms) {
          final long tDocFreq = this.metrics.collection.df(term);
          if (tDocFreq > docFreq) {
            termToRemove = term;
            docFreq = tDocFreq;
          } else if (tDocFreq == docFreq && RandomValue.getBoolean()) {
            termToRemove = term;
          }
        }
        break;
      case HIGHEST_TERMFREQ:
        long collFreq = 0;
        qTerms = new HashSet<>(qTerms);
        for (final ByteArray term : qTerms) {
          final long tCollFreq = this.metrics.collection.tf(term);
          if (tCollFreq > collFreq) {
            termToRemove = term;
            collFreq = tCollFreq;
          } else if (tCollFreq == collFreq && RandomValue.getBoolean()) {
            termToRemove = term;
          }
        }
        break;
      case LAST:
        termToRemove = ((List<ByteArray>) qTerms).get(qTerms.size() - 1);
        break;
      case RANDOM:
        final int idx = RandomValue.getInteger(0, qTerms.size() - 1);
        termToRemove = ((List<ByteArray>) qTerms).get(idx);
        break;
    }

    while (qTerms.contains(termToRemove)) {
      qTerms.remove(termToRemove);
    }

    final StringBuilder sb = new StringBuilder(100);
    for (final ByteArray qTerm : qTerms) {
      sb.append(ByteArrayUtils.utf8ToString(qTerm)).append(' ');
    }

    LOG.debug("Remove term={} policy={} oldQ={} newQ={}", ByteArrayUtils.
        utf8ToString(termToRemove), policy, query, sb.toString().
        trim());
    return sb.toString().trim();
  }

  /**
   * Safe method to get the document model value for a document term. This
   * method will calculate an additional value, if the term in question is not
   * contained in the document.
   * <p/>
   * A call to this method should only needed, if {@code lambda} in {@link
   * #conf} is lesser than {@code 1}.
   *
   * @param docId Id of the document whose model to get
   * @param term Term to calculate the model for
   * @return Model value for document & term
   */
  double getDocumentModel(final int docId, final ByteArray term) {
    assert term != null;

    Double model = getDocumentModel(docId).get(term);
    // if term not in document, calculate new value
    if (model == null) {
      // relative collection frequency of the term
      final double termRelIdxFreq = this.metrics.collection.relTf(term);
      final DocumentModel docModel = this.metrics.getDocumentModel(docId);
      final double docTermFreq = docModel.metrics().tf().doubleValue();
      final double termsInDoc =
          docModel.metrics().uniqueTermCount().doubleValue();

      final double smoothedTerm =
          (this.conf.smoothing * termRelIdxFreq) /
              (docTermFreq + (this.conf.smoothing * termsInDoc));
      model =
          (this.conf.lambda *
              ((this.conf.beta * smoothedTerm) +
                  ((1d - this.conf.beta) * termRelIdxFreq))
          ) + ((1d - this.conf.lambda) * termRelIdxFreq);
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

      // build mapping, if needed
      if (map == null || map.isEmpty()) {
        final DocumentModel docModel = this.metrics.getDocumentModel(docId);
        final double docTermFreq = docModel.metrics().tf().doubleValue();
        final double termsInDoc =
            docModel.metrics().uniqueTermCount().doubleValue();
        final Set<ByteArray> terms = docModel.termFreqMap.keySet();

        map = new HashMap<>(terms.size());
        for (final ByteArray term : terms) {
          // relative collection frequency of the term
          final double termRelIdxFreq = this.metrics.collection.relTf(term);
          // term frequency given the document
          final double termInDocFreq =
              docModel.metrics().tf(term).doubleValue();

          final double smoothedTerm =
              (termInDocFreq + (this.conf.smoothing * termRelIdxFreq)) /
                  (docTermFreq + (this.conf.smoothing * termsInDoc));
          final double value =
              (this.conf.lambda *
                  ((this.conf.beta * smoothedTerm) +
                      ((1d - this.conf.beta) * termRelIdxFreq))
              ) + ((1d - this.conf.lambda) * termRelIdxFreq);
          map.put(term, value);
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
   * Calculate the query model.
   *
   * @param fbTerm Feedback term
   * @param qTerms List of query terms
   * @param fbDocIds List of feedback document
   * @return Query model for the current term and set of feedback documents
   */
  double getQueryModel(final ByteArray fbTerm,
      final List<ByteArray> qTerms,
      final Set<Integer> fbDocIds) {
    assert fbTerm != null;
    assert qTerms != null && !qTerms.isEmpty();
    assert fbDocIds != null && !fbDocIds.isEmpty();

    Objects.requireNonNull(fbTerm);
    if (Objects.requireNonNull(fbDocIds).isEmpty()) {
      throw new IllegalArgumentException("List of feedback document ids was " +
          "empty.");
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
      }
      model += docModel * docModelQtProduct;
    }
    return model;
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
    if (Objects.requireNonNull(query, "Query was null.").trim().isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // result object
    final Result result = new Result(this.getClass());
    // final clarity score
    final AtomicDouble score = new AtomicDouble(0);
    // collection of feedback document ids
    Set<Integer> feedbackDocIds;
    // save base data to result object
    result.addQuery(query);
    result.setConf(this.conf);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // run a query to get feedback
    final TermsQueryBuilder termsQueryBuilder =
        new TermsQueryBuilder(this.idxReader, this.dataProv.getDocumentFields
            ()).setBoolOperator(QueryParser.Operator.AND);

    Query queryObj;
    try {
      queryObj = termsQueryBuilder.query(query).build();
    } catch (Buildable.BuildableException e) {
      throw new ClarityScoreCalculationException(
          "Caught exception while building query.", e);
    }

    feedbackDocIds = new HashSet<>(this.conf.fbMax);

    try {
      feedbackDocIds.addAll(Feedback.get(this.idxReader, queryObj,
          this.conf.fbMax));
    } catch (IOException e) {
      throw new ClarityScoreCalculationException("Error while retrieving " +
          "feedback documents.", e);
    }

    // simplify query, if not enough feedback documents are available
    String simplifiedQuery = query;
    int docsToGet;
    while (feedbackDocIds.size() < this.conf.fbMin) {
      // set flag indicating we simplified the query
      result.setQuerySimplified(true);
      LOG.info("Minimum number of feedback documents not reached "
              + "({}/{}). Simplifying query using {} policy.",
          feedbackDocIds.size(), this.conf.fbMin, this.conf.policy
      );

      try {
        simplifiedQuery = simplifyQuery(simplifiedQuery, this.conf.policy);
      } catch (IOException | ParseException | Buildable.BuildableException
          e) {
        throw new ClarityScoreCalculationException("Error while trying to " +
            "simplify query.", e);
      }

      if (simplifiedQuery.isEmpty()) {
        throw new NoTermsLeftException(
            "No query terms left while trying to reach the minimum number of" +
                " feedback documents."
        );
      }
      result.addQuery(simplifiedQuery);
      docsToGet = this.conf.fbMax - feedbackDocIds.size();

      try {
        queryObj = termsQueryBuilder.query(simplifiedQuery).build();
      } catch (Buildable.BuildableException e) {
        throw new ClarityScoreCalculationException(
            "Error while building query.", e);
      }

      try {
        feedbackDocIds.addAll(Feedback.get(this.idxReader, queryObj,
            docsToGet));
      } catch (IOException e) {
        throw new ClarityScoreCalculationException("Error while retrieving " +
            "feedback documents.", e);
      }
    }

    // collect all unique terms from feedback documents
    final Set<ByteArray> fbTerms = this.dataProv.getDocumentsTermSet(
        feedbackDocIds);

    // get document frequency threshold
    int minDf = (int) (this.metrics.collection.numberOfDocuments()
        * this.conf.threshold);
    if (minDf <= 0) {
      LOG.debug("Document frequency threshold was {} setting to 1", minDf);
      minDf = 1;
    }
    LOG.debug("Document frequency threshold is {} = {}", minDf,
        this.conf.threshold
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
              new FbTermReducerTarget(this.metrics.collection, minDf,
                  reducedFbTerms)
          )
      ).process(fbTerms.size());
    } catch (ProcessingException e) {
      throw new ClarityScoreCalculationException("Failed to reduce terms by " +
          "threshold.", e);
    }
    LOG.debug("Reduced term set size {}", reducedFbTerms.size());

    if (reducedFbTerms.isEmpty()) {
      throw new NoTermsLeftException(
          "No terms remaining after elimination based " +
              "on document frequency threshold (" + minDf + ")."
      );
    }

    // do the final calculation for all remaining feedback terms
    LOG.debug("Using {} feedback documents for calculation.",
        feedbackDocIds.size());
    try {
      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(reducedFbTerms),
              new ModelCalculatorTarget(feedbackDocIds,
                  this.queryUtils.getAllQueryTerms(query), score)
          )
      ).process(reducedFbTerms.size());
    } catch (UnsupportedEncodingException | ParseException | Buildable
        .BuildableException | ProcessingException e) {
      throw new ClarityScoreCalculationException(e);
    }

    result.setScore(score.get());
    result.setFeedbackDocIds(feedbackDocIds);
    result.setFeedbackTerms(new HashSet<>(reducedFbTerms));

    timeMeasure.stop();
    LOG.debug("Calculating improved clarity score for query {} "
            + "with {} document models and {} terms took {}. {}", query,
        feedbackDocIds.size(), fbTerms.size(), timeMeasure.
            getTimeString(), score
    );

    return result;
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   */
  public void preCalcDocumentModels()
      throws ProcessingException {
    if (!this.hasCache) {
      LOG.warn("Won't pre-calculate any values. Cache not set.");
    }
    final Atomic.Boolean hasData = this.db.getAtomicBoolean(
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
      ).process(this.metrics.collection.numberOfDocuments().intValue());
      hasData.set(true);
    }
  }

  /**
   * {@link Processing} {@link Target} to reduce feedback terms.
   */
  final static class FbTermReducerTarget
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
     * Access collection metrics.
     */
    private final Metrics.CollectionMetrics collectionMetrics;

    /**
     * Creates a new {@link Processing} {@link Target} for reducing query
     * terms.
     *
     * @param minDocFreq Minimum document frequency
     * @param reducedFbTerms Target for reduced terms
     */
    FbTermReducerTarget(
        final Metrics.CollectionMetrics metrics,
        final int minDocFreq,
        final Queue<ByteArray> reducedFbTerms) {
      super();
      assert metrics != null;
      assert reducedFbTerms != null;

      this.reducedTermsTarget = reducedFbTerms;
      this.minDf = minDocFreq;
      this.collectionMetrics = metrics;
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null && (this.collectionMetrics.df(term) >= this.minDf)) {
        this.reducedTermsTarget.add(term);
      }
    }
  }

  /**
   * {@link Processing} {@link Target} to calculate document models.
   */
  private final class ModelCalculatorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    /**
     * Query terms. Must be non-unique!
     */
    private final List<ByteArray> queryTerms;
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
     * @param fbDocIds Feedback document ids
     * @param qTerms Query terms
     * @param result Result to add to
     */
    ModelCalculatorTarget(final Set<Integer> fbDocIds,
        final List<ByteArray> qTerms, final AtomicDouble result) {
      super();
      assert fbDocIds != null : "List of feedback document ids was null.";
      assert qTerms != null : "List of query terms was null.";
      assert result != null : "Calculation result target was null.";

      this.queryTerms = qTerms;
      this.feedbackDocIds = fbDocIds;
      this.score = result;
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        final double queryModel = getQueryModel(term, this.queryTerms,
            this.feedbackDocIds);
        score.addAndGet(queryModel * MathUtils.log2(queryModel
            / metrics.collection.relTf(term)));
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for document model creation.
   */
  private final class DocumentModelCalculatorTarget
      extends TargetFuncCall.TargetFunc<Integer> {

    @Override
    public void call(final Integer docId) {
      if (docId != null) {
        // call the calculation method of the main class for each
        // document and term that is available for processing
        getDocumentModel(docId);
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
     * Configuration that was used.
     */
    private ImprovedClarityScoreConfiguration.Conf conf;
    /**
     * Ids of feedback documents used for calculation.
     */
    private Set<Integer> feedbackDocIds;
    /**
     * Terms from feedback documents used for calculation.
     */
    private Set<ByteArray> feedbackTerms;
    /**
     * Flag indicating, if the query was simplified.
     */
    private boolean wasQuerySimplified;
    /**
     * List of queries issued to get feedback documents.
     */
    private final List<String> queries;

    /**
     * Creates an object wrapping the result with meta information.
     *
     * @param cscType Type of the calculation class
     */
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public Result(final Class<? extends ClarityScoreCalculation> cscType) {
      super(cscType);
      this.queries = new ArrayList<>();
      this.feedbackDocIds = Collections.<Integer>emptySet();
      this.feedbackTerms = Collections.<ByteArray>emptySet();
    }

    /**
     * Set the calculation result.
     *
     * @param score Score result
     */
    void setScore(final double score) {
      super._setScore(score);
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
     * Set the list of feedback terms used.
     *
     * @param fbTerms List of feedback terms
     */
    void setFeedbackTerms(final Set<ByteArray> fbTerms) {
      this.feedbackTerms = Collections.unmodifiableSet(Objects
          .requireNonNull(fbTerms, "Feedback terms were null"));
    }

    /**
     * Set the flag, if this query was simplified.
     *
     * @param state True, if simplified
     */
    void setQuerySimplified(final boolean state) {
      this.wasQuerySimplified = state;
    }

    /**
     * Set the configuration that was used.
     *
     * @param newConf Configuration used
     */
    void setConf(final ImprovedClarityScoreConfiguration.Conf newConf) {
      this.conf = Objects.requireNonNull(newConf, "Configuration was null.");
    }

    /**
     * Add a query string to the list of issued queries.
     *
     * @param query Query to add
     */
    void addQuery(final String query) {
      if (Objects.requireNonNull(query, "Query was null.").trim().isEmpty()) {
        throw new IllegalArgumentException("Query was empty.");
      }
      this.queries.add(query);
    }

    /**
     * Get the configuration used for this calculation result.
     *
     * @return Configuration used for this calculation result
     */
    public ImprovedClarityScoreConfiguration.Conf getConfiguration() {
      return this.conf;
    }

    /**
     * Get the collection of feedback documents used for calculation.
     *
     * @return Feedback documents used for calculation
     */
    public Set<Integer> getFeedbackDocuments() {
      return this.feedbackDocIds;
    }

    /**
     * Get the collection of feedback terms used for calculation.
     *
     * @return Feedback terms used for calculation
     */
    public Set<ByteArray> getFeedbackTerms() {
      return this.feedbackTerms;
    }

    /**
     * Get the flag indicating, if the query was simplified.
     *
     * @return True, if it was simplified
     */
    public boolean wasQuerySimplified() {
      return this.wasQuerySimplified;
    }

    /**
     * Get the queries issued to get feedback documents.
     *
     * @return List of queries issued
     */
    public List<String> getQueries() {
      return Collections.unmodifiableList(this.queries);
    }
  }

  /**
   * Builder to create a new {@link ImprovedClarityScore} instance.
   */
  public static final class Builder
      extends AbstractClarityScoreCalculationBuilder<Builder> {
    /**
     * Configuration to use.
     */
    ImprovedClarityScoreConfiguration configuration = new
        ImprovedClarityScoreConfiguration();

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
    public Builder configuration(
        final ImprovedClarityScoreConfiguration conf) {
      this.configuration = Objects.requireNonNull(conf,
          "Configuration was null.");
      return this;
    }

    @Override
    public ImprovedClarityScore build()
        throws BuildableException {
      validate();
      try {
        return ImprovedClarityScore.build(this);
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

  /**
   * Base class for {@link ImprovedClarityScore} specific {@link Exception}s.
   */
  public static class ImprovedClarityScoreCalculationException
      extends ClarityScoreCalculationException {

    public ImprovedClarityScoreCalculationException(final String msg) {
      super(msg);
    }
  }

  /**
   * {@link Exception} indicating that there are no terms left to run a scoring
   * query.
   */
  public final static class NoTermsLeftException
      extends ImprovedClarityScoreCalculationException {
    public NoTermsLeftException(final String msg) {
      super(msg);
    }
  }
}
