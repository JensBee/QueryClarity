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
package de.unihildesheim.lucene.scoring.clarity.impl;

import de.unihildesheim.ByteArray;
import de.unihildesheim.Persistence;
import de.unihildesheim.SupportsPersistence;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.index.ExternalDocTermDataManager;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.metrics.DocumentMetrics;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.query.TermsQueryBuilder;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.Configuration;
import de.unihildesheim.util.MathUtils;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.concurrent.AtomicDouble;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.mapdb.Atomic;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.LoggerFactory;

/**
 * Improved Clarity Score implementation as described by Hauff, Murdock,
 * Baeza-Yates.
 * <p>
 * Reference
 * <p>
 * Hauff, Claudia, Vanessa Murdock, and Ricardo Baeza-Yates. “Improved Query
 * Difficulty Prediction for the Web.” In Proceedings of the 17th ACM
 * Conference on Information and Knowledge Management, 439–448. CIKM ’08. New
 * York, NY, USA: ACM, 2008. doi:10.1145/1458082.1458142.
 *
 * @author Jens Bertram
 */
public final class ImprovedClarityScore implements ClarityScoreCalculation,
        SupportsPersistence {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          ImprovedClarityScore.class);
  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link DataProvider}.
   */
  static final String IDENTIFIER = "ICS";

  /**
   * Configuration object used for all parameters of the calculation.
   */
  private ImprovedClarityScoreConfiguration conf;

  /**
   * Wrapper for persistent data storage.
   */
  private Persistence pData;
  private DB db;
  private ExternalDocTermDataManager extDocMan;
  private boolean hasCache = false;
  private boolean cacheTemporary = false;
  private Map<Fun.Tuple2<Integer, ByteArray>, Double> defaultDocModels;

  /**
   * Policy to use to simplify a query, if no document matches all terms in
   * the initial query.
   * <p>
   * If multiple terms match the same criteria a random one out of those will
   * be chosen.
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
    RANDOM;
  }

  /**
   * Ids of temporary data caches held in the database.
   */
  private enum Caches {

    SMOOTHING,
    LAMBDA,
    BETA,
    HAS_PRECALC_DATA,
    DEFAULT_DOC_MODELS
  }

  private enum DataKeys {

    DM
  }

  /**
   * Cached storage of Document-id -> Term, model-value.
   */
  private Map<Integer, Map<ByteArray, Object>> docModelDataCache;

  /**
   * Create a new scoring instance with the default parameter set.
   */
  public ImprovedClarityScore() {
    this(new ImprovedClarityScoreConfiguration());
  }

  /**
   * Create a new scoring instance with the parameters set in the given
   * configuration.
   *
   * @param newConf Configuration
   */
  public ImprovedClarityScore(final Configuration newConf) {
    super();
    setConfiguration(newConf);
  }

  @Override
  public void loadOrCreateCache(final String name) throws IOException,
          Environment.NoIndexException {
    initCache(name, false, true);
  }

  @Override
  public void createCache(final String name) throws IOException,
          Environment.NoIndexException {
    initCache(name, true, true);
  }

  @Override
  public void loadCache(final String name) throws IOException,
          Environment.NoIndexException {
    initCache(name, false, false);
  }

  protected ExternalDocTermDataManager testGetExtDocMan() {
    return this.extDocMan;
  }

  /**
   *
   * @param name
   * @param createNew
   * @param createIfNeeded
   * @throws IOException
   * @throws de.unihildesheim.lucene.Environment.NoIndexException Thrown, if
   * no index is provided in the {@link Environment}
   */
  private void initCache(final String name, boolean createNew,
          final boolean createIfNeeded) throws IOException,
          Environment.NoIndexException {
    final Persistence.Builder psb;
    if (Environment.isTestRun() || this.cacheTemporary) {
      psb = new Persistence.Builder(IDENTIFIER
              + "_" + name + "_" + RandomValue.getString(6)).temporary();
    } else {
      psb = new Persistence.Builder(IDENTIFIER + "_" + name);
    }
    psb.setDbDefaults();

    if (!psb.exists() && createIfNeeded) {
      createNew = true;
    }

    if (createNew) {
      this.pData = psb.make();
    } else if (!createIfNeeded) {
      this.pData = psb.get();
    } else {
      this.pData = psb.makeOrGet();
    }
    this.db = this.pData.db;

    final Double smoothing = this.conf.getDocumentModelSmoothingParameter();
    final Double lambda = this.conf.getDocumentModelParamLambda();
    final Double beta = this.conf.getDocumentModelParamBeta();

    if (!createNew) {
      if (!this.pData.getMetaData().generationCurrent()) {
        throw new IllegalStateException(
                "Index changed since last caching.");
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
      if (!this.pData.getMetaData().fieldsCurrent()) {
        throw new IllegalStateException(
                "Current fields are different from cached ones.");
      }
      if (!this.pData.getMetaData().stopWordsCurrent()) {
        throw new IllegalStateException(
                "Current stopwords are different from cached ones.");
      }
    } else {
      this.db.
              createAtomicString(Caches.SMOOTHING.name(), smoothing.toString());
      this.db.createAtomicString(Caches.LAMBDA.name(), lambda.toString());
      this.db.createAtomicString(Caches.BETA.name(), beta.toString());
      this.pData.updateMetaData();
    }

    this.defaultDocModels = this.db
            .createTreeMap(Caches.DEFAULT_DOC_MODELS.name())
            .keySerializer(BTreeKeySerializer.TUPLE2)
            .valueSerializer(Serializer.BASIC)
            .makeOrGet();

    this.extDocMan = new ExternalDocTermDataManager(this.db, IDENTIFIER);
    this.hasCache = true;
  }

  @Override
  public final ImprovedClarityScore setConfiguration(
          final Configuration newConf) {
    if (!(newConf instanceof ImprovedClarityScoreConfiguration)) {
      throw new IllegalArgumentException("Wrong configuration type.");
    }
    this.conf = (ImprovedClarityScoreConfiguration) newConf;
    this.conf.debugDump();
    this.docModelDataCache = new ConcurrentHashMap<>(this.conf.
            getMaxFeedbackDocumentsCount());
    parseConfig();
    return this;
  }

  /**
   * Parse the configuration and do some simple pre-checks.
   */
  private void parseConfig() {
    if (this.conf.getMinFeedbackDocumentsCount() > CollectionMetrics.
            numberOfDocuments()) {
      throw new IllegalStateException(
              "Required minimum number of feedback documents ("
              + this.conf.getMinFeedbackDocumentsCount() + ") is larger "
              + "or equal compared to the total amount of indexed documents "
              + "(" + CollectionMetrics.numberOfDocuments()
              + "). Unable to provide feedback.");
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
          final QuerySimplifyPolicy policy) throws IOException, ParseException {
    Collection<ByteArray> qTerms = new ArrayList<>(QueryUtils.
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
        for (ByteArray term : qTerms) {
          final long tDocFreq = CollectionMetrics.df(term);
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
        for (ByteArray term : qTerms) {
          final long tCollFreq = CollectionMetrics.tf(term);
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
    for (ByteArray qTerm : qTerms) {
      sb.append(ByteArrayUtil.utf8ToString(qTerm)).append(' ');
    }

    LOG.debug("Remove term={} policy={} oldQ={} newQ={}", ByteArrayUtil.
            utf8ToString(termToRemove), policy, query, sb.toString().
            trim());
    return sb.toString().trim();
  }

  private double calcDefaultDocumentModel(final DocumentModel docModel,
          final ByteArray term) {
    Double model;
    model = this.defaultDocModels.get(Fun.t2(docModel.id, term));
    if (model == null) {
      final DocumentMetrics dom = new DocumentMetrics(docModel);
      final double smoothing = this.conf.getDocumentModelSmoothingParameter();
      final double uniqueTerms = dom.uniqueTermCount().doubleValue();
      final double lambda = this.conf.getDocumentModelParamLambda();
      final double beta = this.conf.getDocumentModelParamBeta();
      final double totalFreq = dom.tf().doubleValue();
      final double rCollFreq = CollectionMetrics.relTf(term);
      final double termFreq = 0d;

      model = (termFreq + (smoothing * rCollFreq)) / (totalFreq
              + (smoothing * uniqueTerms));
      model = (lambda * ((beta * model) + ((1 - beta) * rCollFreq))) + ((1
              - lambda) * rCollFreq);

      this.defaultDocModels.put(Fun.t2(docModel.id, term.clone()), model);
    }
    return model;
  }

  private void calcDocumentModel(final DocumentModel docModel) {
    final double smoothing = this.conf.getDocumentModelSmoothingParameter();
    final double lambda = this.conf.getDocumentModelParamLambda();
    final double beta = this.conf.getDocumentModelParamBeta();
    final DocumentMetrics dom = new DocumentMetrics(docModel);
    final double totalFreq = dom.tf().doubleValue();
    final double uniqueTerms = dom.uniqueTermCount().doubleValue();

    for (ByteArray term : docModel.termFreqMap.keySet()) {
      // term frequency given the document
      final double termFreq = dom.tf(term).doubleValue();
      // relative collection frequency of the term
      final double rCollFreq = CollectionMetrics.relTf(term);

      double model = (termFreq + (smoothing * rCollFreq)) / (totalFreq
              + (smoothing * uniqueTerms));
      model = (lambda * ((beta * model) + ((1 - beta) * rCollFreq))) + ((1
              - lambda) * rCollFreq);

      this.extDocMan.setData(docModel.id, term.clone(), DataKeys.DM.name(),
              model);
    }
  }

  /**
   * Calculate the document model for a given term. The document model is
   * calculated using Bayesian smoothing using Dirichlet priors.
   *
   * @param dm Document id
   * @param term Term to calculate the model for
   * @return Calculated document model given the term
   */
  private double getDocumentModel(final DocumentModel dm,
          final ByteArray term) {
    Double model;
    if (this.docModelDataCache.containsKey(dm.id)) {
      model = (Double) this.docModelDataCache.get(dm.id).get(term);
    } else {
      Map<ByteArray, Object> td = this.extDocMan.getData(dm.id, DataKeys.DM.
              name());
      if (td == null || td.isEmpty()) {
        calcDocumentModel(DocumentMetrics.getModel(dm.id));
        td = this.extDocMan.getData(dm.id, DataKeys.DM.name());
      }
      model = (Double) td.get(term);
      this.docModelDataCache.put(dm.id, td);
    }

    if (model == null) {
      model = calcDefaultDocumentModel(dm, term);
    }
    return model;
  }

  /**
   * Calculate the query model.
   *
   * @param fbTerm Feedback term
   * @param qTerms List of query terms
   * @param fbDocIds List of feedback document
   * @return Query model for the current term and set of feedback documents
   * @throws UnsupportedEncodingException Thrown, if a query term could not be
   * parsed
   */
  protected final double calcQueryModel(final ByteArray fbTerm,
          final Collection<ByteArray> qTerms,
          final Collection<Integer> fbDocIds) throws
          UnsupportedEncodingException {
    double model = 0d;

    for (Integer fbDocId : fbDocIds) {
      final DocumentModel dm = DocumentMetrics.getModel(fbDocId);
      // document model for the given term pD(t)
      final double docModel = getDocumentModel(dm, fbTerm);
      // calculate the product of the document models for all query terms
      // given the current document
      double docModelQtProduct = 1d;
      for (ByteArray qTerm : qTerms) {
        docModelQtProduct *= getDocumentModel(dm, qTerm);
      }
      model += docModel * docModelQtProduct;
    }
    return model;
  }

  /**
   *
   * @param query
   * @return
   * @throws ParseException
   * @throws de.unihildesheim.lucene.Environment.NoIndexException Thrown, if
   * no index is provided in the {@link Environment}
   */
  @Override
  public final Result calculateClarity(final String query) throws
          ParseException, Environment.NoIndexException {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }
    if (!this.hasCache) {
      this.cacheTemporary = true;
      try {
        initCache("temp", true, true);
      } catch (IOException ex) {
        throw new IllegalStateException("Error creating cache.", ex);
      }
    }

    // result object
    final Result result = new Result(this.getClass());
    // final clarity score
    final AtomicDouble score = new AtomicDouble(0);
    // collection of feedback document ids
    Collection<Integer> feedbackDocIds;
    // save base data to result object
    result.addQuery(query);
    result.setConf(this.conf);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // run a query to get feedback
    TermsQueryBuilder qBuilder = new TermsQueryBuilder().setBoolOperator(
            QueryParser.Operator.AND);
    Query queryObj = qBuilder.buildUsingEnvironment(query);
    try {
      feedbackDocIds = new HashSet<>(this.conf.getMaxFeedbackDocumentsCount());
      feedbackDocIds.addAll(Feedback.get(queryObj, this.conf.
              getMaxFeedbackDocumentsCount()));

      // simplify query, if not enough feedback documents are available
      String simplifiedQuery = query;
      int docsToGet;
      while (feedbackDocIds.size() < this.conf.getMinFeedbackDocumentsCount()) {
        // set flag indicating we simplified the query
        result.setQuerySimplified(true);
        LOG.info("Minimum number of feedback documents not reached "
                + "({}/{}). Simplifying query using {} policy.",
                feedbackDocIds.size(), this.conf.
                getMinFeedbackDocumentsCount(), this.conf.
                getQuerySimplifyingPolicy());

        simplifiedQuery = simplifyQuery(simplifiedQuery, this.conf.
                getQuerySimplifyingPolicy());

        if (simplifiedQuery.isEmpty()) {
          throw new IllegalStateException(
                  "No query terms left while trying "
                  + "to reach the minimum nmber of feedback documents.");
        }
        result.addQuery(simplifiedQuery);
        docsToGet = this.conf.getMaxFeedbackDocumentsCount()
                - feedbackDocIds.size();
        queryObj = qBuilder.buildUsingEnvironment(simplifiedQuery);
        feedbackDocIds.addAll(Feedback.get(queryObj, docsToGet));
      }

      // collect all unique terms from feedback documents
      final List<ByteArray> fbTerms = new ArrayList<>(Environment.
              getDataProvider().getDocumentsTermSet(feedbackDocIds));
      // get document frequency threshold
      int minDf = (int) (CollectionMetrics.numberOfDocuments()
              * this.conf.getFeedbackTermSelectionThreshold());
      if (minDf <= 0) {
        LOG.debug("Document frequency threshold was {} setting to 1", minDf);
        minDf = 1;
      }
      LOG.debug("Document frequency threshold is {} = {}", minDf, this.conf.
              getFeedbackTermSelectionThreshold());
      LOG.debug("Initial term set size {}", fbTerms.size());

      // keep results of concurrent term eliminations
      final ConcurrentLinkedQueue<ByteArray> reducedFbTerms
              = new ConcurrentLinkedQueue<>();

      // remove all terms whose threshold is too low
      new Processing(
              new Target.TargetFuncCall<>(
                      new CollectionSource<>(fbTerms),
                      new FbTermReducerTarget(minDf, reducedFbTerms)
              )).process(fbTerms.size());
      LOG.debug("Reduced term set size {}", reducedFbTerms.size());

      // do the final calculation for all remaining feedback terms
      LOG.debug("Using {} feedback documents.", feedbackDocIds.size());
      new Processing(
              new Target.TargetFuncCall<>(
                      new CollectionSource<>(reducedFbTerms),
                      new ModelCalculatorTarget(
                              feedbackDocIds,
                              QueryUtils.getAllQueryTerms(query),
                              score)
              )).process(reducedFbTerms.size());

      result.setScore(score.get());
      result.setFeedbackDocIds(feedbackDocIds);
      result.setFeedbackTerms(fbTerms);

      timeMeasure.stop();
      LOG.debug("Calculating improved clarity score for query {} "
              + "with {} document models and {} terms took {}. {}", query,
              feedbackDocIds.size(), fbTerms.size(), timeMeasure.
              getTimeString(), score);
    } catch (IOException ex) {
      LOG.error("Caught exception while retrieving feedback documents.", ex);
    }

    return result;
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   *
   * Forcing a recalculation is needed, if the language model weight has
   * changed by calling
   * {@link DefaultClarityScore#setLangmodelWeight(double)}.
   */
  public void preCalcDocumentModels() {
    if (!this.hasCache) {
      LOG.warn("Won't pre-calculate any values. Cache not set.");
    }
    final Atomic.Boolean hasData = this.db.getAtomicBoolean(
            Caches.HAS_PRECALC_DATA.name());
    if (hasData.get()) {
      LOG.info("Precalculated models are current.");
    } else {
      LOG.info("Pre-calculating models.");
      new Processing(
              new Target.TargetFuncCall<>(
                      Environment.getDataProvider().getDocumentIdSource(),
                      new DocumentModelCalculatorTarget()
              )).process(CollectionMetrics.numberOfDocuments().intValue());
      hasData.set(true);
    }
  }

  /**
   * {@link Processing} {@link Target} to reduce feedback terms.
   */
  private static class FbTermReducerTarget
          extends Target.TargetFunc<ByteArray> {

    /**
     * Target to store terms passing through the reducing process.
     */
    private final ConcurrentLinkedQueue<ByteArray> reducedTermsTarget;
    /**
     * Minimum document frequency for a term to pass.
     */
    private final int minDf;

    FbTermReducerTarget(final int minDocFreq,
            final ConcurrentLinkedQueue<ByteArray> reducedFbTerms) {
      super();
      this.reducedTermsTarget = reducedFbTerms;
      this.minDf = minDocFreq;
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        if (CollectionMetrics.df(term) >= this.minDf) {
          this.reducedTermsTarget.add(term);
        }
      }
    }
  }

  /**
   * {@link Processing} {@link Target} to calculate document models.
   */
  private class ModelCalculatorTarget
          extends Target.TargetFunc<ByteArray> {

    /**
     * Query terms.
     */
    private final Collection<ByteArray> queryTerms;
    /**
     * Ids of feedback documents to use.
     */
    private final Collection<Integer> feedbackDocIds;
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
    ModelCalculatorTarget(final Collection<Integer> fbDocIds,
            final Collection<ByteArray> qTerms, final AtomicDouble result) {
      super();
      this.queryTerms = qTerms;
      this.feedbackDocIds = fbDocIds;
      this.score = result;
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        try {
          final double queryModel = calcQueryModel(term, this.queryTerms,
                  this.feedbackDocIds);
          score.addAndGet(queryModel * MathUtils.log2(queryModel
                  / CollectionMetrics.relTf(term)));
        } catch (UnsupportedEncodingException ex) {
          LOG.error("Error calculating model for term '{}'", term, ex);
        }
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for document model creation.
   */
  private final class DocumentModelCalculatorTarget extends
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
  public static final class Result extends ClarityScoreResult {

    /**
     * Configuration that was used.
     */
    private ImprovedClarityScoreConfiguration conf;
    /**
     * Ids of feedback documents used for calculation.
     */
    private Collection<Integer> feedbackDocIds;
    /**
     * Terms from feedback documents used for calculation.
     */
    private Collection<ByteArray> feedbackTerms;
    /**
     * Flag indicating, if the query was simplified.
     */
    private boolean wasQuerySimplified = false;
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
      this.feedbackDocIds = Collections.<Integer>emptyList();
      this.feedbackTerms = Collections.<ByteArray>emptyList();
    }

    /**
     * Set the calculation result.
     *
     * @param score Score result
     */
    protected void setScore(final double score) {
      super._setScore(score);
    }

    /**
     * Set the list of feedback documents used.
     *
     * @param fbDocIds List of feedback documents
     */
    protected void setFeedbackDocIds(final Collection<Integer> fbDocIds) {
      this.feedbackDocIds = Collections.unmodifiableCollection(fbDocIds);
    }

    /**
     * Set the list of feedback terms used.
     *
     * @param fbTerms List of feedback terms
     */
    protected void setFeedbackTerms(final Collection<ByteArray> fbTerms) {
      this.feedbackTerms = Collections.unmodifiableCollection(fbTerms);
    }

    /**
     * Set the flag, if this query was simplified.
     *
     * @param state True, if simplified
     */
    protected void setQuerySimplified(final boolean state) {
      this.wasQuerySimplified = state;
    }

    /**
     * Set the configuration that was used.
     *
     * @param newConf Configuration used
     */
    protected void setConf(final ImprovedClarityScoreConfiguration newConf) {
      this.conf = newConf;
    }

    /**
     * Add a query string to the list of issued queries.
     *
     * @param query Query to add
     */
    protected void addQuery(final String query) {
      this.queries.add(query);
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
    public Collection<Integer> getFeedbackDocuments() {
      return Collections.unmodifiableCollection(this.feedbackDocIds);
    }

    /**
     * Get the collection of feedback terms used for calculation.
     *
     * @return Feedback terms used for calculation
     */
    public Collection<ByteArray> getFeedbackTerms() {
      return Collections.unmodifiableCollection(this.feedbackTerms);
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
}
