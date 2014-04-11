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
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.metrics.DocumentMetrics;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.query.TermsQueryBuilder;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.Configuration;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.concurrent.AtomicDouble;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.slf4j.LoggerFactory;

/**
 * Default Clarity Score implementation as described by Cronen-Townsend,
 * Steve, Yun Zhou, and W. Bruce Croft.
 * <p>
 * Reference:
 * <p>
 * “Predicting Query Performance.” In Proceedings of the 25th Annual
 * International ACM SIGIR Conference on Research and Development in
 * Information Retrieval, 299–306. SIGIR ’02. New York, NY, USA: ACM, 2002.
 * doi:10.1145/564376.564429.
 *
 * @author Jens Bertram
 */
public final class DefaultClarityScore implements ClarityScoreCalculation {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScore.class);

  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link DataProvider}.
   */
  static final String PREFIX = "DCS";

  /**
   * Keys to store calculation results in document models and access
   * properties stored in the {@link DataProvider}.
   */
  private enum DataKeys {

    /**
     * Stores the document model for a specific term in a
     * {@link DocumentModel}. This get suffixed by the currently set
     * {@link #langmodelWeight} do distinguish different calculation values.
     */
    docModel,
    /**
     * Flag to indicate, if all document-models have already been
     * pre-calculated. Stored in the {@link IndexDataProvider}.
     */
    DOCMODELS_PRECALCULATED
  }

  /**
   * Combined key to store data. Identifies this class and the weighting
   * parameters used for calculation.
   */
  private String docModelDataKey;

  /**
   * Get the current data key containing the current language-model weighting
   * value.
   *
   * @return String key to store data
   */
  String getDocModelDataKey() {
    return docModelDataKey;
  }

  /**
   * List of terms used in originating query.
   */
  private Collection<ByteArray> queryTerms;

  /**
   * Models generated for the query model calculation.
   */
  private Map<Integer, Double> queryModels;

  /**
   * Cached storage of Document-id -> Term, model-value.
   */
  private Map<Integer, Map<ByteArray, Object>> docModelDataCache;

  /**
   * Configuration object used for all parameters of the calculation.
   */
  protected DefaultClarityScoreConfiguration conf;

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
    Environment.getDataProvider().registerPrefix(PREFIX);
  }

  @Override
  public final DefaultClarityScore setConfiguration(
          final Configuration newConf) {
    if (!(newConf instanceof DefaultClarityScoreConfiguration)) {
      throw new IllegalArgumentException("Wrong configuration type.");
    }
    this.conf = (DefaultClarityScoreConfiguration) newConf;
    this.docModelDataKey = DataKeys.docModel.name() + "_"
            + this.conf.getLangModelWeight();
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
    return ((1 - this.conf.getLangModelWeight()) * CollectionMetrics.relTf(
            term));
  }

  /**
   * Get the collection of query terms.
   *
   * @return Query terms
   */
  protected Collection<ByteArray> getQueryTerms() {
    return Collections.unmodifiableCollection(queryTerms);
  }

  /**
   * Add a calculated model to the list of query models.
   *
   * @param documentId Document-id of the feedback-document used
   * @param value Calculated model value
   */
  private void addQueryModel(final Integer documentId,
          final Double value) {
    this.queryModels.put(documentId, value);
  }

  /**
   * Get a specific query model from the list of calculated query models.
   *
   * @param documentId Document-id to identify the document
   * @return Model calculated for the given document identified by it's id
   */
  private Double getQueryModel(final Integer documentId) {
    return this.queryModels.get(documentId);
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
      Environment.getDataProvider().setTermData(PREFIX, docModel.id, term.
              clone(), this.docModelDataKey, model);
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
  private synchronized Double getDocumentModel(final Integer docId,
          final ByteArray term) {
    if (!this.docModelDataCache.containsKey(docId)) {
      Map<ByteArray, Object> td = Environment.getDataProvider().
              getTermData(PREFIX, docId, this.docModelDataKey);
      if (td == null || td.isEmpty()) {
        calcDocumentModel(DocumentMetrics.getModel(docId));
        td = Environment.getDataProvider().getTermData(PREFIX, docId,
                this.docModelDataKey);
      }
      this.docModelDataCache.put(docId, td);
    }

    return (Double) this.docModelDataCache.get(docId).get(term);
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   *
   * Forcing a recalculation is needed, if the language model weight has
   * changed by calling
   * {@link DefaultClarityScore#setLangmodelWeight(double)}.
   */
  public void preCalcDocumentModels() {
    final DefaultClarityScorePrecalculator dcsP
            = new DefaultClarityScorePrecalculator(this);

    dcsP.preCalculate();

    // store that we have pre-calculated values
    Environment.setProperty(PREFIX, DataKeys.DOCMODELS_PRECALCULATED.
            name(), "true");
  }

  /**
   * Check, if pre-calculated document models are available. Starts
   * re-calculation, if no pre-calculated data could be found.
   */
  private void checkPrecalculatedModels() {
    // check if document models are pre-calculated and stored
    final boolean hasPrecalcData = Boolean.parseBoolean(Environment.
            getProperty(PREFIX, DataKeys.DOCMODELS_PRECALCULATED.name(),
                    "false"));
    if (hasPrecalcData) {
      LOG.info("Using pre-calculated document models.");
    } else if (!Environment.isTestRun()) {
      // document models have to be calculated - this is not a must, but is a
      // good idea (performance-wise)
      LOG.info("No pre-calculated document models found. Need to calculate.");
      preCalcDocumentModels();
    }
  }

  /**
   * Calculate the clarity score.
   *
   * @param feedbackDocuments Document ids of feedback documents
   * @return Result of the calculation
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  private Result calculateClarity(final Collection<Integer> feedbackDocuments) {
    // check if models are pre-calculated
    checkPrecalculatedModels();

    final Result result = new Result(this.getClass());
    result.setFeedbackDocIds(feedbackDocuments);
    result.setQueryTerms(this.queryTerms);

    this.docModelDataCache = new ConcurrentHashMap<>(feedbackDocuments.size());
    this.queryModels = new ConcurrentHashMap<>(feedbackDocuments.size());

    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // convert query to readable string for logging output
    @SuppressWarnings("StringBufferWithoutInitialCapacity")
    StringBuilder queryStr = new StringBuilder();
    for (ByteArray ba : this.queryTerms) {
      queryStr.append(ByteArrayUtil.utf8ToString(ba)).append(' ');
    }
    LOG.info("Calculating clarity score. query={}", queryStr);

    // remove terms not in index, their value is zero
    final Iterator<ByteArray> queryTermsIt = this.queryTerms.iterator();
    ByteArray queryTerm;
    while (queryTermsIt.hasNext()) {
      queryTerm = queryTermsIt.next();
      if (CollectionMetrics.tf(queryTerm) == null) {
        LOG.warn("Remove term!");
        queryTermsIt.remove();
      }
    }

    // short circuit, if no terms are left
    if (this.queryTerms.isEmpty()) {
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
                    new QueryModelCalulatorTarget()
            )).process();

    final AtomicDouble score = new AtomicDouble(0);
    final Source<ByteArray> sourceCollection;

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
    sourceCollection = new CollectionSource<>(Environment.getDataProvider().
            getDocumentsTermSet(feedbackDocuments));

    p.setSourceAndTarget(
            new Target.TargetFuncCall<>(
                    sourceCollection,
                    new QueryProbabilityCalculatorTarget(
                            feedbackDocuments, score)
            )).process();

    result.setScore(score.doubleValue());

    timeMeasure.stop();
    LOG.debug("Calculating default clarity score for query {} "
            + "with {} document models took {}. {}", queryStr,
            feedbackDocuments.size(), timeMeasure.getTimeString(), score);

    return result;
  }

  /**
   * Proxy method to add feedback documents and prepare the query. Also allows
   * testing of the calculation.
   *
   * @param query Original query
   * @param feedbackDocuments Documents to use for feedback calculations
   * @return Calculated clarity score
   */
  protected Result calculateClarity(final String query,
          final Collection<Integer> feedbackDocuments) {
    try {
      // Get unique query terms
      this.queryTerms = QueryUtils.getUniqueQueryTerms(query);
    } catch (UnsupportedEncodingException | ParseException ex) {
      LOG.error("Caught exception parsing query.", ex);
      return null;
    }

    if (this.queryTerms == null || this.queryTerms.isEmpty()) {
      throw new IllegalStateException("No query terms.");
    }

    return calculateClarity(feedbackDocuments);
  }

  @Override
  public Result calculateClarity(final String query) throws
          ParseException {
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

    return calculateClarity(query, feedbackDocuments);
  }

  /**
   * {@link Processing} {@link Target} for query model calculation.
   */
  private final class QueryModelCalulatorTarget extends
          Target.TargetFunc<Integer> {

    @Override
    public void call(final Integer docId) {
      if (docId != null) {
        double modelWeight = 1d;
        Double model;

        for (ByteArray term : getQueryTerms()) {
          model = getDocumentModel(docId, term);
          if (model == null) {
            modelWeight *= calcDefaultDocumentModel(term);
          } else {
            modelWeight *= model;
          }
        }
        addQueryModel(docId, modelWeight);
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
    private final double weightFactor = 1 - DefaultClarityScore.this.conf.
            getLangModelWeight();

    /**
     * @param source {@link Processing.Source} providing document-ids
     * @param feedbackDocuments Feedback documents to use for calculation
     * @param newResult Shared value of calculation results
     */
    QueryProbabilityCalculatorTarget(
            final Collection<Integer> feedbackDocuments,
            final AtomicDouble newResult) {
      super();
      this.fbDocIds = feedbackDocuments;
      this.result = newResult;
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        // calculate the query probability of the current term
        double qLangMod = 0d;
        final double termRelFreq = CollectionMetrics.relTf(term);
        Double model;

        for (Integer docId : this.fbDocIds) {
          model = getDocumentModel(docId, term);
          if (model == null) {
            qLangMod += this.weightFactor * termRelFreq * getQueryModel(docId);
          } else {
            qLangMod += model * getQueryModel(docId);
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
   * Extended result object containing additional meta information about what
   * values were actually used for calculation.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Result extends ClarityScoreResult {

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
