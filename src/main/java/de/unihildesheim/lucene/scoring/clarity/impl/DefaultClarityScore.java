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

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.query.TermsQueryBuilder;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.concurrent.processing.ProcessingException;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.concurrent.AtomicDouble;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
 * @author Jens Bertram <code@jens-bertram.net>
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
  private final String docModelDataKey;

  String getDocModelDataKey() {
    return docModelDataKey;
  }

  /**
   * Default multiplier value for relative term frequency inside documents.
   */
  private static final double DEFAULT_LANGMODEL_WEIGHT = 0.6d;

  /**
   * Multiplier for relative term frequency inside documents.
   */
  private final double langmodelWeight;

  /**
   * Default number of feedback documents to use. Cronen-Townsend et al.
   * recommend 500 documents.
   */
  private static final int DEFAULT_FEEDBACK_DOCS_COUNT = 500;

  /**
   * Number of feedback documents to use.
   */
  private int fbDocCount = DEFAULT_FEEDBACK_DOCS_COUNT;

  /**
   * List of terms used in originating query.
   */
  private Collection<BytesWrap> queryTerms;

  /**
   * Models generated for the query model calculation.
   */
  private Map<Integer, Double> queryModels;

  /**
   * Cached storage of Document-id -> Term, model-value.
   */
  private Map<Integer, Map<BytesWrap, Object>> docModelDataCache;

  /**
   * Default constructor using the {@link IndexDataProvider} for statistical
   * index data.
   */
  public DefaultClarityScore() {
    super();
    this.langmodelWeight = DEFAULT_LANGMODEL_WEIGHT;
    this.docModelDataKey = DataKeys.docModel.name() + "_"
            + this.langmodelWeight;
    Environment.getDataProvider().registerPrefix(PREFIX);
  }

  /**
   * Calculates the default value, if the term is not contained in document.
   * This value is also part of the regular calculation formula.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   */
  private double calcDefaultDocumentModel(final BytesWrap term) {
    return ((1 - langmodelWeight) * Environment.getDataProvider().
            getRelativeTermFrequency(term));
  }

  /**
   * Get the collection of query terms.
   *
   * @return Query terms
   */
  Collection<BytesWrap> getQueryTerms() {
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
   * Get the language model weighting value used for calculation of document
   * and query models.
   *
   * @return Weighting value
   */
  public double getLangmodelWeight() {
    return langmodelWeight;
  }

  /**
   * Calculate the document distribution of a term, document model (pD).
   *
   * @param docModel Document model to do the calculation for
   */
  void calcDocumentModel(final DocumentModel docModel) {
    for (BytesWrap term : docModel.termFreqMap.keySet()) {
      final double model = langmodelWeight * docModel.
              getRelativeTermFrequency(term) + calcDefaultDocumentModel(term);
//      final double model = langmodelWeight * docModel.
//              getSmoothedRelativeTermFrequency(this.dataProv, term, 100)
//              + calcDefaultDocumentModel(term);
      Environment.getDataProvider().setTermData(PREFIX, docModel.id, term,
              this.docModelDataKey, model);
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
          final BytesWrap term) {
    if (!this.docModelDataCache.containsKey(docId)) {
      this.docModelDataCache.put(docId, Environment.getDataProvider().
              getTermData(PREFIX, docId, this.docModelDataKey));
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
    } else {
      // document models have to be calculated - this is not a must, but is a
      // good idea (performance-wise)
      LOG.info("No pre-calculated document models found. Need to calculate.");
      preCalcDocumentModels();
    }
  }

  /**
   * Get the number of feedback document this instance tries to use. The
   * actual number of documents used is restricted by the number of documents
   * available in the index.
   *
   * @return Number of documents this instance tries to get
   */
  public int getFeedbackDocumentCount() {
    return this.fbDocCount;
  }

  /**
   * Sets the number of feedback document this instance tries to use. The
   * actual number of documents used is restricted by the number of documents
   * available in the index.
   *
   * @param newFbDocCount Number of documents this instance should try to get
   */
  public void setFeedbackDocumentCount(final int newFbDocCount) {
    this.fbDocCount = newFbDocCount;
  }

  /**
   * Calculate the clarity score.
   *
   * @param feedbackDocuments Document ids of feedback documents
   * @return Result of the calculation
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  private ClarityScoreResult calculateClarity(
          final Collection<Integer> feedbackDocuments) {
    // check if models are pre-calculated
    checkPrecalculatedModels();

    this.docModelDataCache = new ConcurrentHashMap<>(feedbackDocuments.size());
    this.queryModels = new ConcurrentHashMap<>(feedbackDocuments.size());

    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // convert query to readable string for logging output
    StringBuilder queryStr = new StringBuilder(100);
    for (BytesWrap bw : this.queryTerms) {
      queryStr.append(bw.toString()).append(' ');
    }
    LOG.info("Calculating clarity score. query={}", queryStr);

    // remove terms not in index, their value is zero
    final Iterator<BytesWrap> queryTermsIt = this.queryTerms.iterator();
    BytesWrap queryTerm;
    while (queryTermsIt.hasNext()) {
      queryTerm = queryTermsIt.next();
      if (CollectionMetrics.tf(queryTerm) == null) {
        queryTermsIt.remove();
      }
    }

    // short circuit, if no terms are left
    if (this.queryTerms.isEmpty()) {
      LOG.warn("No query term matched in index. Result is 0.");
      return new ClarityScoreResult(this.getClass(), 0d);
    }

    Processing processing = new Processing();

    // calculate query models for feedback documents.
    LOG.info("Calculating query models for {} feedback documents.",
            feedbackDocuments.size());
    processing.setSourceAndTarget(new TargetQueryModelCalulator(
            new CollectionSource<>(feedbackDocuments)));
    processing.process();

    final AtomicDouble score = new AtomicDouble(0);
    final Source<BytesWrap> sourceCollection;

    // calculation with all terms from collection
//    LOG.info("Calculating query probability values "
//            + "for {} unique terms in collection.", this.dataProv.
//            getUniqueTermsCount());
//    sourceCollection = this.dataProv.getTermsSource();
    // collection with query terms only
//    LOG.info("Calculating query probability values for {} query terms.",
//            this.queryTerms.size());
//    sourceCollection = new CollectionSource<>(this.queryTerms);
    // terms from feedback documents only
    sourceCollection = new CollectionSource<>(Environment.getDataProvider().
            getDocumentsTermSet(feedbackDocuments));

    processing.setSourceAndTarget(new TargetQueryProbabilityCalulator(
            sourceCollection, feedbackDocuments, score));

    processing.process();

    LOG.debug("Calculation results: query={} docModels={} score={}.",
            queryStr, feedbackDocuments.size(), score);

    final ClarityScoreResult result = new ClarityScoreResult(this.getClass(),
            score.doubleValue());

    timeMeasure.stop();
    LOG.debug("Calculating default clarity score for query {} "
            + "with {} document models took {}.", queryStr,
            feedbackDocuments.size(), timeMeasure.getTimeString());

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
  protected ClarityScoreResult calculateClarity(final String query,
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
  public ClarityScoreResult calculateClarity(final String query) throws
          ParseException {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }

    final Query queryObj = TermsQueryBuilder.buildFromEnvironment(query);

    // get feedback documents
    final Collection<Integer> feedbackDocuments;
    try {
      feedbackDocuments = Feedback.getFixed(queryObj, this.fbDocCount);

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
   * Runnable {@link ProcessPipe.Target} to calculate query models.
   */
  private class TargetQueryModelCalulator
          extends Target<Integer> {

    /**
     * @param source {@link Source} for this {@link Target}
     */
    TargetQueryModelCalulator(final Source<Integer> source) {
      super(source);
    }

    @Override
    public Target<Integer> newInstance() {
      return new TargetQueryModelCalulator(getSource());
    }

    @Override
    public void runProcess() throws ProcessingException, InterruptedException {
      while (!isTerminating()) {
        Integer docId;
        try {
          docId = getSource().next();
        } catch (ProcessingException.SourceHasFinishedException ex) {
          break;
        }

        if (docId != null) {
          double modelWeight = 1d;
          Double model;

          for (BytesWrap term : getQueryTerms()) {
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
  }

  /**
   * Runnable {@link ProcessPipe.Target} to calculate query models.
   */
  private class TargetQueryProbabilityCalulator
          extends Target<BytesWrap> {

    /**
     * List of feedback document-ids.
     */
    private final Collection<Integer> fbDocIds;
    /**
     * Shared results of calculation.
     */
    private final AtomicDouble result;

    /**
     * @param source {@link Processing.Source} providing document-ids
     * @param feedbackDocuments Feedback documents to use for calculation
     * @param newResult Shared value of calculation results
     */
    public TargetQueryProbabilityCalulator(
            final Source<BytesWrap> source,
            final Collection<Integer> feedbackDocuments,
            final AtomicDouble newResult) {
      super(source);
      this.fbDocIds = feedbackDocuments;
      this.result = newResult;
    }

    @Override
    public Target<BytesWrap> newInstance() {
      return new TargetQueryProbabilityCalulator(getSource(), this.fbDocIds,
              this.result);
    }

    @Override
    public void runProcess() throws ProcessingException, InterruptedException {
      final double weightFactor = 1 - getLangmodelWeight();

      while (!isTerminating()) {
        BytesWrap term;
        try {
          term = getSource().next();
        } catch (ProcessingException.SourceHasFinishedException ex) {
          break;
        }

        if (term != null) {
          // calculate the query probability of the current term
          double qLangMod = 0d;
          final double termRelFreq = Environment.getDataProvider().
                  getRelativeTermFrequency(term);
          Double model;

          StringBuilder sb = new StringBuilder(term.toString()).append(' ');

          for (Integer docId : this.fbDocIds) {
            model = getDocumentModel(docId, term);
            if (model == null) {
              sb.append("d[").append(docId).append("][").append(weightFactor
                      * termRelFreq * getQueryModel(docId)).append("] ");
              qLangMod += weightFactor * termRelFreq * getQueryModel(docId);
            } else {
              sb.append("m[").append(docId).append("][").append(model
                      * getQueryModel(docId)).append("] ");
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
  }
}
