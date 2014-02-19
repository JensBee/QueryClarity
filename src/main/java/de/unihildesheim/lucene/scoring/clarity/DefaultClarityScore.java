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
package de.unihildesheim.lucene.scoring.clarity;

import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.util.Processing;
import de.unihildesheim.util.Processing.Source;
import de.unihildesheim.util.Processing.Target;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.ProcessingException;
import de.unihildesheim.util.TimeMeasure;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.slf4j.LoggerFactory;

/**
 * Default Clarity Score implementation as defined by Cronen-Townsend, Steve,
 * Yun Zhou, and W. Bruce Croft.
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
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "DCS_";

  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link DataProvider}.
   */
  private static final String PREFIX = ClarityScoreConfiguration.get(
          CONF_PREFIX + "dataPrefix", "DCS");

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

  /**
   * Default multiplier value for relative term frequency inside documents.
   */
  private static final double DEFAULT_LANGMODEL_WEIGHT
          = ClarityScoreConfiguration.getDouble(CONF_PREFIX
                  + "defaultLangModelWeight", 0.6d);

  /**
   * Multiplier for relative term frequency inside documents.
   */
  private final double langmodelWeight;

  /**
   * Default number of feedback documents to use. Cronen-Townsend et al.
   * recommend 500 documents.
   */
  private static final int DEFAULT_FEEDBACK_DOCS_COUNT
          = ClarityScoreConfiguration.getInt(CONF_PREFIX
                  + "defaultFeedbackDocCount", 500);

  /**
   * Number of feedback documents to use.
   */
  private int fbDocCount = DEFAULT_FEEDBACK_DOCS_COUNT;

  /**
   * Provider for statistical index related informations. Accessed from nested
   * thread class.
   */
  private final IndexDataProvider dataProv;

  /**
   * Index reader used by this instance. Accessed from nested thread class.
   */
  private final IndexReader reader;

  /**
   * List of terms used in originating query.
   */
  private Collection<BytesWrap> queryTerms;

  /**
   * Models generated for the query model calculation.
   */
  private Map<Integer, Double> queryModels = new ConcurrentHashMap<>();

  /**
   * Default constructor using the {@link IndexDataProvider} for statistical
   * index data.
   *
   * @param indexReader {@link IndexReader} to use by this instance
   * @param dataProvider Provider for statistical index data
   */
  public DefaultClarityScore(final IndexReader indexReader,
          final IndexDataProvider dataProvider) {
    super();
    this.langmodelWeight = DEFAULT_LANGMODEL_WEIGHT;
    this.reader = indexReader;
    this.dataProv = dataProvider;
    this.docModelDataKey = DataKeys.docModel.name() + "_"
            + this.langmodelWeight;
  }

  /**
   * Calculates the default value, if the term is not contained in document.
   * This value is also part of the regular calculation formula.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   */
  private double calcDefaultDocumentModel(final BytesWrap term) {
    return (double) ((1 - langmodelWeight) * this.dataProv.
            getRelativeTermFrequency(term));
  }

  /**
   * Get the collection of query terms.
   *
   * @return Query terms
   */
  protected Collection<BytesWrap> getQueryTerms() {
    return Collections.unmodifiableCollection(queryTerms);
  }

  /**
   * Add a calculated model to the list of query models.
   *
   * @param documentId Document-id of the feedback-document used
   * @param value Calculated model value
   */
  protected void addQueryModel(final Integer documentId,
          final Double value) {
    this.queryModels.put(documentId, value);
  }

  /**
   * Get a specific query model from the list of calculated query models.
   *
   * @param documentId Document-id to identify the document
   * @return Model calculated for the given document identified by it's id
   */
  protected Double getQueryModel(final Integer documentId) {
    return this.queryModels.get(documentId);
  }

  /**
   * Get the {@link IndexDataProvider} used.
   *
   * @return Data provider
   */
  protected IndexDataProvider getDataProv() {
    return dataProv;
  }

  /**
   * Get the language model weighting value used for calculation of document
   * and query models.
   *
   * @return Weighting value
   */
  protected double getLangmodelWeight() {
    return langmodelWeight;
  }

  /**
   * Calculate the document model for the given document and terms.
   *
   * @param docModel Document model to do the calculation for
   * @param terms List of terms to calculate
   */
  protected void calcDocumentModel(final DocumentModel docModel,
          final Collection<BytesWrap> terms) {
    final double termFreq = docModel.termFrequency;

    for (BytesWrap term : terms) {
      if (docModel.contains(term)) {
        final double model = langmodelWeight * ((double) docModel.
                termFrequency(term) / termFreq) + calcDefaultDocumentModel(
                        term);
        this.dataProv.setTermData(PREFIX, docModel.id, term,
                this.docModelDataKey, model);
      }
    }
  }

  /**
   * Calculate the document language model for a given term.
   *
   * @param docId Document model to do the calculation for
   * @param term Term to do the calculation for
   * @return Calculated language model for the given document and term or
   * <tt>null</tt> if the term was not found in the document
   */
  private Double getDocumentModel(final Integer docId, final BytesWrap term) {
    // try to get the already calculated value
    return (Double) this.dataProv.getTermData(PREFIX, docId, term,
            this.docModelDataKey);
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   *
   * Forcing a recalculation is needed, if the language model weight has
   * changed by calling
   * {@link DefaultClarityScore#setLangmodelWeight(double)}.
   *
   * @param force If true, the recalculation is forced
   */
  public void preCalcDocumentModels(final boolean force) {
    final DefaultClarityScorePrecalculator dcsP
            = new DefaultClarityScorePrecalculator(this);

    dcsP.preCalculate();

    // store that we have pre-calculated values
    this.dataProv.setProperty(PREFIX, DataKeys.DOCMODELS_PRECALCULATED.
            name(), "true");
  }

  /**
   * Check, if pre-calculated document models are available. Starts
   * re-calculation, if no pre-calculated data could be found.
   */
  private void checkPrecalculatedModels() {
    // check if document models are pre-calculated and stored
    final boolean hasPrecalcData = Boolean.parseBoolean(this.dataProv.
            getProperty(PREFIX, DataKeys.DOCMODELS_PRECALCULATED.name()));
    if (hasPrecalcData) {
      LOG.info("Using pre-calculated document models.");
    } else {
      // document models have to be calculated - this is not a must, but is a
      // good idea (performance-wise)
      LOG.info("No pre-calculated document models found. Need to calculate.");
      preCalcDocumentModels(false);
    }
  }

  /**
   * Calculate the clarity score.
   *
   * @param feedbackDocuments Document ids of feedback documents
   * @param newQueryTerms Terms contained in the originating query
   * @return Result of the calculation
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  private ClarityScoreResult calculateClarity(
          final Collection<BytesWrap> newQueryTerms,
          final Collection<Integer> feedbackDocuments) {
    // check if models are pre-calculated
    checkPrecalculatedModels();

    // make query terms globally accessible for calculation threads
    this.queryTerms = newQueryTerms;

    final TimeMeasure timeMeasure = new TimeMeasure().start();
    double score = 0d;

    LOG.info("Calculating clarity score. query={}", this.queryTerms);

    // calculate query models for feedback documents.
    LOG.info("Calculating query models.");
    Processing qmPPipe = new Processing(new TargetQueryModelCalulator(
            new Processing.CollectionSource<>(feedbackDocuments)));
    qmPPipe.process();

    Collection<Double> qProbabilities = new ConcurrentSkipListSet();
    LOG.info("Calculating query probability values.");
    Processing qpPPipe = new Processing(new TargetQueryProbabilityCalulator(
            this.dataProv.getTermsSource(), feedbackDocuments,
            qProbabilities));
    qpPPipe.process(Processing.THREADS * 2);

    for (Double result : qProbabilities) {
      score += result;
    }

    LOG.debug("Calculation results: query={} docModels={} score={}.",
            this.queryTerms, feedbackDocuments.size(), score);

    final ClarityScoreResult result = new ClarityScoreResult(this.getClass(),
            score);

    timeMeasure.stop();
    LOG.debug("Calculating default clarity score for query {} "
            + "with {} document models took {}.", this.queryTerms,
            feedbackDocuments.size(), timeMeasure.getElapsedTimeString());

    return result;
  }

  @Override
  public ClarityScoreResult calculateClarity(final Query query) {
    if (query == null) {
      throw new IllegalArgumentException("Query was null.");
    }

    Collection<BytesWrap> newQueryTerms;
    final Collection<Integer> feedbackDocuments;
    try {
      newQueryTerms = QueryUtils.getQueryTerms(this.reader, query);
      feedbackDocuments = Feedback.getFixed(this.reader, query,
              this.fbDocCount);
    } catch (IOException ex) {
      LOG.error("Caught exception while preparing calculation.", ex);
      return null;
    }

    if (newQueryTerms == null || newQueryTerms.isEmpty()) {
      throw new IllegalStateException("No query terms.");
    }
    if (feedbackDocuments == null || feedbackDocuments.isEmpty()) {
      throw new IllegalStateException("No feedback documents.");
    }

    ClarityScoreResult result = calculateClarity(newQueryTerms,
            feedbackDocuments);

    return result;
  }

  @Override
  public IndexDataProvider getIndexDataProvider() {
    return this.dataProv;
  }

  @Override
  public IndexReader getReader() {
    return this.reader;
  }

  /**
   * Runnable {@link ProcessPipe.Target} to calculate query models.
   */
  private class TargetQueryModelCalulator
          extends Processing.Target<Integer> {

    /**
     * Flag to indicate, if this {@link Runnable} should terminate.
     */
    private volatile boolean terminate = false;
    /**
     * Name to identify this {@link Runnable}.
     */
    private final String rId = "(" + TargetQueryModelCalulator.class + "-"
            + this.hashCode() + ")";
    /**
     * Shared latch to track running threads.
     */
    private final CountDownLatch latch;

    /**
     * Base constructor without setting a {@link CountDownLatch}. This
     * instance is not able to be run.
     *
     * @param source {@link Source} for this {@link Target}
     */
    TargetQueryModelCalulator(final Processing.Source<Integer> source) {
      super(source);
      this.latch = null;
    }

    /**
     * Creates a new instance able to run. Meant to be called from the factory
     * method.
     *
     * @param source @param source {@link Source} for this {@link Target}
     * @param newLatch Shared latch to track running threads
     */
    private TargetQueryModelCalulator(final Processing.Source<Integer> source,
            final CountDownLatch newLatch) {
      super(source);
      this.latch = newLatch;
    }

    @Override
    public void terminate() {
      LOG.debug("{} Received termination signal.", this.rId);
      this.terminate = true;
    }

    @Override
    public Processing.Target<Integer> newInstance(
            final CountDownLatch newLatch) {
      return new TargetQueryModelCalulator(getSource(), newLatch);
    }

    @Override
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch"})
    public void run() {
      LOG.debug("{} Starting.", this.rId);
      try {
        if (this.latch == null) {
          throw new IllegalStateException(this.rId
                  + " Tracking latch not set.");
        }

        while (!this.terminate && getSource().isRunning()) {
          final Integer docId = getSource().next();
          if (docId != null) {
            double modelWeight = 1d;

            for (BytesWrap term : getQueryTerms()) {
              if (getDataProv().documentContains(docId, term)) {
                modelWeight *= getDocumentModel(docId, term);
              } else {
                modelWeight *= calcDefaultDocumentModel(term);
              }
            }
            addQueryModel(docId, modelWeight);
          }
        }

        LOG.debug("{} Terminating.", this.rId);
      } catch (Exception ex) {
        LOG.debug("{} Caught exception. Terminating.", this.rId, ex);
      } finally {
        this.latch.countDown();
      }
    }
  }

  /**
   * Runnable {@link ProcessPipe.Target} to calculate query models.
   */
  private class TargetQueryProbabilityCalulator
          extends Processing.Target<BytesWrap> {

    /**
     * Flag to indicate, if this {@link Runnable} should terminate.
     */
    private volatile boolean terminate = false;
    /**
     * Name to identify this {@link Runnable}.
     */
    private final String rId = "(" + TargetQueryProbabilityCalulator.class
            + "-" + this.hashCode() + ")";
    /**
     * Shared latch to track running threads.
     */
    private final CountDownLatch latch;
    /**
     * Shared {@link Collection} to gather results.
     */
    private final Collection<Double> results;
    /**
     * List of feedback document-ids.
     */
    private final Collection<Integer> fbDocIds;

    /**
     * {@link Processing.Target} reading document-ids and calculate the
     * corresponding query models. This constructor is used as initializer and
     * is not able to run.
     *
     * @param source {@link Processing.Source} providing document-ids
     * @param feedbackDocuments Feedback documents to use for calculation
     * @param resultsCollection Collection to gather the calculation results
     */
    public TargetQueryProbabilityCalulator(
            final Processing.Source<BytesWrap> source,
            final Collection<Integer> feedbackDocuments,
            final Collection<Double> resultsCollection) {
      super(source);
      this.latch = null;
      this.results = resultsCollection;
      this.fbDocIds = feedbackDocuments;
    }

    /**
     * {@link Processing.Target} reading document-ids and calculate the
     * corresponding query models.
     *
     * @param source {@link Processing.Source} providing document-ids
     * @param feedbackDocuments Feedback documents to use for calculation
     * @param resultsCollection Collection to gather the calculation results
     * @param newLatch Latch to track running threads
     */
    public TargetQueryProbabilityCalulator(
            final Processing.Source<BytesWrap> source,
            final Collection<Integer> feedbackDocuments,
            final Collection<Double> resultsCollection,
            final CountDownLatch newLatch) {
      super(source);
      this.latch = newLatch;
      this.results = resultsCollection;
      this.fbDocIds = feedbackDocuments;
    }

    @Override
    public void terminate() {
      this.terminate = true;
    }

    @Override
    public Target<BytesWrap> newInstance(final CountDownLatch newLatch) {
      return new TargetQueryProbabilityCalulator(getSource(),
              this.fbDocIds, this.results, newLatch);
    }

    @Override
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch"})
    public void run() {
      try {
        if (this.latch == null) {
          throw new IllegalStateException(this.rId
                  + " Tracking latch not set.");
        }

        final double weightFactor = 1 - getLangmodelWeight();

        while (!this.terminate && getSource().isRunning()) {
          final BytesWrap term = getSource().next();

          if (term != null) {
            // calculate the query probability of the current term
            double qLangMod = 0d;
            final double termRelFreq = getDataProv().getRelativeTermFrequency(
                    term);

            for (Integer docId : this.fbDocIds) {
              final Double model = getDocumentModel(docId, term);
              if (model == null) {
                qLangMod += weightFactor * termRelFreq * getQueryModel(docId);
              } else {
                qLangMod += model * getQueryModel(docId);
              }
            }

            // calculate logarithmic part of the formular
            final double log = (Math.log(qLangMod) / Math.log(2))
                    / (Math.log(termRelFreq) / Math.log(2));
            // add up final score for each term
            this.results.add(qLangMod * log);
          }
        }
        LOG.debug("{} Terminating.", this.rId);
      } catch (ProcessingException.SourceHasFinishedException ex) {
        LOG.debug("Source has finished unexpectedly.");
      } catch (Exception ex) {
        LOG.debug("{} Caught exception. Terminating.", this.rId, ex);
      } finally {
        this.latch.countDown();
      }
    }
  }
}
