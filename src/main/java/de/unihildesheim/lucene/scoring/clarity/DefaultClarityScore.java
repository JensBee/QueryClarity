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

import de.unihildesheim.lucene.LuceneDefaults;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.DocumentModelPool;
import de.unihildesheim.lucene.document.DocumentModelPoolObserver;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.document.TermDataManager;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.TimeMeasure;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.BytesRef;
import org.slf4j.LoggerFactory;

/**
 * Default Clarity Score implementation as defined by Cronen-Townsend, Steve,
 * Yun Zhou, and W. Bruce Croft.
 *
 * Reference:
 *
 * “Predicting Query Performance.” In Proceedings of the 25th Annual
 * International ACM SIGIR Conference on Research and Development in Information
 * Retrieval, 299–306. SIGIR ’02. New York, NY, USA: ACM, 2002.
 * doi:10.1145/564376.564429.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultClarityScore implements ClarityScoreCalculation {

  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link DataProvider}.
   */
  private static final String PREFIX = "DCS";

  // Threading parameters
  /**
   * Model pre-calculation: number of models to cache in pool
   */
  private static final int PCALC_POOL_SIZE = 1000;
  /**
   * Model pre-calculation: the maximum number of terms that should be queued
   */
  private static final int PCALC_THREAD_QUEUE_MAX_CAPACITY = 100;
  /**
   * Model pre-calculation: number of calculation threads to run
   */
  private static final int PCALC_THREADS = 5;

  /**
   * Keys to store calculation results in document models and access properties
   * stored in the {@link DataProvider}.
   */
  private enum DataKeys {

    /**
     * Stores the document model for a specific term in a {@link DocumentModel}.
     */
    DOC_MODEL,
    /**
     * Flag to indicate, if all document-models have already been
     * pre-calculated. Stored in the {@link IndexDataProvider}.
     */
    DOCMODELS_PRECALCULATED
  }

  /**
   * Default multiplier value for relative term frequency inside documents.
   */
  private static final double DEFAULT_LANGMODEL_WEIGHT = 0.6d;

  /**
   * Multiplier for relative term frequency inside documents.
   */
  private double langmodelWeight = DEFAULT_LANGMODEL_WEIGHT;

  /**
   * Default number of feedback documents to use. Cronen-Townsend et al.
   * recommend 500 documents.
   */
  private static final int DEFAULT_FEDDBACK_DOCS_COUNT = 500;

  /**
   * Number of feedback documents to use.
   */
  private int fbDocCount = DEFAULT_FEDDBACK_DOCS_COUNT;

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScore.class);

  /**
   * Provider for statistical index related informations. Accessed from nested
   * thread class.
   */
  protected final IndexDataProvider dataProv;

  /**
   * Index reader used by this instance. Accessed from nested thread class.
   */
  protected final IndexReader reader;

  /**
   * {@link TermDataManager} to access to extended data storage for
   * {@link DocumentModel} data storage.
   */
  private final TermDataManager tdMan;

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
    this.reader = indexReader;
    this.dataProv = dataProvider;
    this.tdMan = new TermDataManager(PREFIX, this.dataProv);
  }

  /**
   * Calculates the default value, if the term is not contained in document.
   * This value is also part of the regular calculation formula.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   */
  private double calcDefaultDocumentModel(final BytesWrap term) {
    return (double) (1 - langmodelWeight) * dataProv.getRelativeTermFrequency(
            term);
  }

  /**
   * Calculate the document model for the given term.
   *
   * @param docModel Document data model to use
   * @param term Term do do the calculation for
   * @param update If true, value will be written to the documents data model
   * @return Calculated model value
   */
  private double calcDocumentModel(final DocumentModel docModel,
          final BytesWrap term, final boolean update) {
    // no value was stored, so calculate it
    final double model = langmodelWeight * ((double) docModel.getTermFrequency(
            term) / (double) docModel.getTermFrequency())
            + calcDefaultDocumentModel(term);
    // update document model
    if (update) {
      docModel.unlock();
      this.tdMan.setTermData(docModel, term, DataKeys.DOC_MODEL.name(), model);
      docModel.lock();
    }
    return model;
  }

  /**
   * Calculate the document language model for a given term.
   *
   * @param docModel Document model to do the calculation for
   * @param term Term to do the calculation for
   * @param force If true, the recalculation of the stored model values is
   * forced
   * @return Calculated language model for the given document and term
   */
  private double getDocumentModel(final DocumentModel docModel,
          final BytesWrap term, final boolean force) {
    Double model = null;

    if (docModel.containsTerm(term)) {
      if (!force) {
        // try to get the already calculated value
        model = (Double) this.tdMan.getTermData(docModel, term,
                DataKeys.DOC_MODEL.name());
      }

      if (force || model == null) {
        // no value was stored, so calculate and store it
        model = calcDocumentModel(docModel, term, true);
      }
    } else {
      // term not in document
      model = calcDefaultDocumentModel(term);
    }
    return model;
  }

  /**
   * Calculate the weighting value for all terms in the query.
   *
   * @param docModels Document models to use for calculation
   * @param queryTerms Terms of the originating query
   * @return Mapping of {@link DocumentModel} to calculated language modelString
   */
  private Map<DocumentModel, Double> calculateQueryModelWeight(
          final Set<DocumentModel> docModels,
          final BytesRef[] queryTerms) {
    final Map<DocumentModel, Double> weights = new HashMap(docModels.size());
    @SuppressWarnings("UnusedAssignment")
    double modelWeight;

    for (DocumentModel docModel : docModels) {
      modelWeight = 1d;
      for (BytesRef term : queryTerms) {
        modelWeight *= getDocumentModel(docModel, BytesWrap.wrap(term),
                false);
      }
      weights.put(docModel, modelWeight);
    }
    return weights;
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   *
   * Forcing a recalculation is needed, if the language model weight has changed
   * by calling {@link DefaultClarityScore#setLangmodelWeight(double)}.
   *
   * @param force If true, the recalculation is forced
   */
  public void preCalcDocumentModels(final boolean force) {
    final int termsCount = this.dataProv.getTermsCount();
    LOG.info("Pre-calculating document models ({}) "
            + "for all unique terms ({}) in index.",
            this.dataProv.getDocModelCount(), termsCount);

    final TimeMeasure timeMeasure = new TimeMeasure().start();
    Iterator<BytesWrap> idxTermsIt;

    // debug helpers
    int[] dbgStatus = new int[]{-1, 100, termsCount};
    TimeMeasure dbgTimeMeasure = null;
    if (LOG.isDebugEnabled() && dbgStatus[2] > dbgStatus[1]) {
      dbgStatus[0] = 0;
      dbgTimeMeasure = new TimeMeasure();
    }

    // bounded queue to hold index terms that should be processed
    final BlockingQueue<BytesWrap> termsToProcess = new ArrayBlockingQueue(
            PCALC_THREAD_QUEUE_MAX_CAPACITY);
    final DocumentModelPool docModelPool
            = new DocumentModelPool(PCALC_POOL_SIZE);
    // unbounded queue holding models currently being modified
    final Set<Integer> modifyingModels = new HashSet(PCALC_THREADS * 10);
    // a latch that counts down the items we have processed
    final CountDownLatch consumeLatch = new CountDownLatch(termsCount);
    // all threads spawned
    final Thread[] pCalcThreads = new Thread[PCALC_THREADS];

    LOG.debug("Spawning {} threads for document model calculation.",
            PCALC_THREADS);
    for (int i = 0; i < PCALC_THREADS; i++) {
      pCalcThreads[i] = new Thread(new DocumentModelCalculator(docModelPool,
              termsToProcess, modifyingModels, consumeLatch));
      pCalcThreads[i].setDaemon(true);
      pCalcThreads[i].start();
    }

    // create document pool observer
    final DocumentModelPoolObserver dpObserver = new DocumentModelPoolObserver(
            this.dataProv, docModelPool);
    final Thread dpObserverThread = new Thread(dpObserver);
    dpObserverThread.setDaemon(true);
    dpObserverThread.start();

    idxTermsIt = this.dataProv.getTermsIterator();
    if (dbgStatus[0] > -1) {
      dbgTimeMeasure.start();
    }

    try {
      while (idxTermsIt.hasNext()) {
        termsToProcess.put(idxTermsIt.next());
        // debug operating indicator
        if (dbgStatus[0] >= 0 && ++dbgStatus[0] % dbgStatus[1] == 0) {
          LOG.debug("models for {} terms of {} calculated ({}s)", dbgStatus[0],
                  dbgStatus[2], dbgTimeMeasure.stop().getElapsedSeconds());
          dbgTimeMeasure.start();
        }
      }

      consumeLatch.await(); // wait until all waiting models are processed
      dpObserver.terminate(); // commit all pending models
    } catch (InterruptedException ex) {
      LOG.error(
              "Model pre-calculation thread interrupted. "
              + "This may have caused data loss.", ex);
    }

    timeMeasure.stop();
    LOG.info("Pre-calculating document models for all unique terms in index "
            + "took {} seconds", timeMeasure.getElapsedSeconds());
    // store that we have pre-calculated values
    this.dataProv.setProperty(PREFIX, DataKeys.DOCMODELS_PRECALCULATED.
            name(), "true");
  }

  /**
   * Calculate the clarity score.
   *
   * @param docModels Document models to use for calculation
   * @param idxTermsIt Iterator over all terms from the index
   * @param queryTerms Terms contained in the originating query
   * @return Result of the calculation
   */
  private ClarityScoreResult calculateClarity(
          final Set<DocumentModel> docModels,
          final Iterator<BytesWrap> idxTermsIt,
          final BytesRef[] queryTerms) {
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    double score = 0d;
    double log;
    double qLangMod;

    LOG.debug("Calculating clarity score query={}", (Object[]) queryTerms);

    Map<DocumentModel, Double> modelWeights = calculateQueryModelWeight(
            docModels, queryTerms);

    // iterate over all terms in index
    BytesWrap term;
    while (idxTermsIt.hasNext()) {
      term = idxTermsIt.next();

      // calculate the query probability of the current term
      qLangMod = 0d;
      for (DocumentModel docModel : docModels) {
        qLangMod += getDocumentModel(docModel, term, false) * modelWeights.get(
                docModel);
      }

      // calculate logarithmic part of the formular
      log = (Math.log(qLangMod) / Math.log(2)) / (Math.log(
              dataProv.getRelativeTermFrequency(term)) / Math.log(2));
      // add up final score for each term
      score += qLangMod * log;
    }

    LOG.debug("Calculation results: query={} docModels={} score={} ({}).",
            queryTerms, docModels.size(), score, score);

    final ClarityScoreResult result = new ClarityScoreResult(this.getClass(),
            score);

    timeMeasure.stop();
    LOG.debug("Calculating default clarity score for query {} "
            + "with {} document models took {} seconds.", queryTerms, docModels.
            size(), timeMeasure.getElapsedSeconds());

    return result;
  }

  /**
   * Same as {@link DefaultClarityScore#calculateClarity(Query)}, but allows to
   * pass in the list of feedback documents.
   *
   * @param query Query used for term extraction
   * @param fbDocIds List of document-ids to use for feedback calculation
   * @return Calculated clarity score for the given terms
   */
  public ClarityScoreResult calculateClarity(final Query query,
          final Integer[] fbDocIds) throws ParseException, IOException {
    if (query == null) {
      throw new IllegalArgumentException("Query was null.");
    }
    if (fbDocIds == null || fbDocIds.length == 0) {
      throw new IllegalArgumentException("No feedback documents given.");
    }

    ClarityScoreResult result;

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

    final Set<DocumentModel> docModels = new HashSet(fbDocIds.length);

    for (Integer docId : fbDocIds) {
      docModels.add(this.dataProv.getDocumentModel(docId));
    }

    try {
      result = calculateClarity(docModels, this.dataProv.getTermsIterator(),
              QueryUtils.getQueryTerms(this.reader, query));
    } catch (IOException ex) {
      result = new ClarityScoreResult(this.getClass());
      LOG.error("Caught exception while calculating clarity score.", ex);
    }

    return result;
  }

  @Override
  public ClarityScoreResult calculateClarity(final Query query) {
    if (query == null) {
      throw new IllegalArgumentException("Query was null.");
    }

    ClarityScoreResult result;
    try {
      // get feedback documents..
      final Integer[] fbDocIds = Feedback.getFixed(this.reader, query,
              this.fbDocCount);
      // ..and calculate score
      result = calculateClarity(query, fbDocIds);
    } catch (IOException ex) {
      LOG.error("Error while trying to get feedback documents.", ex);
      // return an empty result on errors
      result = new ClarityScoreResult(this.getClass());
    } catch (ParseException ex) {
      LOG.error("Error while calculating document models.", ex);
      result = new ClarityScoreResult(this.getClass());
    }

    return result;
  }

  /**
   * Runnable to query Lucene for documents matching a specific term and
   * calculate the document models for this term and the matching documents.
   */
  private class DocumentModelCalculator implements Runnable {

    /**
     * Lucene parser for all available document fields.
     */
    private final MultiFieldQueryParser mfQParser;
    /**
     * Terms queue.
     */
    private final BlockingQueue<BytesWrap> queue;
    /**
     * Cached {@link DocumentModel}s pool.
     */
    private final DocumentModelPool pool;
    /**
     * Queue holding models currently being modified.
     */
    private final Set<Integer> modQueue;
    /**
     * Tracking counter.
     */
    private final CountDownLatch latch;
    /**
     * Searcher to access the index.
     */
    private final IndexSearcher searcher;
    /**
     * {@link Collector} to get the total number of matching documents for a
     * term.
     */
    private TotalHitCountCollector coll = new TotalHitCountCollector();
    /**
     * Scoring documents for a single term.
     */
    private ScoreDoc[] results = null;
    /**
     * Expected number of documents to retrieve. Estimated by {@link #coll}.
     */
    private int expResults;
    /**
     * Query for the current term.
     */
    private Query query;
    /**
     * Current {@link DocumentModel} to update.
     */
    private DocumentModel docModel;
    /**
     * Id of current document.
     */
    private Integer currentDocId = null;

    /**
     * Creates a new calculator for document models which gets the term to query
     * from a global working queue.
     *
     * @param blockingQueue Queue to get the terms from
     * @param modifyingModels Shared list of models currently being modified
     * @param cLatch Countdown latch to track the progress
     */
    DocumentModelCalculator(final DocumentModelPool docModelPool,
            final BlockingQueue<BytesWrap> blockingQueue,
            final Set<Integer> modifyingModels,
            final CountDownLatch cLatch) {
      this.pool = docModelPool;
      this.queue = blockingQueue;
      this.modQueue = modifyingModels;
      this.latch = cLatch;
      this.searcher = new IndexSearcher(DefaultClarityScore.this.reader);
      // NOTE: this analyzer won't remove any stopwords!
      final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
              CharArraySet.EMPTY_SET);
      this.mfQParser = new MultiFieldQueryParser(
              LuceneDefaults.VERSION, DefaultClarityScore.this.dataProv.
              getTargetFields(),
              analyzer);
    }

    /**
     * Updates all matching document models for the given term.
     *
     * @param term Term to use for updates
     * @throws InterruptedException Thrown if thread was interrupted
     */
    private void updateModels(final BytesWrap term)
            throws InterruptedException {
      if (this.results == null || this.results.length == 0) {
        return;
      }
      // calculate models for results
      for (ScoreDoc sDoc : this.results) {

        synchronized (this.modQueue) {
          this.modQueue.remove(this.currentDocId);
          this.modQueue.notifyAll();
        }

        this.currentDocId = sDoc.doc;

        // add current model to modifying queue
        synchronized (this.modQueue) {
          while (this.modQueue.contains(this.currentDocId)) {
            this.modQueue.wait();
            break;
          }
          this.modQueue.add(this.currentDocId);
        }

        if (this.pool.containsDocId(currentDocId)) {
          this.docModel = this.pool.get(currentDocId);
        } else {
          this.docModel = DefaultClarityScore.this.dataProv.getDocumentModel(
                  this.currentDocId);
        }

        if (this.docModel == null) {
          LOG.warn("Error retrieving document with id={}. Got null.",
                  this.currentDocId);
        } else {
          if (this.docModel.containsTerm(term)) { // double check?
            try {
              calcDocumentModel(this.docModel, term, true);
              this.docModel.setChanged(true);
              this.pool.put(this.docModel);
            } catch (NullPointerException ex) {
              LOG.error("NPE docModel={} docId={} term={}", this.docModel,
                      this.currentDocId, term, ex);
            }
          }
        }
      }
    }

    @Override
    public void run() {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          // get the next available term
          BytesWrap term = this.queue.take();

          LOG.trace("Query for {}...", BytesWrapUtil.bytesWrapToString(term));

          // decrement the countdown latch
          latch.countDown();

          // build query for matching documents
          try {
            this.query = this.mfQParser.parse(BytesWrapUtil.
                    bytesWrapToString(term));
          } catch (ParseException ex) {
            LOG.error("Caught exception while parsing term query '"
                    + BytesWrapUtil.bytesWrapToString(term) + "'.", ex);
            continue;
          }

          // run the queries
          try {
            this.searcher.search(this.query, this.coll);
            this.expResults = coll.getTotalHits();
            LOG.trace("Query for {} ({}) yields {} results.", BytesWrapUtil.
                    bytesWrapToString(term), this.query, this.expResults);
            if (this.expResults == 0) {
              continue;
            }

            // get number of all matching documents
            this.coll = new TotalHitCountCollector();

            // collect the results
            this.results = this.searcher.search(
                    this.query, this.expResults).scoreDocs;
          } catch (IOException ex) {
            LOG.error("Caught exception while searching index.", ex);
            continue;
          }

          updateModels(term);
        }
      } catch (InterruptedException ex) {
        LOG.warn("Thread interrupted.", ex);
      }
    }
  }
}
