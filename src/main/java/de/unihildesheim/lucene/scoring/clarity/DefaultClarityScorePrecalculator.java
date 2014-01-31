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
import de.unihildesheim.lucene.document.DocumentModelCalculator;
import de.unihildesheim.lucene.document.DocumentModelPool;
import de.unihildesheim.lucene.document.DocumentModelPoolObserver;
import de.unihildesheim.lucene.document.DocumentModelUpdater;
import de.unihildesheim.lucene.document.DocumentModelUpdaterFactory;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TotalHitCountCollector;
import org.slf4j.LoggerFactory;

/**
 * Threaded document model pre-calculation. This calculation may be very
 * expensive in time, so this class makes heavy use of parallel calculations to
 * try to minimize th needed time.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DefaultClarityScorePrecalculator implements
        ClarityScorePrecalculator {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScorePrecalculator.class);
  /**
   * Model pre-calculation: the maximum number of terms that should be queued.
   */
  private static final int THREAD_QUEUE_MAX_CAPACITY = 200;
  /**
   * Model pre-calculation: number of calculation threads to run.
   */
  private static final int THREADS = 10;
  /**
   * Unbound cached for {@link DocumentModel}s that have been processed.
   *
   * Accessed from inner classes.
   */
  private final DocumentModelPool docModelPool;
  /**
   * Provider for statistical index related informations.
   *
   * Accessed from inner classes.
   */
  private final IndexDataProvider dataProv;
  /**
   * Bound queue of terms being processed by threads.
   *
   * Accessed from inner classes.
   */
  private final BlockingQueue<BytesWrap> termQueue;
  /**
   * Unbounded queue holding models currently being modified.
   */
  private final ConcurrentSkipListSet<Integer> lockedModels;
  /**
   * Latch that tracks the running threads.
   */
  private final CountDownLatch treadTrackingLatch;

  /**
   * Parent calculation instance.
   */
  private final DefaultClarityScore dcsInstance;

  /**
   * Creates a new document-model pre-calculator for the given
   * {@link DefaultClarityScore} instance.
   *
   * @param dcs Instance to run the calculation for
   */
  public DefaultClarityScorePrecalculator(final DefaultClarityScore dcs) {
    this.dcsInstance = dcs;
    this.dataProv = this.dcsInstance.getIndexDataProvider();
    this.docModelPool = new DocumentModelPool();
    this.termQueue = new ArrayBlockingQueue(THREAD_QUEUE_MAX_CAPACITY);
    this.lockedModels = new ConcurrentSkipListSet();
    this.treadTrackingLatch = new CountDownLatch(THREADS);
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   *
   * @return True, if the pre-calculation finished without errors
   */
  public boolean preCalculate() {
    boolean success = true;
    final long termsCount = this.dataProv.getTermsCount();
    LOG.info("Pre-calculating document models ({}) "
            + "for all unique terms ({}) in index.",
            this.dataProv.getDocModelCount(), termsCount);

    final TimeMeasure timeMeasure = new TimeMeasure().start();
    Iterator<BytesWrap> idxTermsIt;

    // debug helpers
    final long[] dbgStatus = new long[]{-1, 100, termsCount};
    TimeMeasure dbgTimeMeasure = null;
    if (LOG.isDebugEnabled() && dbgStatus[2] > dbgStatus[1]) {
      dbgStatus[0] = 0;
      dbgTimeMeasure = new TimeMeasure();
    }
    if (dbgStatus[0] > -1) {
      dbgTimeMeasure.start();
    }

    // spawn threads
    final DocumentModelCalculator[] pCalcThreads
            = new DocumentModelCalculator[THREADS];
    LOG.debug("Spawning {} threads for document model calculation.",
            THREADS);
    for (int i = 0; i < THREADS; i++) {
      pCalcThreads[i] = new DocumentModelCalculator(this);
      final Thread t = new Thread(pCalcThreads[i], "Updater-" + i);
      t.setDaemon(true);
      t.start();
    }

    // create document pool observer
    final DocumentModelPoolObserver dpObserver = new DocumentModelPoolObserver(
            this.dataProv, this.docModelPool, this.lockedModels);
    final Thread dpObserverThread = new Thread(dpObserver);
    dpObserverThread.setDaemon(true);
    dpObserverThread.start();

    idxTermsIt = this.dataProv.getTermsIterator();
    // do the calculation
    try {
      while (idxTermsIt.hasNext()) {
        this.termQueue.put(idxTermsIt.next());
        // debug operating indicator
        if (dbgStatus[0] >= 0 && ++dbgStatus[0] % dbgStatus[1] == 0) {
          dbgTimeMeasure.stop();
          // estimate time needed
          long estimate = (long) ((dbgStatus[2] - dbgStatus[0]) / (dbgStatus[0]
                  / timeMeasure.getElapsedSeconds()));

          LOG.info("models for {} terms of {} calculated ({}s) after {}. "
                  + "Estimated time needed {}.",
                  dbgStatus[0], dbgStatus[2], dbgTimeMeasure.getElapsedSeconds(),
                  timeMeasure.getElapsedTimeString(),
                  TimeMeasure.getTimeString(estimate));
          dbgTimeMeasure.start();
        }
      }

      // clean up
      for (int i = 0; i < THREADS; i++) {
        pCalcThreads[i].terminate();
      }

      this.treadTrackingLatch.await(); // wait until all waiting models are processed
      dpObserver.terminate(); // commit all pending models
    } catch (InterruptedException ex) {
      LOG.error("Model pre-calculation thread interrupted. "
              + "This may have caused data loss.", ex);
      success = false;
    }

    timeMeasure.stop();
    LOG.info("Pre-calculating document models for all unique terms in index "
            + "took {} seconds", timeMeasure.getElapsedSeconds());
    return success;
  }

  @Override
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public BlockingQueue<BytesWrap> getTermQueue() {
    return this.termQueue;
  }

  @Override
  public IndexDataProvider getDataProvider() {
    return this.dataProv;
  }

  @Override
  public DocumentModelPool getDocumentModelPool() {
    return this.docModelPool;
  }

  @Override
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public Set<Integer> getLockedModelsSet() {
    return this.lockedModels;
  }

  @Override
  public IndexReader getIndexReader() {
    return this.dcsInstance.getReader();
  }

  @Override
  public CountDownLatch getTrackingLatch() {
    return this.treadTrackingLatch;
  }

  /**
   * Small worker thread to calculate and update a single document model with a
   * new value.
   */
  public static class DCSDocumentModelUpdater extends DocumentModelUpdater {

    /**
     * Parent {@link ClarityScorePrecalculator} instance.
     */
    private final DefaultClarityScorePrecalculator pInstance;

    /**
     * Creates a new {@link DocumentModelUpdater} for updating values for the
     * {@link DefaultClarityScore} calculation.
     *
     * @param parentInstance Parent {@link ClarityScorePrecalculator} instance
     * @param currentDocId Document-id to process
     * @param terms Terms to process
     */
    public DCSDocumentModelUpdater(
            final DefaultClarityScorePrecalculator parentInstance,
            final Integer currentDocId,
            final BytesWrap[] terms) {
      super(parentInstance, currentDocId, terms);
      this.pInstance = parentInstance;
    }

    @Override
    protected final void calculate(final DocumentModel docModel,
            final BytesWrap term) {
      this.pInstance.dcsInstance.calcDocumentModel(docModel, term, true);
    }
  }
}
