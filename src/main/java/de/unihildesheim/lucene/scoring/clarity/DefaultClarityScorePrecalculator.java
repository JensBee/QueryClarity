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

import de.unihildesheim.lucene.document.model.DocumentModel;
import de.unihildesheim.lucene.document.model.DocumentModelPool;
import de.unihildesheim.lucene.document.model.processing.WorkerDocumentTerm;
import de.unihildesheim.lucene.document.model.processing.Processing;
import de.unihildesheim.lucene.document.model.processing.ProcessingSource;
import de.unihildesheim.lucene.document.model.processing.ProcessingWorker;
import de.unihildesheim.lucene.document.model.processing.ProcessingWorker.DocTerms;
import de.unihildesheim.lucene.document.model.processing.TargetDocTerms;
import de.unihildesheim.lucene.scoring.clarity.DefaultClarityScorePrecalculator.WorkerFactory;
import de.unihildesheim.lucene.util.BytesWrap;
import java.util.Iterator;
import org.apache.lucene.index.IndexReader;
import org.slf4j.LoggerFactory;

/**
 * Threaded document model pre-calculation. This calculation may be very
 * expensive in time, so this class makes heavy use of parallel calculations
 * to try to minimize th needed time.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultClarityScorePrecalculator extends
        ProcessingSource.DocQueue {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScorePrecalculator.class);

  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "DCSPrecalc_";

  /**
   * Parent calculation instance.
   */
  private final DefaultClarityScore dcsInstance;

  /**
   * Number of threads to use for model pre-calculation. Default takes the
   * number of processors times two.
   */
  private static final int THREADS = ClarityScoreConfiguration.INSTANCE.
          getInt(CONF_PREFIX + "threads", Runtime.getRuntime().
                  availableProcessors());

  /**
   * Creates a new document-model pre-calculator for the given
   * {@link DefaultClarityScore} instance.
   *
   * @param dcs Instance to run the calculation for
   */
  public DefaultClarityScorePrecalculator(final DefaultClarityScore dcs) {
    super(dcs.getIndexDataProvider());
    this.dcsInstance = dcs;
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   *
   * @return True, if the pre-calculation finished without errors
   */
  public boolean preCalculate() {
    boolean success = true;
    final Processing processing = new Processing(THREADS);
    final Processing.QueueProcessor processingQueue = processing.
            getDocQueueInstance(this, new TargetDocTerms.Factory(this,
                            new WorkerFactory()));
    try {
      if (!this.dcsInstance.getIndexDataProvider().transactionHookRequest()) {
        throw new IllegalStateException("Failed to aquire transaction hook.");
      }
      processingQueue.process();
      this.dcsInstance.getIndexDataProvider().transactionHookRelease();
    } catch (InterruptedException ex) {
      LOG.error("Model pre-calculation thread interrupted. "
              + "This may have caused data corruption.", ex);
      success = false;
    }
    return success;
  }

  @Override
  public long getNumberOfItemsToProcess() {
    return getDataProvider().getDocModelCount();
  }

  @Override
  public Iterator<Integer> getItemsToProcessIterator() {
    return getDataProvider().getDocIdIterator();
  }

  @Override
  public IndexReader getIndexReader() {
    return this.dcsInstance.getReader();
  }

  /**
   * Worker instance calculating the document model for a single document and
   * a list of terms.
   */
  @SuppressWarnings("PublicInnerClass")
  public final class Worker extends WorkerDocumentTerm {

    /**
     * Create a new worker with a caching pool attached and a specific
     * document and list of terms.
     *
     * @param docModelPool Cached document models pool
     * @param currentDocId Document id to process
     * @param terms List of terms to process
     */
    public Worker(final DocumentModelPool docModelPool,
            final Integer currentDocId, final BytesWrap[] terms) {
      super(docModelPool, currentDocId, terms);
    }

    @Override
    protected void doWork(final DocumentModel docModel, final BytesWrap term) {
      // call the calculation method of the main class for each document and
      // term that is available for processing
      DefaultClarityScorePrecalculator.this.dcsInstance.calcDocumentModel(
              docModel, term, true);
    }
  }

  /**
   * Factory creating new worker instances to do the default clarity score
   * model calculation.
   */
  @SuppressWarnings("PublicInnerClass")
  public final class WorkerFactory implements
          ProcessingWorker.DocTerms.Factory {

    /**
     * Track the number of threads spawned.
     */
    private int threadCounter = 0;

    @Override
    public DocTerms newInstance(final Integer docId, final BytesWrap[] terms) {
      return new Worker(DefaultClarityScorePrecalculator.this.
              getDocumentModelPool(), docId, terms);
    }

    @Override
    public Thread newThread(final Runnable r) {
      return new Thread(r, "DefaultClarityScorePrecalculator_Worker-"
              + (++threadCounter));
    }

  }
}
