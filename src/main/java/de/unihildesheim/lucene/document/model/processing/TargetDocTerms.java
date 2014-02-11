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
package de.unihildesheim.lucene.document.model.processing;

import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.BytesRef;
import org.slf4j.LoggerFactory;

/**
 * Process document-ids from the source queue by getting all terms for each
 * document and feed each <tt>document, terms</tt> pair to a worker.
 *
 * <p>
 * IN: {@link ProcessingSource.DocQueue}
 * <p>
 * OUT: {@link ProcessingWorker.DocTerms}
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TargetDocTerms implements ProcessingTarget.DocQueue {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          TargetDocTerms.class);
  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "ProcTargetDocTerms_";
  /**
   * Maximum time to wait for the next query term to be available (in
   * seconds).
   */
  private static final int DOC_MAXWAIT = ClarityScoreConfiguration.INSTANCE.
          getInt(CONF_PREFIX + "docMaxWait", 2);
  /**
   * Number of max queued items per thread.
   */
  private static final int QUEUED_ITEMS_PER_THREAD
          = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
                  + "queuedItemsPerThread", 50);

  /**
   * Name of this runnable.
   */
  private String runName;
  /**
   * Termination flag to cancel processing.
   */
  private boolean terminate = false;
  /**
   * Queue for documents that should be processed.
   */
  private final BlockingDeque<Tuple.Tuple2<Integer, BytesWrap[]>> workQueue;
  /**
   * Size of the worker task queue.
   */
  private final int workQueueSize;

  /**
   * Document-ids source.
   */
  private final ProcessingSource.DocQueue source;
  /**
   * Factory creating worker threads.
   */
  private final ProcessingWorker.DocTerms.Factory workerFactory;

  /**
   * Thread observing the work queue.
   */
  private final Thread wqoThread;
  /**
   * {@link WorkQueueObserver} instance to submit work to worker threads.
   */
  private WorkQueueObserver.DocTermWorkQueueObserver wqObserver;

  @Override
  public void terminate() {
    LOG.debug("({}) Runnable got terminating signal.", this.runName);
    this.terminate = true;
    this.wqObserver.terminate();
  }

  @Override
  public void run() {
    this.runName = Thread.currentThread().getName();
    this.wqoThread.start();
    try {
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(
              this.source.getIndexReader(),
              this.source.getDataProvider().getTargetFields());

      try {
        while (!Thread.currentThread().isInterrupted() && !(this.terminate
                && this.source.getQueue().isEmpty())) {
          if (this.terminate) {
            LOG.debug("Terminate flag set. Queue size {}", this.source.
                    getQueue().size());
          }
          final Integer docId = this.source.getQueue().poll(
                  DOC_MAXWAIT, TimeUnit.SECONDS);
          if (docId == null) {
            // no document available in expected time, stop here
            continue;
          }

          dftEnum.setDocument(docId);
          BytesRef bytesRef = dftEnum.next();
          List<BytesWrap> termList = new ArrayList<>();
          while (bytesRef != null) {
            termList.add(new BytesWrap(bytesRef));
            bytesRef = dftEnum.next();
          }

          if (!termList.isEmpty()) {
            this.workQueue.put(Tuple.tuple2(docId, termList.toArray(
                    new BytesWrap[termList.size()])));
          }
        }
        // processing done
        this.terminate = true; // signal observer threads we're done
        LOG.debug("({}) Waiting for WorkQueueObserver..", this.runName);
        if (this.wqoThread.isAlive()) {
          this.wqoThread.join();
        }
      } catch (InterruptedException ex) {
        LOG.warn("({}) Runnable interrupted.", this.runName, ex);
      }
    } catch (IOException ex) {
      LOG.error("({}) Runnable caught exception and cannot proceed.", ex);
      return;
    }

    // decrement the thread tracking latch
    this.source.getTrackingLatch().countDown();
    LOG.debug("({}) Runnable terminated.", this.runName);
  }

  /**
   * Create a new {@link ProcessingTarget} processing document-ids.
   *
   * @param termsSource Source to provide document-ids
   * @param termsTargetFactory Factory creating instances working with
   * documents and terms
   */
  public TargetDocTerms(final ProcessingSource.DocQueue termsSource,
          final ProcessingWorker.DocTerms.Factory termsTargetFactory) {
    this.source = termsSource;
    this.workerFactory = termsTargetFactory;
    this.workQueueSize = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
            + "workQueueCap", this.source.getThreadCount()
            * QUEUED_ITEMS_PER_THREAD);
    this.workQueue = new LinkedBlockingDeque<>(this.workQueueSize);
    this.wqObserver = new WorkQueueObserver.DocTermWorkQueueObserver(
            "TargetDocTerms", this.source, this.workerFactory, this.workQueue);
    this.wqoThread = new Thread(this.wqObserver, "WorkQueueObserver");
  }

  /**
   * Factory to create new {@link ProcessingTarget} instances of
   * {@link ProcessingTagetSearch} type.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Factory implements
          ProcessingTarget.DocQueue.Factory {

    /**
     * Document-ids source.
     */
    private final ProcessingSource.DocQueue source;
    /**
     * Factory creating worker threads.
     */
    private final ProcessingWorker.DocTerms.Factory workerFactory;

    /**
     * Initialize the factory with the given source and worker factory.
     *
     * @param docSource Documents source to use by the created instances.
     * @param termsTargetFactory Factory to create worker instances processing
     * a document and a list of terms
     */
    public Factory(final ProcessingSource.DocQueue docSource,
            final ProcessingWorker.DocTerms.Factory termsTargetFactory) {
      this.source = docSource;
      this.workerFactory = termsTargetFactory;
    }

    @Override
    public DocQueue newInstance() {
      return new TargetDocTerms(source, workerFactory);
    }
  }
}
