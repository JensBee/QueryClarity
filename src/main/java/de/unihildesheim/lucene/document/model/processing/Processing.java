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

import de.unihildesheim.lucene.document.model.DocumentModelPool;
import de.unihildesheim.lucene.document.model.processing.ProcessingSource.ProcessingQueueSource;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.TimeMeasure;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import org.slf4j.LoggerFactory;

/**
 * General processing framework to connect a data source with processing
 * targets.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class Processing {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          Processing.class);

  /**
   * Global configuration object.
   */
  private static final ClarityScoreConfiguration CONF
          = ClarityScoreConfiguration.getInstance();

  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "Processing_";

  /**
   * Default number of calculation threads to run.
   */
  private static final int THREADS = CONF.getInt(CONF_PREFIX + "threadsDefault",
          Runtime.getRuntime().availableProcessors());

  /**
   * Number of threads to use for processing.
   */
  private final int numOfthreads;

  /**
   * Plain constructor.
   */
  public Processing() {
    this.numOfthreads = THREADS;
  }

  /**
   * Plain constructor.
   */
  public Processing(final int threads) {
    if (threads <= 0) {
      throw new IllegalArgumentException("Invalid number of threads specified: "
              + threads + ".");
    }
    this.numOfthreads = threads;
  }

  /**
   * Create an observer thread for the {@link DocumentModelPool}.
   *
   * @param source Source providing the pool
   * @return Pool observer instance
   */
  private DocumentModelPool.Observer createPoolObserver(
          final ProcessingSource source) {
    // create document pool observer
    final DocumentModelPool.Observer dpObserver = DocumentModelPool.Observer.
            getInstance(source.getTrackingLatch(), source.getDataProvider(),
                    source.getDocumentModelPool());
    return dpObserver;
  }

  /**
   * Get a termQueue processor instance with source and target set.
   *
   * @param termQueueSource Source of the termQueue
   * @param termQueueTargetFactory Factory creating targets for the termQueue
   * @return {@link ProcessingTermQueue} instance with source and target set
   */
  public ProcessingTermQueue getTermQueueInstance(
          final ProcessingSource.TermQueue termQueueSource,
          final ProcessingTarget.TermQueue.Factory termQueueTargetFactory) {
    return new ProcessingTermQueue(termQueueSource, termQueueTargetFactory);
  }

  /**
   * Get a docQueue processor instance with source and target set.
   *
   * @param docQueueSorce Source of the docQueue
   * @param docQueueTargetFactory Factory creating targets for the docQueue
   * @return {@link ProcessingDocQueue} instance with source and target set
   */
  public ProcessingDocQueue getDocQueueInstance(
          final ProcessingSource.DocQueue docQueueSorce,
          final ProcessingTarget.DocQueue.Factory docQueueTargetFactory) {
    return new ProcessingDocQueue(docQueueSorce, docQueueTargetFactory);
  }

  /**
   * Base class for {@link Processing} instances using a {@link BlockingQueue}
   * to queue work items.
   *
   * @param <T> Type of the queued items
   */
  @SuppressWarnings("PublicInnerClass")
  public abstract class QueueProcessor<T> {

    /**
     * Flag indicating, if a source is set.
     */
    private boolean hasSource = false;
    /**
     * Flag indicating if a target factory is set.
     */
    private boolean hasTargetFactory = false;
    /**
     * Source providing items to process.
     */
    private ProcessingQueueSource source;
    /**
     * Factory creating consumers for the provided items.
     */
    private ProcessingTarget.Factory targetFactory;

    public QueueProcessor(
            final ProcessingQueueSource qSource,
            final ProcessingTarget.Factory qTargetFactory) {
      setSource(qSource);
      setTargetFactory(qTargetFactory);
    }

    /**
     * Set the items source.
     *
     * @param qSource Source providing items to process
     */
    private void setSource(final ProcessingQueueSource qSource) {
      if (hasSource) {
        throw new IllegalArgumentException("Source already set.");
      }
      this.source = qSource;
      this.hasSource = true;
    }

    /**
     * Set the items target factory.
     *
     * @param qTargetFactory Factory creating target processes consuming the
     * created items.
     */
    private void setTargetFactory(
            final ProcessingTarget.Factory qTargetFactory) {
      if (hasTargetFactory) {
        throw new IllegalArgumentException("Target factory already set.");
      }
      this.targetFactory = qTargetFactory;
      this.hasTargetFactory = true;
    }

    /**
     * Public method to kick start the processor.
     *
     * @throws InterruptedException Thrown, if runnable was interupted
     */
    public final void process() throws InterruptedException {
      // pre-check if source & target are available
      if (!this.hasSource || !this.hasTargetFactory) {
        throw new IllegalArgumentException(
                "Source or target factory not specified.");
      }

      // number of terms the sorce will provide
      final long docsCount = this.source.getNumberOfItemsToProcess();
      // show running status in timed intervals
      final TimeMeasure timeMeasure = new TimeMeasure().start();

      // show a progress status in 1% steps
      final long[] runStatus = new long[]{0, (int) (docsCount * 0.01),
        docsCount, 0}; // counter, step, max, percentage
      TimeMeasure runTimeMeasure = new TimeMeasure().start();

      // spawn threads
      LOG.debug("Spawning {} threads to process {} items.",
              Processing.this.numOfthreads, docsCount);
      final ProcessingTarget[] targetThreads
              = new ProcessingTarget[Processing.this.numOfthreads];
      for (int i = 0; i < Processing.this.numOfthreads; i++) {
        targetThreads[i] = targetFactory.newInstance();
        final Thread t = new Thread(targetThreads[i], "ProcessingTarget-" + i);
        t.start();
      }

      // start a pool observer to commmit cached models
      final DocumentModelPool.Observer poolObserver = createPoolObserver(
              this.source);
      final Thread poolObserverThread = new Thread(poolObserver, "PoolObserver");
      poolObserverThread.start();

      final Iterator<T> sourceItemsIt = this.source.getItemsToProcessIterator();

      // do the iteration
      final BlockingQueue<T> sourceQueue = source.getQueue();
      while (sourceItemsIt.hasNext()) {
//        source.getQueue().put(sourceItemsIt.next());
        sourceQueue.put(sourceItemsIt.next());
        // debug operating indicator
        if (++runStatus[0] % runStatus[1] == 0) {
          runTimeMeasure.stop();
          // estimate time needed
          final long estimate = (long) ((runStatus[2] - runStatus[0])
                  / (runStatus[0] / timeMeasure.getElapsedSeconds()));

          LOG.info("Items for {} terms of {} calculated ({}s, {}%) after {}. "
                  + "Estimated time needed {}.",
                  runStatus[0], runStatus[2],
                  runTimeMeasure.getElapsedSeconds(),
                  ++runStatus[3], timeMeasure.getElapsedTimeString(),
                  TimeMeasure.getTimeString(estimate));
          runTimeMeasure.start();
        }
      }

      // clean up
      for (int i = 0; i < Processing.this.numOfthreads; i++) {
        targetThreads[i].terminate();
      }

      // wait until all waiting models are processed
      source.getTrackingLatch().await();
      poolObserver.terminate(); // commit all pending models
      poolObserverThread.join();

      timeMeasure.stop();
      LOG.info("Item processing took {}", timeMeasure.getElapsedTimeString());
    }
  }

  /**
   * Specific processing instance working on a documents queue.
   */
  @SuppressWarnings("PublicInnerClass")
  public final class ProcessingDocQueue extends QueueProcessor<Integer> {

    /**
     * Create a new docQueue processor with the given document-ids source and
     * target factory.
     *
     * @param docQueueSorce Source for document-ids
     * @param docQueueTargetFactory Factory creating term targets
     */
    public ProcessingDocQueue(
            final ProcessingSource.DocQueue docQueueSorce,
            final ProcessingTarget.DocQueue.Factory docQueueTargetFactory) {
      super(docQueueSorce, docQueueTargetFactory);
    }
  }

  /**
   * Specific processing instance working on a term queue.
   */
  @SuppressWarnings("PublicInnerClass")
  public final class ProcessingTermQueue extends QueueProcessor<BytesWrap> {

    /**
     * Create a new termQueue processor with the given terms source and target
     * factory.
     *
     * @param termQueueSource Source for terms
     * @param termQueueTargetFactory Factory creating term targets
     */
    public ProcessingTermQueue(
            final ProcessingSource.TermQueue termQueueSource,
            final ProcessingTarget.TermQueue.Factory termQueueTargetFactory) {
      super(termQueueSource, termQueueTargetFactory);
    }
  }
}
