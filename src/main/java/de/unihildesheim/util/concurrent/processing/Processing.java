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
package de.unihildesheim.util.concurrent.processing;

import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;

/**
 * One source, one or more targets.
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
   * Processing {@link Source}.
   */
  private Source source = null;

  /**
   * Processing {@link Target}.
   */
  private Target target = null;

  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "ProcessPipe_";
  /**
   * Default number of target threads to run.
   */
  public static final int THREADS = ClarityScoreConfiguration.getInt(
          CONF_PREFIX + "targetThreadsCount", Runtime.getRuntime().
          availableProcessors());
  /**
   * Latch that tracks the running threads.
   */
  private CountDownLatch threadTrackingLatch;
  /**
   * Thread pool handling thread execution.
   */
  private static ProcessingThreadPoolExecutor executor = null;

  /**
   * Creates a new processing pipe with the given {@link Source} and
   * {@link Target}.
   *
   * @param newSource Processing {@link Source}
   * @param newTarget Processing {@link Target}
   */
  public Processing(final Source newSource, final Target newTarget) {
    if (newSource == null) {
      throw new IllegalArgumentException("Source was null.");
    }
    if (newTarget == null) {
      throw new IllegalArgumentException("Target was null.");
    }
    this.source = newSource;
    this.target = newTarget;
    initPool();
  }

  /**
   * Creates a new processing pipe, deriving the {@link Source} from the
   * {@link Target}.
   *
   * @see Processing#Processing(Source, Target)
   * @param newTarget Processing {@link Target}
   */
  public Processing(final Target newTarget) {
    this(newTarget.getSource(), newTarget);
  }

  /**
   * Plain constructor. Source and target must be set manually to start
   * processing.
   */
  public Processing() {
    initPool();
  }

  /**
   * Reuse this instance by setting a new {@link Processing.Source}.
   *
   * @param newSource New source to use
   */
  public void setSource(final Source newSource) {
    this.source = newSource;
  }

  /**
   * Creates a new processing pipe source and target, deriving the
   * {@link Source} from the {@link Target}.
   *
   * @param newTarget Processing target
   */
  public void setSourceAndTarget(final Target newTarget) {
    this.source = newTarget.getSource();
    this.target = newTarget;
  }

  /**
   * Reuse this instance by setting a new {@link Processing.Target}.
   *
   * @param newTarget New target to use
   */
  public void setTarget(final Target newTarget) {
    this.target = newTarget;
  }

  /**
   * Initialize the thread pool. Adds a shutdown hook to terminate the pool.
   */
  private void initPool() {
    if (executor == null) {
      LOG.debug("Initialize thread pool.");
      executor = new ProcessingThreadPoolExecutor();
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          Processing.shutDown();
        }
      }, "Processing_shutdownHandler"));
    }
  }

  /**
   * Shuts down the thread pool maintained by this instance.
   */
  public static void shutDown() {
    if (executor != null) {
      LOG.trace("Shutting down thread pool.");
      executor.shutDown();
    }
  }

  /**
   * Start processing with the defined {@link Source} and {@link Target} with
   * the default number of threads.
   */
  public void process() {
    process(THREADS);
  }

  /**
   * Start processing with the defined {@link Source} and {@link Target} with
   * a specified number of threads.
   *
   * @param threadCount Number of threads to use
   */
  public void process(final int threadCount) {
    if (this.source == null) {
      throw new IllegalStateException("No source set.");
    }
    if (this.target == null) {
      throw new IllegalStateException("No target set.");
    }

    final Collection<Target> targets = new ArrayList<>(threadCount);
    this.threadTrackingLatch = new CountDownLatch(threadCount);
    executor.setTargetThreadsCount(threadCount);
    SourceObserver sourceObserver;

    LOG.debug("Spawning {} Processing-Target threads.", threadCount);
    for (int i = 0; i < threadCount; i++) {
      final Target aTarget = this.target.newInstance();
      aTarget.setLatch(this.threadTrackingLatch);
      targets.add(aTarget);
    }

    LOG.trace("Starting Processing-Source.");
    final Future sourceThread = executor.runSource(this.source);
    // start observer, if applicable
    if (this.source instanceof ObservableSource) {
      sourceObserver = new SourceObserver((ObservableSource) this.source);
      executor.runTask(sourceObserver);
    } else {
      sourceObserver = null;
    }

    LOG.trace("Starting Processing-Target threads.");
    for (Target aTarget : targets) {
      executor.runTask(aTarget);
    }

    // wait until source has finished
    try {
      sourceThread.get();
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error("Caught exception while tracking source state.", ex);
    }

    // terminate observer, if any was used
    if (sourceObserver != null) {
      LOG.trace("Processing-Source finished. Terminating observer.");
      sourceObserver.terminate(((ObservableSource) this.source).
              getSourcedItemCount());
    }

    LOG.trace("Processing-Source finished. Terminating Targets.");
    for (Target aTarget : targets) {
      aTarget.terminate();
    }

    LOG.trace("Awaiting Processing-Targets termination.");
    try {
      this.threadTrackingLatch.await();
    } catch (InterruptedException ex) {
      LOG.error("Processing interrupted.", ex);
    }

    LOG.trace("Processing finished.");
  }

  /**
   * Thread pool manager for processing threads.
   */
  private static class ProcessingThreadPoolExecutor {

    /**
     * Keep-alive time for waiting threads. Set to <tt>Integer.MAX_VALUE</tt>
     * to never expire.
     */
    private static final long KEEP_ALIVE_TIME = Integer.MAX_VALUE;
    /**
     * Thread pool manager.
     */
    private ThreadPoolExecutor threadPool = null;
    /**
     * Size of the work queue, adapted to the number of threads to run.
     */
    private int workQueueSize = 15;
    /**
     * Working queue for runnable jobs.
     */
    private ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(
            workQueueSize);

    /**
     * Create a new processing thread pool manager. The initial allowed number
     * of parallel threads is derived from the number of processors available.
     */
    ProcessingThreadPoolExecutor() {
      this(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Create a new processing thread pool manager with a given number of
     * parallel threads.
     *
     * @param numOfTargetThreads Number of parallel threads allowed
     */
    ProcessingThreadPoolExecutor(final int numOfTargetThreads) {
      final int poolSize = calcPoolSize(numOfTargetThreads);
      this.threadPool = new ThreadPoolExecutor(poolSize,
              poolSize, KEEP_ALIVE_TIME, TimeUnit.SECONDS, workQueue);
    }

    /**
     * Calculates the number of needed threads to allow running all needed
     * processes.
     *
     * @param numOfTargetThreads Number of targets to rn in parallel
     * @return Final number of threads needed
     */
    private int calcPoolSize(final int numOfTargetThreads) {
      // use the number of targets and reserve two threads: one for the source
      // and one for an source-observer
      final int poolSize = numOfTargetThreads + 2;
      // resize queue, if needed
      if (poolSize > workQueueSize) {
        final Runnable[] queueContent = this.workQueue.toArray(
                new Runnable[this.workQueue.size()]);
        final int newQueueSize = (int) (poolSize * 1.5);
        this.workQueue = new ArrayBlockingQueue<>(newQueueSize);
        this.workQueue.addAll(Arrays.asList(queueContent));
        this.workQueueSize = newQueueSize;
      }
      return poolSize;
    }

    /**
     * Set the number of targets in use. This reserves space in the fixed
     * thread pool for the {@link Processing.Source} and a
     * {@link Processing.SourceObserver}.
     *
     * @param numOfTargets Number of targets to use
     */
    protected void setTargetThreadsCount(final int numOfTargets) {
      final int poolSize = calcPoolSize(numOfTargets);
      this.threadPool.setCorePoolSize(poolSize);
      this.threadPool.setMaximumPoolSize(poolSize);
    }

    /**
     * Here we add our jobs to working queue.
     *
     * @param task a Runnable task
     */
    void runTask(final Runnable task) {
      threadPool.execute(task);
    }

    /**
     * Submit the {@link Processing.Source} to the thread queue, returning a
     * {@link Future} to track it's state.
     *
     * @param task Source runnable
     * @return Future to track the state
     */
    Future runSource(final Runnable task) {
      return threadPool.submit(task);
    }

    /**
     * Shutdown the pool.
     */
    void shutDown() {
      threadPool.shutdown();
    }
  }
}
