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
package de.unihildesheim.iw.util.concurrent.processing;

import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.util.TimeMeasure;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility class to simplify concurrent processing of large data-sets. One
 * source, one or more targets.
 *
 * @author Jens Bertram
 */
public final class Processing {

  /**
   * Default number of target threads to run. Defaults to the number of
   * available processors.
   */
  public static final int THREADS;

  /**
   * Logger instance for this class.
   */
  static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      Processing.class);

  /**
   * Prefix used to store {@link GlobalConfiguration configuration}.
   */
  private static final String IDENTIFIER = "Processing";

  /**
   * Thread pool manager.
   */
  private static final ProcessingThreadPoolExecutor POOL_EXECUTOR;

  static {
    final Integer maxThreads = GlobalConfiguration.conf().getAndAddInteger
        (IDENTIFIER + "_max-threads", 0);
    final int processors = Runtime.getRuntime().availableProcessors();
    if (maxThreads == null || maxThreads <= 0) {
      THREADS = processors;
    } else {
      THREADS = Math.min(processors, maxThreads);
    }

    LOG.debug("Initialize thread pool.");
    POOL_EXECUTOR = new ProcessingThreadPoolExecutor();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        LOG.trace("Shutting down thread pool.");
        getPoolExecutor().shutDown();
      }
    }, Processing.class.getSimpleName() + "_shutdownHandler"));
  }

  /**
   * Processing {@link Source}.
   */
  private Source source;

  /**
   * Processing {@link Target}.
   */
  private Target target;

  /**
   * Creates a new processing pipe, deriving the {@link Source} from the {@link
   * Target}.
   *
   * @param newTarget Processing {@link Target}
   * @see Processing#Processing(Source, Target)
   */
  public Processing(final Target newTarget) {
    this(newTarget.getSource(), newTarget);
  }

  /**
   * Creates a new processing pipe with the given {@link Source} and {@link
   * Target}.
   *
   * @param newSource Processing {@link Source}
   * @param newTarget Processing {@link Target}
   */
  public Processing(final Source newSource, final Target newTarget) {
    this.source = Objects.requireNonNull(newSource, "Source was null.");
    this.target = Objects.requireNonNull(newTarget, "Target was null.");
  }

  /**
   * Plain empty constructor. Source and target must be set manually to start
   * processing.
   */
  public Processing() {
  }

  /**
   * Get the thread pool handling thread execution.
   *
   * @return thread pool handling thread executor
   */
  static ProcessingThreadPoolExecutor getPoolExecutor() {
    return POOL_EXECUTOR;
  }

  /**
   * Reuse this instance by setting a new {@link Source}.
   *
   * @param newSource New source to use
   */
  public void setSource(final Source newSource) {
    this.source = Objects.requireNonNull(newSource, "Source was null.");
  }

  /**
   * Creates a new processing pipe source and target, deriving the {@link
   * Source} from the {@link Target}.
   *
   * @param newTarget Processing target
   * @return self reference
   */
  public Processing setSourceAndTarget(final Target
      newTarget) {
    this.target = Objects.requireNonNull(newTarget, "Target was null.");
    this.source = Objects.requireNonNull(newTarget.getSource(),
        "Source was null.");
    return this;
  }

  /**
   * Reuse this instance by setting a new {@link Target}.
   *
   * @param newTarget New target to use
   */
  public void setTarget(final Target newTarget) {
    this.target = Objects.requireNonNull(newTarget, "Target was null.");
  }

  /**
   * Start processing with the defined {@link Source} and {@link Target} with
   * the default number of threads.
   *
   * @throws ProcessingException Thrown, if any process encountered an error
   */
  public void process()
      throws ProcessingException {
    process(THREADS);
  }

  /**
   * Start processing with the defined {@link Source} and {@link Target} with a
   * specified number of threads.
   *
   * @param maxThreadCount Maximum number of threads to use
   * @return Number of processed items. May be {@code null} on errors.
   * @throws ProcessingException Thrown, if any process encountered an error
   */
  public Long process(final int maxThreadCount)
      throws ProcessingException {
    final int threadCount;
    if (maxThreadCount > THREADS) {
      threadCount = THREADS;
    } else {
      threadCount = maxThreadCount;
    }

    if (this.source == null) {
      throw new IllegalStateException("No source set.");
    }
    if (this.target == null) {
      throw new IllegalStateException("No target set.");
    }

    final Collection<Target<?>> targets = new ArrayList<>(threadCount);
    final Collection<Future<?>> targetStates = new ArrayList<>
        (threadCount);
    // Latch that tracks the running threads.
    final CountDownLatch threadTrackingLatch = new CountDownLatch(threadCount);

    LOG.debug("Using {} Processing-Target threads.", threadCount);
    for (int i = 0; i < threadCount; i++) {
      final Target<?> aTarget;
      try {
        aTarget = this.target.newInstance();
      } catch (final Exception e) {
        throw new TargetException.TargetFailedException(
            "Target throwed an exception.", e);
      }
      aTarget.setLatch(threadTrackingLatch);
      targets.add(aTarget);
    }

    LOG.trace("Starting Processing-Source.");
    final Future<Long> sourceThread = POOL_EXECUTOR.runSource(this.source);
    LOG.trace("Starting Processing-Observer.");
    final SourceObserver sourceObserver = new SourceObserver(threadCount,
        this.source);
    final Future<Double> sourceTime = POOL_EXECUTOR.runObserver(sourceObserver);

    LOG.trace("Starting Processing-Target threads.");
    for (final Target<?> aTarget : targets) {
      targetStates.add(POOL_EXECUTOR.runTarget(aTarget));
    }

    try {
      // wait until targets have finished
      threadTrackingLatch.await();
    } catch (final InterruptedException ex) {
      // TODO: ugly skip source finished exception
      if (!(ex
          .getCause() instanceof SourceException.SourceHasFinishedException)) {
        throw new SourceException.SourceFailedException("Caught " +
            "exception while tracking source state.", ex);
      }
    }

    // terminate observer
    LOG.trace("Processing-Source finished. Terminating observer.");
    sourceObserver.terminate();

    LOG.trace("Processing-Source finished. Terminating Targets.");
    for (final Target aTarget : targets) {
      aTarget.terminate();
    }

    LOG.trace("Awaiting Processing-Targets termination.");
    try {
      threadTrackingLatch.await();
    } catch (final InterruptedException ex) {
      throw new ProcessingException("Processing interrupted.", ex);
    }

    // check target states
    for (final Future<?> state : targetStates) {
      try {
        state.get();
      } catch (final InterruptedException e) {
        throw new TargetException.TargetFailedException("Target " +
            "interrupted.", e);
      } catch (final ExecutionException e) {
        throw new TargetException.TargetFailedException(
            "Target throwed an exception.", e);
      }
    }

    Long processedItems = 0L;
    try {
      // retrieve result from source
      processedItems = sourceThread.get(3L, TimeUnit.SECONDS);
      LOG.debug("Source finished with {} items after {}.",
          processedItems,
          TimeMeasure.getTimeString(sourceTime.get(1L, TimeUnit.SECONDS))
      );
    } catch (final TimeoutException ex) {
      sourceThread.cancel(true);
      throw new TargetException.TargetFailedException(
          "Source finished without result. "
              + "There may be processing errors.", ex);
    } catch (final InterruptedException e) {
      throw new SourceException.SourceFailedException("Source interrupted" +
          ".", e);
    } catch (final ExecutionException e) {
      // TODO: ugly skip source finished exception
      if (!(e.getCause() instanceof
          SourceException.SourceHasFinishedException)) {
        throw new SourceException.SourceFailedException(
            "Source throwed an exception.", e);
      }
    }

    LOG.trace("Processing finished.");
    return processedItems;
  }

  /**
   * Thread pool manager for processing threads.
   */
  private static final class ProcessingThreadPoolExecutor {

    /**
     * Maximum seconds a thread may be idle in the pool, before it gets removed
     * (seconds).
     */
    private static final long KEEPALIVE_TIME = 60L;
    /**
     * Thread pool manager.
     */
    private final ThreadPoolExecutor threadPool;

    /**
     * Create a new processing thread pool manager. The maximum number of
     * threads concurrently run is determined by the number of processors
     * available.
     */
    ProcessingThreadPoolExecutor() {
      final int maxPool = (int) ((double) THREADS * 1.75);
      LOG.debug("Initializing thread pool executor. core={} maxPool={} ka={}",
          THREADS, maxPool, KEEPALIVE_TIME);
      this.threadPool = new ThreadPoolExecutor(THREADS,
          maxPool, KEEPALIVE_TIME, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>());
    }

    /**
     * Submit the {@link Source} to the thread queue, returning a {@link Future}
     * to track it's state.
     *
     * @param task Source runnable
     * @return Future to track the state
     */
    @SuppressWarnings("unchecked")
    Future<Long> runSource(final Source task) {
      return this.threadPool
          .submit(Objects.requireNonNull((Callable<Long>) task,
              "Task was null."));
    }

    /**
     * Submit the {@link SourceObserver} to the thread queue, returning a {@link
     * Future} to track it's state.
     *
     * @param task Source runnable
     * @return Future to track the state
     */
    @SuppressWarnings("unchecked")
    Future<Double> runObserver(final SourceObserver task) {
      return this.threadPool
          .submit(Objects.requireNonNull((Callable<Double>) task,
              "Task was null."));
    }

    /**
     * Submit a task to the thread queue, returning a {@link Future} to track
     * it's state.
     *
     * @param task Target to run
     * @return Future to track the state
     */
    Future<?> runTarget(final Target<?> task) {
      return this.threadPool
          .submit(Objects.requireNonNull(task, "Task was null."));
    }

    /**
     * Shutdown the pool.
     */
    void shutDown() {
      this.threadPool.shutdown();
    }
  }
}
