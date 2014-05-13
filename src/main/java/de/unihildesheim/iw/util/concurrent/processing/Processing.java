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

import de.unihildesheim.iw.util.TimeMeasure;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
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
   * Default number of target threads to run. Defaults to the 1/3 of
   * available processors.
   */
  public static final int THREADS =
      Runtime.getRuntime().availableProcessors() / 3;
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      Processing.class);
  /**
   * Thread pool handling thread execution.
   */
  private static volatile ProcessingThreadPoolExecutor executor;
  /**
   * Processing {@link Source}.
   */
  private Source source;
  /**
   * Processing {@link Target}.
   */
  private Target target;
  /**
   * Latch that tracks the running threads.
   */
  private CountDownLatch threadTrackingLatch;

  /**
   * Creates a new processing pipe with the given {@link Source} and {@link
   * Target}.
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
   * Plain constructor. Source and target must be set manually to start
   * processing.
   */
  public Processing() {
    initPool();
  }

  /**
   * Shuts down the thread pool maintained by this instance.
   */
  public static void shutDown() {
    if (executor != null) {
      LOG.trace("Shutting down thread pool.");
      executor.shutDown();
      executor = null;
    }
  }

  /**
   * Reuse this instance by setting a new {@link Source}.
   *
   * @param newSource New source to use
   */
  public void setSource(final Source newSource) {
    if (newSource == null) {
      throw new IllegalArgumentException("Source was null.");
    }
    this.source = newSource;
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
    if (newTarget == null) {
      throw new IllegalArgumentException("Target was null.");
    }
    if (newTarget.getSource() == null) {
      throw new IllegalArgumentException("Source was null.");
    }
    this.source = newTarget.getSource();
    this.target = newTarget;
    return this;
  }

  /**
   * Reuse this instance by setting a new {@link Target}.
   *
   * @param newTarget New target to use
   */
  public void setTarget(final Target newTarget) {
    if (newTarget == null) {
      throw new IllegalArgumentException("Target was null.");
    }
    this.target = newTarget;
  }

  /**
   * Initialize the thread pool. Adds a shutdown hook to terminate the pool.
   */
  private void initPool() {
    if (executor == null) {
      LOG.debug("Initialize thread pool.");
      executor = new ProcessingThreadPoolExecutor();
      Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook(),
          "Processing_shutdownHandler"));
    }
  }

  /**
   * Start processing with the defined {@link Source} and {@link Target} with
   * the default number of threads.
   */
  public void process()
      throws ProcessingException {
    process(Processing.THREADS);
  }

  /**
   * Start processing with the defined {@link Source} and {@link Target} with a
   * specified number of threads.
   *
   * @param maxThreadCount Maximum number of threads to use
   */
  @SuppressWarnings("ThrowableResultIgnored")
  public void process(final int maxThreadCount)
      throws ProcessingException {
    final int threadCount;
    if (maxThreadCount > Processing.THREADS) {
      threadCount = Processing.THREADS;
    } else {
      threadCount = maxThreadCount;
    }

    if (this.source == null) {
      throw new IllegalStateException("No source set.");
    }
    if (this.target == null) {
      throw new IllegalStateException("No target set.");
    }

    initPool();

    final Collection<Target<Boolean>> targets = new ArrayList<>(threadCount);
    final Collection<Future<Boolean>> targetStates = new ArrayList<>
        (threadCount);
    this.threadTrackingLatch = new CountDownLatch(threadCount);

    LOG.debug("Spawning {} Processing-Target threads.", threadCount);
    for (int i = 0; i < threadCount; i++) {
      final Target<Boolean> aTarget = this.target.newInstance();
      aTarget.setLatch(this.threadTrackingLatch);
      targets.add(aTarget);
    }

    LOG.trace("Starting Processing-Source.");
    final Future<Long> sourceThread = executor.runSource(this.source);
    LOG.trace("Starting Processing-Observer.");
    final SourceObserver sourceObserver = new SourceObserver(this.source);
    final Future<Double> sourceTime = executor.runObserver(sourceObserver);

    LOG.trace("Starting Processing-Target threads.");
    for (final Target aTarget : targets) {
      targetStates.add(executor.runTarget(aTarget));
    }

    try {
      // wait until targets have finished
      this.threadTrackingLatch.await();
    } catch (InterruptedException ex) {
      // TODO: ugly skip source finished exception
      if (!(ex.getCause() instanceof ProcessingException
          .SourceHasFinishedException)) {
        throw new ProcessingException.SourceFailedException("Caught " +
            "exception while tracking source state.", ex.getCause());
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
      this.threadTrackingLatch.await();
    } catch (InterruptedException ex) {
      throw new ProcessingException("Processing interrupted.", ex);
    }

    // check target states
    for (Future<Boolean> state : targetStates) {
      try {
        state.get();
      } catch (InterruptedException e) {
        throw new ProcessingException.TargetFailedException("Target " +
            "interrupted.", e.getCause());
      } catch (ExecutionException e) {
        throw new ProcessingException.TargetFailedException(
            "Target throwed an exception.", e.getCause());
      }
    }

    try {
      // retrieve result from source
      LOG.debug("Source finished with {} items after {}.",
          sourceThread.get(3, TimeUnit.SECONDS),
          TimeMeasure.getTimeString(sourceTime.get(1, TimeUnit.SECONDS))
      );
    } catch (TimeoutException ex) {
      throw new ProcessingException.TargetFailedException(
          "Source finished without result. "
              + "There may be processing errors."
      );
    } catch (InterruptedException e) {
      throw new ProcessingException.SourceFailedException("Source interrupted" +
          ".", e.getCause());
    } catch (ExecutionException e) {
      // TODO: ugly skip source finished exception
      if (!(e.getCause() instanceof ProcessingException
          .SourceHasFinishedException)) {
        throw new ProcessingException.SourceFailedException(
            "Source throwed an exception.", e.getCause());
      }
    }

//    assert executor.threadPool.getActiveCount() == 0 :
//        "There are (~" + executor.threadPool.getActiveCount() + ") " +
//            "tasks left in the pool.";

    LOG.trace("Processing finished.");
  }

  /**
   * Thread pool manager for processing threads.
   */
  private static class ProcessingThreadPoolExecutor {

    /**
     * Maximum seconds a thread may be idle in the pool, before it gets
     * removed.
     */
    private static final long KEEPALIVE_TIME = 5L;
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
      this.threadPool = new ThreadPoolExecutor(Processing.THREADS,
          Integer.MAX_VALUE, KEEPALIVE_TIME, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>());
    }

//    /**
//     * Here we add our jobs to working queue.
//     *
//     * @param task a Runnable task
//     */
//    void runTask(final Runnable task) {
//      if (task == null) {
//        throw new IllegalArgumentException("Task was null.");
//      }
//      threadPool.execute(task);
//    }

    /**
     * Submit the {@link Source} to the thread queue, returning a {@link Future}
     * to track it's state.
     *
     * @param task Source runnable
     * @return Future to track the state
     */
    Future<Long> runSource(final Source<Long> task) {
      if (task == null) {
        throw new IllegalArgumentException("Source was null.");
      }
      return threadPool.submit(task);
    }

    /**
     * Submit the {@link SourceObserver} to the thread queue, returning a {@link
     * Future} to track it's state.
     *
     * @param task Source runnable
     * @return Future to track the state
     */
    Future<Double> runObserver(final SourceObserver<Double> task) {
      if (task == null) {
        throw new IllegalArgumentException("Source was null.");
      }
      return threadPool.submit(task);
    }

    /**
     * Submit a task to the thread queue, returning a {@link Future} to track
     * it's state.
     *
     * @param task Target to run
     * @return Future to track the state
     */
    Future<Boolean> runTarget(final Target<Boolean> task) {
      if (task == null) {
        throw new IllegalArgumentException("Target was null.");
      }
      return threadPool.submit(task);
    }

    /**
     * Shutdown the pool.
     */
    void shutDown() {
      threadPool.shutdown();
    }
  }

  /**
   * Shutdown hook for processing pool.
   */
  private static final class ShutDownHook
      implements Runnable {

    @Override
    public void run() {
      Processing.shutDown();
    }
  }
}
