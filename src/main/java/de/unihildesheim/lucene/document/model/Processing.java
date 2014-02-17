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
package de.unihildesheim.lucene.document.model;

import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.util.TimeMeasure;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.LoggerFactory;

/**
 * One source, one or more targets.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class Processing {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          Processing.class);

  /**
   * Processing {@link Source}.
   */
  private final Source source;
  /**
   * Processing {@link Target}.
   */
  private final Target target;

  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "ProcessPipe_";
  /**
   * Default number of target threads to run.
   */
  private static final int THREADS = ClarityScoreConfiguration.INSTANCE.
          getInt(CONF_PREFIX + "targetThreadsCount", Runtime.getRuntime().
                  availableProcessors());
  /**
   * Latch that tracks the running threads.
   */
  private final CountDownLatch threadTrackingLatch;

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
    this.threadTrackingLatch = new CountDownLatch(THREADS);
  }

  /**
   * Creates a new processing pipe, deriving the {@link Source} from the
   * {@link Target}.
   *
   * @see ProcessPipe#ProcessPipe(Source, Target)
   * @param newTarget Processing {@link Target}
   */
  public Processing(final Target newTarget) {
    this(newTarget.getSource(), newTarget);
  }

  /**
   * Start processing with the defined {@link Source} and {@link Target}.
   */
  public void process() {
    final Collection<Target> targets = new ArrayList<>(THREADS);
    final Collection<Thread> targetThreads = new ArrayList<>(THREADS);

    LOG.debug("Spawning {} Processing-Target threads.", THREADS);
    for (int i = 0; i < THREADS; i++) {
      final Target aTarget = this.target.newInstance(this.threadTrackingLatch);
      targets.add(aTarget);
      targetThreads.add(new Thread(aTarget, "Target-" + i));
    }

    LOG.debug("Starting Processing-Source.");
    final Thread sourceThread = new Thread(this.source);
    sourceThread.start();

    LOG.debug("Starting Processing-Target threads.");
    for (Thread thread : targetThreads) {
      thread.start();
    }

    while (sourceThread.isAlive()) {
      // wait until source has finished
    }

    LOG.debug("Processing-Source finished. Terminating Targets.");
    for (Target aTarget : targets) {
      aTarget.terminate();
    }

    LOG.debug("Awaiting Processing-Targets termination.");
    try {
      this.threadTrackingLatch.await();
    } catch (InterruptedException ex) {
      LOG.error("Processing interrupted.", ex);
    }

    LOG.debug("Processing finished.");
  }

  private interface ObservableSource {

    /**
     * Get the number of items that will be provided.
     *
     * @return Number of items, or <tt>null</tt> if there is no such
     * information.
     */
    Integer getItemCount();

    /**
     * Get the number of items already served.
     *
     * @return Number of items served
     */
    int getSourcedItemCount();
  }

  private static class ProcessStatus implements Runnable {

    /**
     * {@link Source} to observe.
     */
    private final ObservableSource source;
    /**
     * Overall time taken.
     */
    private final TimeMeasure overallTime;
    /**
     * Time taken between two status messages.
     */
    private final TimeMeasure runTime;
    /**
     * Flag indicating if {@link Source} provides the number of items it will
     * provide.
     */
    private boolean hasItemCount;
    /**
     * Update interval in seconds. Used,if {@link Source} does not provide the
     * expected number of items.
     */
    private static final int INTERVAL = 15;
    /**
     * Flag indicating, if this instance should terminate.
     */
    private volatile boolean terminate;
    /**
     * Thread running this {@link Runnable} instance.
     */
    private Thread oThread;

    /**
     * Attach a status observer to a specified {@link Source}.
     *
     * @param newSource Source whose process to observe
     */
    ProcessStatus(final ObservableSource newSource) {
      this.terminate = false;
      this.source = newSource;
      this.overallTime = new TimeMeasure();
      this.runTime = new TimeMeasure();
    }

    /**
     * Start observing the process.
     */
    public void observe() {
      oThread = new Thread(this);
      oThread.start();
    }

    /**
     * Stop observing and terminate.
     */
    public void terminate() {
      this.terminate = true;
    }

    @Override
    public void run() {
      this.overallTime.start();
      this.runTime.start();

      int itemCount, step;
      int lastStatus = 0;
      int status = 0;
      if (this.source.getItemCount() != null) {
        this.hasItemCount = true;
        step = (int) (this.source.getItemCount() * 0.01);
        itemCount = this.source.getItemCount();
      } else {
        this.hasItemCount = false;
        step = 0;
        itemCount = 0;
      }
      while (!this.terminate) {
        if (!this.hasItemCount) {
          // timed status
          if (this.runTime.getElapsedSeconds() > INTERVAL) {
            this.runTime.stop();
            LOG.info("Processing {} of {} items after {}s, running for {}.",
                    this.source.hashCode(), this.source.getSourcedItemCount(),
                    this.runTime.getElapsedSeconds(), this.overallTime.
                    getElapsedTimeString());
            this.runTime.start();
          }
        } else {
          // status based on progress
          status = this.source.getSourcedItemCount();
          if (lastStatus < status) {
            lastStatus = status;
            if (lastStatus % step == 0) {
              runTime.stop();
              final long estimate = (long) ((itemCount - lastStatus)
                      / (lastStatus / overallTime.getElapsedSeconds()));
              LOG.info("Processing {} of {} items ({}%). "
                      + "{}s since last status. Running for {}. "
                      + "Estimated time needed {}.", lastStatus, itemCount,
                      ((lastStatus * 100) / itemCount), runTime.stop().
                      getElapsedSeconds(), overallTime.getElapsedTimeString(),
                      TimeMeasure.getTimeString(estimate));
              runTime.start();
            }
          }
        }
      }
      this.overallTime.stop();
      LOG.info("Source finished after {}.", this.overallTime.
              getElapsedTimeString());
    }
  }

  /**
   * Base class for all {@link Source} implementations.
   * <p>
   * A {@link Source} is meant to be used once, so starting it more than one
   * time may lead to unexpected behavior.
   * <p>
   * If the {@link Source} has finished processing items it should decrement
   * the latch (if set), to indicate it has finished. This is handled by
   * {@link #stop()} which must be called by the implementing class when
   * finished.
   *
   * @param <T> Type this {@link Source} provides
   */
  @SuppressWarnings("PublicInnerClass")
  public abstract static class Source<T> implements Runnable {

    /**
     * Signal the {@link Source}, that it should stop generating items.
     */
    abstract void stop();

    /**
     * Check, if this {@link Source} has more items to process.
     *
     * @return True, if there are more items for processing
     */
    public abstract boolean hasNext();

    /**
     * Get the next item to process.
     *
     * @return Next item to process
     */
    public abstract T next();

    /**
     * Get the number of items to process.
     *
     * @return Number of items to process
     */
    public abstract Integer getItemCount();
  }

  /**
   * Wraps the given {@link Collection} as {@link Source}. Thread safety is
   * handled by using a synchronized collection wrapper. If the collection
   * gets modified while being used as source, the behavior is undefined.
   *
   * @param <T> Type of the collections elements
   */
  public static final class CollectionSource<T> extends Source<T> implements
          ObservableSource {

    /**
     * Wrapped collection acting as source.
     */
    private Collection<T> collection;
    /**
     * Iterator over the wrapped source.
     */
    private Iterator<T> itemsIt;
    /**
     * Flag indicating if this {@link Source} should terminate.
     */
    private volatile boolean terminate;
    /**
     * Flag indicating if this {@link Source} is running.
     */
    private volatile boolean isRunning;
    /**
     * Synchronize lock.
     */
    final Object lock = new Object();

    private AtomicInteger sourcedItemCount = new AtomicInteger(0);

    /**
     * Wrap the specified collection using it as source.
     *
     * @param coll Collection to wrap
     */
    public CollectionSource(final Collection<T> coll) {
      this.terminate = false;
      this.collection = Collections.synchronizedCollection(coll);
    }

    /**
     * Check, if instance is running. Optionally throwing an exception, if
     * it's not the case.
     *
     * @param fail If true, throws an exception, if not running
     */
    private boolean checkRunning(final boolean fail) {
      if (this.isRunning) {
        return true;
      }
      if (fail) {
        throw new IllegalStateException("Source is not running.");
      }
      return false;
    }

    @Override
    public synchronized boolean hasNext() {
      if (!checkRunning(false)) {
        return false;
      }
      if (itemsIt.hasNext()) {
        return true;
      } else {
        stop();
        return false;
      }
    }

    @Override
    public synchronized T next() {
      checkRunning(true);
      sourcedItemCount.getAndIncrement();
      return itemsIt.next();
    }

    @Override
    public Integer getItemCount() {
      checkRunning(true);
      return this.collection.size();
    }

    @Override
    public void run() {
      ProcessStatus status = new ProcessStatus(this);
      if (checkRunning(false)) {
        throw new IllegalStateException(
                "Source is already running.");
      }
      this.itemsIt = this.collection.iterator();
      this.isRunning = true;
      status.observe();
      while (!this.terminate) {
        // wait until stop() is called
      }
      status.terminate();
    }

    @Override
    void stop() {
      checkRunning(true);
      this.terminate = true;
    }

    @Override
    public int getSourcedItemCount() {
      return sourcedItemCount.intValue();
    }
  }

  /**
   * {@link Target} for a processing {@link Source}. Implements
   * {@link Runnable} to be executed by using {@link Thread}s.
   *
   * @param <T> Type that this {@link Target} accepts
   */
  @SuppressWarnings("PublicInnerClass")
  public static abstract class Target<T> implements Runnable {

    /**
     * {@link Source} used by this instance.
     */
    private final Source<T> source;

    /**
     * Create a new {@link Target} with a specific {@link Source}.
     *
     * @param newSource <tt>Source</tt> to use
     */
    public Target(final Source<T> newSource) {
      if (newSource == null) {
        throw new IllegalArgumentException("Source was null.");
      }
      this.source = newSource;
    }

    /**
     * Get the {@link Source} for this {@link Target} instance.
     *
     * @return {@link Source} used by this {@link Target}.
     */
    public final Source<T> getSource() {
      return this.source;
    }

    /**
     * Set the termination flag for this {@link Runnable}. If the instance
     * terminates, the provided {@link CountdownLatch} must be decremented.
     */
    public abstract void terminate();

    /**
     * Create a new {@link Target} instance
     *
     * @param latch Shared latch to track running threads.
     * @return New {@link Target} instance
     */
    public abstract Target<T> newInstance(final CountDownLatch latch);
  }
}
