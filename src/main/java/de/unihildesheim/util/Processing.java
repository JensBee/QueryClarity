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
package de.unihildesheim.util;

import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.util.Processing.Source;
import de.unihildesheim.util.Processing.Target;
import de.unihildesheim.util.ProcessingException.SourceHasFinishedException;
import de.unihildesheim.util.ProcessingException.SourceIsRunningException;
import de.unihildesheim.util.ProcessingException.SourceNotReadyException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
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
  public static final int THREADS = ClarityScoreConfiguration.getInt(
          CONF_PREFIX + "targetThreadsCount", Runtime.getRuntime().
          availableProcessors());
  /**
   * Latch that tracks the running threads.
   */
  private CountDownLatch threadTrackingLatch;

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
    final Collection<Target> targets = new ArrayList<>(threadCount);
    final Collection<Thread> targetThreads = new ArrayList<>(threadCount);
    this.threadTrackingLatch = new CountDownLatch(threadCount);

    LOG.debug("Spawning {} Processing-Target threads.", threadCount);
    for (int i = 0; i < threadCount; i++) {
      final Target aTarget = this.target.newInstance(this.threadTrackingLatch);
      targets.add(aTarget);
      targetThreads.add(new Thread(aTarget, "Target-" + i));
    }

    LOG.debug("Starting Processing-Source.");
    final Thread sourceThread = new Thread(this.source);
    sourceThread.start();

    while (!this.source.isRunning()) {
      // wait for source to start
    }

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

  /**
   * Interface to provide access for {@link Processing.SourceObserver} access.
   */
  @SuppressWarnings("PublicInnerClass")
  public interface ObservableSource {

    /**
     * Get the number of items that will be provided.
     *
     * @return Number of items, or <tt>null</tt> if there is no such
     * information.
     */
    Integer getItemCount() throws ProcessingException;

    /**
     * Get the number of items already served.
     *
     * @return Number of items served
     */
    int getSourcedItemCount();
  }

  /**
   * Observer for {@link Processing.Source}es, printing status messages to the
   * system log.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SourceObserver implements Runnable {

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
     * Final number of items provided (optional).
     */
    private Integer finalSourcedCount = null;

    /**
     * Attach a status observer to a specified {@link Source}.
     *
     * @param newSource Source whose process to observe
     */
    public SourceObserver(final ObservableSource newSource) {
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

    /**
     * Stop observing and terminate.
     *
     * @param sourcedCount Final number of items provided
     */
    public void terminate(final Integer sourcedCount) {
      this.terminate = true;
      this.finalSourcedCount = sourcedCount;
    }

    @Override
    public void run() {
      try {
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
                      this.source.hashCode(), this.source.
                      getSourcedItemCount(),
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
                        getElapsedSeconds(), overallTime.
                        getElapsedTimeString(),
                        TimeMeasure.getTimeString(estimate));
                runTime.start();
              }
            }
          }
        }
        this.overallTime.stop();
        if (this.finalSourcedCount == null) {
          LOG.info("Source finished after {}.", this.overallTime.
                  getElapsedTimeString());
        } else {
          LOG.info("Source finished after {} with {} items.",
                  this.overallTime.
                  getElapsedTimeString(), this.finalSourcedCount);
        }
      } catch (ProcessingException ex) {
        LOG.error("Caught exception while running observer.", ex);
      }
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
     * Flag indicating if this {@link Source} is running.
     */
    private volatile boolean isRunning;

    /**
     * Flag indicating if this {@link Source} has finished.
     */
    private volatile boolean isFinished;

    /**
     * Observer to use, if {@link Processing.Source} is of
     * {@link Processing.ObservableSource} type.
     */
    private final SourceObserver observer;

    /**
     * Default constructor attaching a {@link Processing.SourceObserver} if
     * applicable.
     */
    protected Source() {
      this.isRunning = false;
      this.isFinished = false;
      if (this instanceof ObservableSource) {
        this.observer = new SourceObserver((ObservableSource) this);
      } else {
        this.observer = null;
      }
    }

    @Override
    public synchronized void run() {
      if (isRunning()) {
        throw new ProcessingException.SourceIsRunningException();
      }
      if (isFinished()) {
        throw new ProcessingException.SourceHasFinishedException();
      }
      setRunning();
      if (this.observer != null) {
        this.observer.observe();
      }
      awaitTermination();
      if (this.observer != null) {
        this.observer.terminate(((ObservableSource) this).
                getSourcedItemCount());
      }
    }

    /**
     * Signal the {@link Source}, that it should stop generating items.
     */
    protected final synchronized void stop() throws ProcessingException {
      checkRunning(true);
      this.isRunning = false;
      this.isFinished = true;
      this.notifyAll();
    }

    /**
     * Get the next item to process.
     *
     * @return Next item to process
     */
    public abstract T next() throws ProcessingException;

    /**
     * Get the number of items to process.
     *
     * @return Number of items to process
     */
    public abstract Integer getItemCount() throws ProcessingException;

    /**
     * Status, if the source is running (providing data).
     *
     * @return True, if source is ready to serve data
     */
    public final synchronized boolean isRunning() {
      return this.isRunning;
    }

    /**
     * Status, if the source is running (providing data).
     *
     * @return True, if source is ready to serve data
     */
    public final synchronized boolean isFinished() {
      return this.isFinished;
    }

    /**
     * Check, if instance is running. Optionally throwing an exception, if
     * it's not the case.
     *
     * @param fail If true, throws an exception, if not running
     * @return True, if running
     * @throws
     * de.unihildesheim.util.ProcessingException.SourceNotReadyException
     * Thrown, if the {@link Processing.Source} has not been started and
     * <tt>fail</tt> is <tt>true</tt>
     */
    protected final synchronized boolean checkRunning(final boolean fail)
            throws SourceNotReadyException {
      if (this.isRunning) {
        return true;
      }
      if (fail) {
        if (this.isFinished) {
          throw new SourceHasFinishedException();
        }
        if (!this.isRunning) {
          throw new SourceNotReadyException();
        }
      }
      return false;
    }

    /**
     * Set the flag indicating this {@link Processing.Source} is running.
     */
    protected final void setRunning() {
      this.isRunning = true;
    }

    /**
     * Await termination.
     */
    protected final synchronized void awaitTermination() {
      try {
        this.wait();
      } catch (InterruptedException ex) {
        LOG.error("Interrupted.", ex);
      }
    }
  }

  /**
   * Wraps the given {@link Collection} as {@link Source}. Thread safety is
   * handled by using a synchronized collection wrapper. If the collection
   * gets modified while being used as source, the behavior is undefined.
   *
   * @param <T> Type of the collections elements
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class CollectionSource<T> extends Source<T> implements
          ObservableSource {

    /**
     * Wrapped collection acting as source.
     */
    private Collection<T> collection;
    /**
     * Iterator over the wrapped source.
     */
    private volatile Iterator<T> itemsIt;
    /**
     * Number of provided items.
     */
    private volatile int sourcedItemCount;

    /**
     * Wrap the specified collection using it as source.
     *
     * @param coll Collection to wrap
     */
    public CollectionSource(final Collection<T> coll) {
      super();
      this.sourcedItemCount = 0;
      this.collection = coll;
    }

    @Override
    public synchronized T next() throws ProcessingException {
      checkRunning(true);
      if (itemsIt.hasNext()) {
        this.sourcedItemCount++;
        return itemsIt.next();
      }
      stop();
      return null;
    }

    @Override
    public Integer getItemCount() throws ProcessingException {
      checkRunning(true);
      return this.collection.size();
    }

    @Override
    public synchronized void run() {
      if (isRunning()) {
        throw new SourceIsRunningException();
      }
      this.itemsIt = this.collection.iterator();
      super.run();
    }

    @Override
    public int getSourcedItemCount() {
      return this.sourcedItemCount;
    }
  }

  /**
   * {@link Target} for a processing {@link Source}. Implements
   * {@link Runnable} to be executed by using {@link Thread}s.
   *
   * @param <T> Type that this {@link Target} accepts
   */
  @SuppressWarnings("PublicInnerClass")
  public abstract static class Target<T> implements Runnable {

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
