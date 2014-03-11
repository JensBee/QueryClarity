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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.LoggerFactory;

/**
 * {@link Target} for a processing {@link Source}. Implements {@link Runnable}
 * to be executed by using {@link Thread}s.
 *
 * @param <T> Type that this {@link Target} accepts
 */
public abstract class Target<T> implements Runnable {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          Target.class);
  /**
   * {@link Source} used by this instance.
   */
  private final Source<T> source;
  /**
   * Flag to indicate, if this {@link Runnable} should terminate.
   */
  private volatile boolean terminate;
  /**
   * Shared latch to track running threads.
   */
  private CountDownLatch latch;

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
    this.terminate = false;
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
  public final void terminate() {
    if (this.terminate) {
      LOG.trace("({}) Received termination signal, but already terminating.",
              getName());
    } else {
      LOG.trace("({}) Received termination signal.", getName());
      this.terminate = true;
    }
  }

  /**
   * Check, if the termination flag is set.
   *
   * @return True, if instance should terminate
   */
  public final boolean isTerminating() {
    return this.terminate;
  }

  /**
   * Create a new {@link Target} instance.
   *
   * @return New {@link Target} instance
   */
  public abstract Target<T> newInstance();

  /**
   * Set the thread tracking latch.
   *
   * @param newLatch Shared latch to track running threads.
   */
  public final void setLatch(final CountDownLatch newLatch) {
    this.latch = newLatch;
  }

  /**
   * Get a unique name for this {@link Runnable}.
   *
   * @return Name for this instance
   */
  public final String getName() {
    return this.getClass().getSimpleName() + "-" + this.hashCode();
  }

  /**
   * Equivalent for <tt>run()</tt> function called by abstract Target class.
   *
   * @throws java.lang.Exception Any exception is catched by main function
   */
  public abstract void runProcess() throws Exception;

  @Override
  public final void run() {
    try {
      LOG.trace("({}) Starting.", getName());
      getSource().awaitStart();
      runProcess();
    } catch (ProcessingException.SourceHasFinishedException ex) {
      LOG.error("({}) Source has finished unexpectedly.", getName(), ex);
    } catch (Throwable t) { // make sure we catch everything
      LOG.error("({}) Caught exception.", getName(), t.getStackTrace());
    } finally {
      this.terminate = true;
      LOG.trace("({}) Terminating.", getName());
      this.latch.countDown();
    }
  }

  /**
   * Debug {@link Target} to test {@link Source}es.
   *
   * @param <T> Type of this target
   */
  @SuppressWarnings("ProtectedInnerClass")
  protected final static class TargetTest<T> extends Target<T> implements
          Callable<Long> {

    /**
     * Synchronization lock.
     */
    private final Object syncLock;
    /**
     * Flag to indicate, if processing is done.
     */
    private boolean done = false;
    /**
     * Counter of processed items.
     */
    private AtomicLong itemCount = new AtomicLong(0);

    /**
     * Creates a new debugging {@link Target}. Only one instance may be run.
     *
     * @param newSource Source to use
     */
    public TargetTest(Source<T> newSource) {
      super(newSource);
      this.syncLock = new Object();
    }

    @Override
    public Target<T> newInstance() {
      return new TargetTest<>(getSource());
    }

    @Override
    public void runProcess() throws Exception {
      while (!isTerminating()) {
        try {
          final T o = getSource().next();
          if (o != null) {
            itemCount.incrementAndGet();
          }
        } catch (ProcessingException.SourceHasFinishedException ex) {
          break;
        }
      }
      synchronized (this.syncLock) {
        this.done = true;
        this.syncLock.notifyAll();
      }
    }

    @Override
    public Long call() throws Exception {
      super.run();
      synchronized (this.syncLock) {
        while (!this.done) {
          this.syncLock.wait();
        }
      }
      return itemCount.get();
    }

  }
}
