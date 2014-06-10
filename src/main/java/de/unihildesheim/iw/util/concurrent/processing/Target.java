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

import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * {@link Target} for a processing {@link Source}. Implements {@link Runnable}
 * to be executed by using {@link Thread}s.
 *
 * @param <T> Type that this {@link Target} accepts
 */
public abstract class Target<T>
    implements Callable<Boolean> {

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
   * Create a new Target with a specific {@link Source}.
   *
   * @param newSource <tt>Source</tt> to use
   */
  public Target(final Source<T> newSource) {
    this.source = Objects.requireNonNull(newSource, "Source was null.");
    this.terminate = false;
  }

  /**
   * Set the termination flag for this {@link Runnable}. If the instance
   * terminates, the provided {@link java.util.concurrent.CountDownLatch} must
   * be decremented.
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
   * Get a unique name for this {@link Runnable}.
   *
   * @return Name for this instance
   */
  public final String getName() {
    return this.getClass().getSimpleName() + "-" + this.hashCode();
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
   * Create a new Target instance.
   *
   * @return New Target instance
   * @throws Exception Any exception thrown by implementing class
   */
  public abstract Target<T> newInstance()
      throws Exception;

  /**
   * Set the thread tracking latch.
   *
   * @param newLatch Shared latch to track running threads.
   */
  public final void setLatch(final CountDownLatch newLatch) {
    this.latch = Objects.requireNonNull(newLatch, "Latch was null.");
  }

  @Override
  public final Boolean call()
      throws Exception {
    Boolean success = Boolean.FALSE;
    try {
      LOG.trace("({}) Starting.", getName());
      this.source.awaitStart();
      runProcess();
      success = Boolean.TRUE;
      return true; // simple flag indication success
    } catch (final Throwable t) {
      LOG.debug("({}) Terminating with error.", getName(), t);
      throw t;
    } finally {
      this.terminate = true;
      this.latch.countDown();
    }
  }

  /**
   * Equivalent for <tt>run()</tt> function called by abstract Target class.
   *
   * @throws Exception Any exception thrown by implementing class
   */
  public abstract void runProcess()
      throws Exception;

  /**
   * Get the {@link Source} for this Target instance.
   *
   * @return {@link Source} used by this Target.
   */
  public final Source<T> getSource() {
    return this.source;
  }
}
