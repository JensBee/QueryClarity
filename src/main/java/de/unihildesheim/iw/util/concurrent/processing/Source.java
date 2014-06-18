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

import de.unihildesheim.iw.util.concurrent.processing.SourceException
    .SourceNotReadyException;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Base class for all Source implementations. <br> A Source is meant to be used
 * once, so starting it more than one time may lead to unexpected behavior. <br>
 * If the Source has finished processing items it should decrement the latch (if
 * set), to indicate it has finished. This is handled by {@link #stop()} which
 * must be called by the implementing class when finished.
 *
 * @param <T> Type this Source provides
 */
public abstract class Source<T>
    implements Callable<Long> {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      Source.class);
  /**
   * Flag indicating if this Source is running.
   */
  private volatile boolean isRunning;
  /**
   * Flag indicating if this Source has finished.
   */
  private volatile boolean isFinished;

  /**
   * Default constructor attaching a {@link SourceObserver} if applicable.
   */
  protected Source() {
    this.isRunning = false;
    this.isFinished = false;
  }

  @Override
  public synchronized Long call() {
    if (isRunning()) {
      throw new SourceException.SourceIsRunningException();
    }
    if (isFinished()) {
      throw new SourceException.SourceHasFinishedException();
    }
    setRunning();
    try {
      awaitTermination();
    } catch (final InterruptedException ex) {
      LOG.error("Interrupted.", ex);
    }
    stop();
    return getSourcedItemCount();
  }

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
   * Set the flag indicating this Source is running.
   */
  protected final synchronized void setRunning() {
    this.isRunning = true;
    this.notifyAll();
  }

  /**
   * Await termination.
   *
   * @throws java.lang.InterruptedException Thrown, if thread gets interrupted
   */
  protected final synchronized void awaitTermination()
  throws InterruptedException {
    while (this.isRunning) {
      this.wait();
    }
  }

  /**
   * Signal the Source, that it should stop generating items.
   */
  protected final synchronized void stop() {
    this.isRunning = false;
    this.isFinished = true;
    this.notifyAll();
  }

  /**
   * Get the number of items already served.
   *
   * @return Number of items served
   */
  public abstract long getSourcedItemCount();

  /**
   * Get the next item to process.
   *
   * @return Next item to process
   * @throws ProcessingException Thrown, if source has not been started or
   * already finished
   * @throws java.lang.InterruptedException Thrown, if thread gets interrupted
   */
  public abstract T next();

  /**
   * Get the number of items to process. May return {@code null} if no such
   * information is provided.
   *
   * @return Number of items to process
   * @throws ProcessingException Thrown, if source has not been started or
   * already finished
   */
  public abstract Long getItemCount()
      throws ProcessingException;

  /**
   * Check, if instance is running. Optionally throwing an exception, if it's
   * not the case.
   *
   * @return True, if running
   * @throws SourceNotReadyException Thrown, if the Source has not been started
   * and {@code fail} is {@code true}
   */
  @SuppressWarnings("SameReturnValue")
  protected final synchronized boolean checkRunStatus()
      throws SourceNotReadyException {
    if (this.isRunning) {
      return true;
    }
    if (this.isFinished) {
      throw new SourceException.SourceHasFinishedException();
    }
    throw new SourceNotReadyException();
  }

  /**
   * Blocking wait until the source starts providing items.
   *
   * @throws InterruptedException Thrown, if thread gets interrupted
   */
  public final void awaitStart()
      throws InterruptedException {
    if (!this.isFinished) {
      synchronized (this) {
        while (!this.isRunning && !this.isFinished) {
          this.wait();
        }
      }
    }
  }
}
