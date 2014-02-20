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

import de.unihildesheim.util.concurrent.processing.ProcessingException.SourceNotReadyException;
import org.slf4j.LoggerFactory;

/**
 * Base class for all {@link Source} implementations.
 * <p>
 * A {@link Source} is meant to be used once, so starting it more than one
 * time may lead to unexpected behavior.
 * <p>
 * If the {@link Source} has finished processing items it should decrement the
 * latch (if set), to indicate it has finished. This is handled by
 * {@link #stop()} which must be called by the implementing class when
 * finished.
 *
 * @param <T> Type this {@link Source} provides
 */
public abstract class Source<T> implements Runnable {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          Source.class);
  /**
   * Flag indicating if this {@link Source} is running.
   */
  private volatile boolean isRunning;
  /**
   * Flag indicating if this {@link Source} has finished.
   */
  private volatile boolean isFinished;

  /**
   * Default constructor attaching a {@link Processing.SourceObserver} if
   * applicable.
   */
  protected Source() {
    this.isRunning = false;
    this.isFinished = false;
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
    try {
      awaitTermination();
    } catch (InterruptedException ex) {
      LOG.error("Interrupted.", ex);
    }
  }

  /**
   * Signal the {@link Source}, that it should stop generating items.
   */
  protected final synchronized void stop() {
    this.isRunning = false;
    this.isFinished = true;
    this.notifyAll();
  }

  /**
   * Get the next item to process.
   *
   * @return Next item to process
   * @throws ProcessingException Thrown, if source has not been started or
   * already finished
   * @throws java.lang.InterruptedException Thrown, if thread gets interrupted
   */
  public abstract T next() throws ProcessingException, InterruptedException;

  /**
   * Get the number of items to process.
   *
   * @return Number of items to process
   * @throws ProcessingException Thrown, if source has not been started or
   * already finished
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
   * Check, if instance is running. Optionally throwing an exception, if it's
   * not the case.
   *
   * @return True, if running
   * @throws ProcessingException.SourceNotReadyException Thrown, if the
   * {@link Processing.Source} has not been started and
   * <tt>fail</tt> is <tt>true</tt>
   */
  protected final synchronized boolean checkRunStatus() throws
          SourceNotReadyException {
    if (this.isRunning) {
      return true;
    }
    if (this.isFinished) {
      throw new ProcessingException.SourceHasFinishedException();
    }
    if (!this.isRunning) {
      throw new ProcessingException.SourceNotReadyException();
    }
    return true;
  }

  /**
   * Set the flag indicating this {@link Processing.Source} is running.
   */
  protected final void setRunning() {
    synchronized (this) {
      this.isRunning = true;
      this.notifyAll();
    }
  }

  /**
   * Await termination.
   *
   * @throws java.lang.InterruptedException Thrown, if thread gets interrupted
   */
  protected final synchronized void awaitTermination() throws
          InterruptedException {
    while (this.isRunning) {
      this.wait();
    }
  }

  /**
   * Blocking wait until the source starts providing items.
   *
   * @throws InterruptedException Thrown, if thread gets interrupted
   */
  public final void awaitStart() throws InterruptedException {
    if (!this.isFinished) {
      synchronized (this) {
        while (!this.isRunning) {
          this.wait();
        }
      }
    }
  }

}
