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

import java.util.concurrent.Callable;

/**
 * Observer for {@link Source}es, printing status messages to the system log.
 */
public final class SourceObserver
    implements Callable<Double> {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      SourceObserver.class);
  /**
   * Update interval in seconds. Used,if {@link Source} does not provide the
   * expected number of items.
   */
  private static final int INTERVAL = 15;
  /**
   * Percentage steps to show item based progress.
   */
  private static final double STEP_SIZE = 0.10;
  /**
   * Wait time in milliseconds between updates.
   */
  private static final int UPDATE_INTERVAL = 100;
  /**
   * {@link Source} to observe.
   */
  private final Source source;
  /**
   * Overall time taken.
   */
  private final TimeMeasure overallTime;
  /**
   * Time taken between two status messages.
   */
  private final TimeMeasure runTime;
  /**
   * Flag indicating, if this instance should terminate.
   */
  private volatile boolean terminate;

  /**
   * Attach a status observer to a specified {@link Source}.
   *
   * @param newSource Source whose process to observe
   */
  public SourceObserver(final Source newSource) {
    this.terminate = false;
    this.source = newSource;
    this.overallTime = new TimeMeasure();
    this.runTime = new TimeMeasure();
  }

  /**
   * Stop observing and terminate.
   */
  public void terminate() {
    synchronized (this) {
      this.terminate = true;
      this.notifyAll(); // awake from sleep, if neccessary
    }
  }

  /**
   * Show a timed status message.
   */
  private void showStatus() {
    LOG.info("Processing {} of {} items after {}s, running for {}.",
        this.source.hashCode(), this.source.getSourcedItemCount(),
        this.runTime.getElapsedSeconds(),
        this.overallTime.getTimeString());
  }

  /**
   * Show a progress status message.
   *
   * @param itemCount Currently processed items
   * @param lastStatus Items processed on last status
   */
  private void showStatus(final long itemCount, final long lastStatus) {
    final long estimate = (long) ((itemCount - lastStatus) / (lastStatus
                                                              / overallTime
        .getElapsedSeconds()));
    LOG.info("Processing {} of {} items ({}%). "
             + "{}s since last status. Running for {}. "
             + "Time left {}.", lastStatus, itemCount,
        (lastStatus * 100) / itemCount,
        runTime.stop().getElapsedSeconds(),
        overallTime.getTimeString(),
        TimeMeasure.getTimeString(estimate)
    );
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public Double call() {
    // short circuit if source has already finished
    if (this.source.isFinished()) {
      return 0d;
    }
    this.overallTime.start();
    try {
      this.source.awaitStart();
      this.runTime.start();
      long lastStatus = 0;
      long status = 0;
      int step;
      Long itemCount;

      // flag indicating if Source provides the number of items
      // it will provide
      boolean hasItemCount;
      if (this.source.getItemCount() != null) {
        hasItemCount = true;
        itemCount = this.source.getItemCount();
        step = (int) (itemCount * STEP_SIZE);
      } else {
        hasItemCount = false;
        step = 0;
        itemCount = 0L;
      }

      while (!this.terminate) {
        if (hasItemCount) {
          status = this.source.getSourcedItemCount();
        }

        // check if max wait time elapsed
        if (this.runTime.getElapsedSeconds() > INTERVAL) {
          this.runTime.stop();
          if (hasItemCount) {
            lastStatus = status;
            showStatus(itemCount, lastStatus);
          } else {
            showStatus();
          }
          this.runTime.start();
        } else if (hasItemCount && step > 0 && lastStatus < status
                   && status % step == 0) {
          // max wait time not elapsed, check if we should provide
          // a status
          // based on progres
          this.runTime.stop();
          lastStatus = status;
          showStatus(itemCount, lastStatus);
          this.runTime.start();
        }

        // wait a bit, till next update
        synchronized (this) {
          if (!this.terminate) {
            this.wait(UPDATE_INTERVAL);
          }
        }
      }
    } catch (ProcessingException ex) {
      LOG.error("Caught exception while running observer.", ex);
    } catch (InterruptedException ex) {
      LOG.error("Interrupted.", ex);
    }
    return this.overallTime.stop().getElapsedNanos();
  }
}
