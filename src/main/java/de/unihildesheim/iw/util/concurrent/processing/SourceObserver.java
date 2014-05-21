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

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Observer for {@link Source}es, printing status messages to the system log.
 */
public final class SourceObserver<T>
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
   * Number of threads running.
   */
  private final int numberOfThreads;

  private enum LoopType {
    ITEM_COUNTER,
    PLAIN,
    SINGLE
  }

  /**
   * Attach a status observer to a specified {@link Source}.
   *
   * @param newSource Source whose process to observe
   */
  public SourceObserver(final int threadCount, final Source newSource) {
    this.terminate = false;
    this.source = Objects.requireNonNull(newSource, "Source was null.");
    this.overallTime = new TimeMeasure();
    this.runTime = new TimeMeasure();
    this.numberOfThreads = threadCount;
  }

  /**
   * Stop observing and terminate.
   */
  public void terminate() {
    synchronized (this) {
      this.terminate = true;
      this.notifyAll(); // awake from sleep, if necessary
    }
  }

  /**
   * Show a timed status message.
   */
  private void showStatus(final boolean isSingle) {
    if (isSingle) {
      LOG.info("Processing {} items since {}.",
          this.source.getSourcedItemCount(),
          this.overallTime.getTimeString());
    } else {
      LOG.info("Processed {} items after {}s, running since {}.",
          this.source.getSourcedItemCount(),
          this.runTime.getElapsedSeconds(),
          this.overallTime.getTimeString());
    }
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

    if (numberOfThreads > 1) {
      // multi process info
      LOG.info("Processing {} of {} items ({}%). "
              + "{}s since last status. Running for {}. Time left {}.",
          lastStatus, itemCount, (lastStatus * 100) / itemCount,
          runTime.stop().getElapsedSeconds(),
          overallTime.getTimeString(),
          TimeMeasure.getTimeString(estimate)
      );
    } else {
      // single process info
      LOG.info("Processing {} item. {}s since last status. Running for {}. "
              + "Time left {}.",
          itemCount, runTime.stop().getElapsedSeconds(),
          overallTime.getTimeString(),
          TimeMeasure.getTimeString(estimate)
      );
    }
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public Double call() {
    // short circuit if source has already finished
    if (this.source.isFinished()) {
      return 0d;
    }

    LoopType type;

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
        itemCount = this.source.getItemCount();
        if (itemCount != numberOfThreads) {
          hasItemCount = true;
          type = LoopType.ITEM_COUNTER;
          step = (int) (itemCount * STEP_SIZE);
        } else {
          type = LoopType.SINGLE;
          hasItemCount = false;
          step = 0;
        }
      } else {
        type = LoopType.PLAIN;
        hasItemCount = false;
        step = 0;
        itemCount = 0L;
      }

      while (!this.terminate) {
        switch (type) {
          case PLAIN:
            if (this.runTime.getElapsedSeconds() > INTERVAL) {
              this.runTime.stop();
              showStatus(false);
              this.runTime.start();
            }
            break;
          case ITEM_COUNTER:
            status = this.source.getSourcedItemCount();
            // check if max wait time elapsed
            if (this.runTime.getElapsedSeconds() > INTERVAL) {
              this.runTime.stop();
              lastStatus = status;
              showStatus(itemCount, lastStatus);
              this.runTime.start();
            } else
              // max wait time not elapsed, check if we should provide
              // a status based on progress
              if (lastStatus < status && step > 0 && status % step == 0) {
                this.runTime.stop();
                lastStatus = status;
                showStatus(itemCount, lastStatus);
                this.runTime.start();
              }
            break;
          case SINGLE:
            if (this.runTime.getElapsedSeconds() > INTERVAL) {
              this.runTime.stop();
              showStatus(true);
              this.runTime.start();
            }
            break;
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
