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
   * Identifier used for storing {@link GlobalConfiguration configuration}
   * options.
   */
  private static final String IDENTIFIER = "SourceObserver";
  /**
   * {@link GlobalConfiguration Configuration} options prefix.
   */
  private static final String CONF_PREFIX = GlobalConfiguration.mkPrefix
      (IDENTIFIER);

  /**
   * Update interval in seconds. Used,if {@link Source} does not provide the
   * expected number of items. Defaults to 15 seconds.
   */
  private static final int INTERVAL = GlobalConfiguration.conf()
      .getAndAddInteger(CONF_PREFIX + "update-interval-ms", 15000);
  /**
   * Percentage steps to show item based progress. Defaults to 0.1 (1%).
   */
  private static final double STEP_SIZE = GlobalConfiguration.conf()
      .getAndAddDouble(CONF_PREFIX + "update-interval-percentage", 0.1);
  /**
   * Wait time in milliseconds between updates. Defaults to 1/2 second.
   */
  private static final int UPDATE_INTERVAL = GlobalConfiguration.conf()
      .getAndAddInteger(CONF_PREFIX + "check-interval-ms", 500);
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
   * Number of threads running.
   */
  private final int numberOfThreads;
  /**
   * Flag indicating, if this instance should terminate.
   */
  private volatile boolean terminate;
  /**
   * Number of items in source. Set if provided manually.
   */
  private long numberOfItems = -1L;

  /**
   * Attach a status observer to a specified {@link Source}.
   *
   * @param threadCount Number of threads querying the {@link Source}
   * @param newSource {@link Source} whose progress to observe
   */
  public SourceObserver(final int threadCount, final Source newSource,
      final Long itemCount) {
    this.terminate = false;
    if (itemCount != null && itemCount > 0L) {
      this.numberOfItems = itemCount;
    }
    this.source = Objects.requireNonNull(newSource, "Source was null.");
    this.overallTime = new TimeMeasure();
    this.runTime = new TimeMeasure();
    this.numberOfThreads = threadCount;
  }

  /**
   * Stop observing and terminate.
   */
  public synchronized void terminate() {
    this.terminate = true;
    this.notifyAll(); // awake from sleep, if necessary
  }

  @Override
  public Double call() {
    // short circuit if source has already finished
    if (this.source.isFinished()) {
      return 0d;
    }

    final LoopType type;

    this.overallTime.start();
    try {
      this.source.awaitStart();
      this.runTime.start();
      long lastStatus = 0L;
      long status = 0L;
      final int step;

      if (this.numberOfItems > -1L || this.source.getItemCount() != null) {
        if (this.numberOfItems == -1L) {
          this.numberOfItems = this.source.getItemCount();
        }
        if (this.numberOfThreads == this.numberOfItems) {
          type = LoopType.SINGLE;
          step = 0;
        } else {
          type = LoopType.ITEM_COUNTER;
          step = (int) ((double) this.numberOfItems * STEP_SIZE);
        }
      } else {
        type = LoopType.PLAIN;
        step = 0;
      }

      while (!this.terminate) {
        status = this.source.getSourcedItemCount();
        switch (type) {
          case PLAIN:
            if (this.runTime.getElapsedMillis() >= (double) INTERVAL) {
              this.runTime.stop();
              showStatus(lastStatus, status);
              lastStatus = status;
              this.runTime.start();
            }
            break;
          case ITEM_COUNTER:
            // check if max wait time elapsed
            if (this.runTime.getElapsedMillis() >= (double) INTERVAL) {
              this.runTime.stop();
              showStatus(this.numberOfItems, lastStatus, status);
              lastStatus = status;
              this.runTime.start();
            } else
              // max wait time not elapsed, check if we should provide
              // a status based on progress
              if (lastStatus < status && step > 0 &&
                  status % (long) step == 0) {
                this.runTime.stop();
                showStatus(this.numberOfItems, lastStatus, status);
                lastStatus = status;
                this.runTime.start();
              }
            break;
          case SINGLE:
            if (this.runTime.getElapsedMillis() >= (double) INTERVAL) {
              this.runTime.stop();
              showStatus(lastStatus, status);
              lastStatus = status;
              this.runTime.start();
            }
            break;
        }
        // wait a bit, till next update
        synchronized (this) {
          if (!this.terminate) {
            if (LoopType.ITEM_COUNTER == type) {
              this.wait((long) UPDATE_INTERVAL);
            } else {
              this.wait((long) INTERVAL);
            }
          }
        }
      }
    } catch (final ProcessingException ex) {
      LOG.error("Caught exception while running observer.", ex);
    } catch (final InterruptedException ex) {
      LOG.error("Interrupted.", ex);
    }
    return this.overallTime.stop().getElapsedNanos();
  }

  /**
   * Show a timed status message.
   *
   * @param lastStatus Items processed on last status
   * @param status Items processed on current status
   */
  private void showStatus(final long lastStatus,
      final long status) {
    final long count = this.source.getSourcedItemCount();
    final String text;
    if (count <= 1L) {
      LOG.info("Processed {} item after {}s, running since {}.",
          this.source.getSourcedItemCount(),
          this.runTime.getElapsedSeconds(),
          this.overallTime.getTimeString());
    } else {
      final long progress = status - lastStatus;
      LOG.info("Processed {} items (+{}) after {}s, running since {}.",
          status,
          progress,
          this.runTime.getElapsedSeconds(),
          this.overallTime.getTimeString());
    }
  }

  /**
   * Show a progress status message.
   *
   * @param itemCount Currently processed items
   * @param lastStatus Items processed on last status
   * @param status Items processed on current status
   */
  private void showStatus(final long itemCount, final long lastStatus,
      final long status) {
    final long estimate = (long) (
        (double) (itemCount - status) / ((double) status
            / this.overallTime.getElapsedSeconds()));

    if (this.numberOfThreads > 1) {
      // multi process info
      final long progress = status - lastStatus;
      LOG.info("Processing {} of {} items ({} {}%). "
              + "{}s since last status. Running for {}. Time left: {}.",
          status, itemCount,
          "+" + progress,
          (status * 100L) / itemCount,
          this.runTime.getElapsedSeconds(),
          this.overallTime.getTimeString(),
          TimeMeasure.getTimeString(estimate)
      );
    } else {
      // single process info
      LOG.info("Processing {} item. {}s since last status. Running for {}. "
              + "Time left: {}.",
          itemCount, this.runTime.getElapsedSeconds(),
          this.overallTime.getTimeString(),
          TimeMeasure.getTimeString(estimate)
      );
    }
  }

  /**
   * Different types of how to poll the {@link Source} for updates.
   */
  private enum LoopType {
    /**
     * {@link Source} with counter information.
     */
    ITEM_COUNTER,
    /**
     * {@link Source} without counter information.
     */
    PLAIN,
    /**
     * Single threaded {@link Source} usage.
     */
    SINGLE
  }
}
