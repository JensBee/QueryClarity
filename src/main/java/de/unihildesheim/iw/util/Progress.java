/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

package de.unihildesheim.iw.util;

import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import org.slf4j.LoggerFactory;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class Progress {
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      Progress.class);

  /**
   * Identifier used for storing {@link GlobalConfiguration configuration}
   * options.
   */
  private static final String IDENTIFIER = "Progress";
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
   * Number of items done processing.
   */
  private long itemCounter;
  /**
   * Number of total items to process. May be zero, if value is not available.
   */
  private final long itemCount;

  private long itemCounterLast = 0L;

  private int stepSize = 0;

  /**
   * Overall time taken.
   */
  private final TimeMeasure overallTime;
  /**
   * Time taken between two status messages.
   */
  private final TimeMeasure runTime;

  /**
   * Indicate if processing has started.
   */
  private boolean started = false;

  public Progress() {
    this.itemCount = 0L; // no total item count available
    this.itemCounter = 0L;
    this.overallTime = new TimeMeasure();
    this.runTime = new TimeMeasure();
  }

  public Progress(final long items) {
    this.itemCount = items;
    this.itemCounter = 0L;
    this.overallTime = new TimeMeasure();
    this.runTime = new TimeMeasure();
    this.stepSize = (int) ((double) this.itemCount * STEP_SIZE);
  }

  private void printStatus(final boolean finished) {
    boolean print = false;
    long progress = this.itemCounter - this.itemCounterLast;
    if (finished ||
        this.runTime.getElapsedMillis() >= (double) INTERVAL ||
        (this.stepSize > 0 && progress > 0L &&
            progress % (long) this.stepSize == 0)
        ) {
      this.runTime.stop();
      LOG.info("Processed {} items (+{}) after ~{}s, running since {}.",
          this.itemCounter,
          progress,
          this.runTime.getElapsedFullSeconds(),
          this.overallTime.getTimeString());
      this.runTime.start();
      this.itemCounterLast = this.itemCounter;
    }
  }

  /**
   * Signal the start of the processing.
   */
  public Progress start() {
    if (this.started) {
      LOG.warn("Progress already started.");
    } else {
      this.overallTime.start();
      this.runTime.start();
      this.started = true;
    }
    return this;
  }

  /**
   * Signal an item was processed.
   */
  public long inc() {
    if (!this.started) {
      start();
    }
    this.itemCounter++;
    printStatus(false);
    return this.itemCounter;
  }

  public void reset() {
    this.itemCounter = 0L;
    this.itemCounterLast = 0L;
    this.overallTime.stop();
    this.runTime.stop();
    this.started = false;
  }

  /**
   * Signal the finishing of the processing.
   */
  public long finished() {
    printStatus(true);
    return this.itemCounter;
  }
}
