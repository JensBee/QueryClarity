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

/**
 * Simple class to measure the elapsed time.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TimeMeasure {

  /**
   * Start time in nanoseconds of the current measurement.
   */
  private long startTime;

  /**
   * Overall time elapsed.
   */
  private long elapsed = 0L;

  /**
   * True, if measurement is paused.
   */
  private boolean paused = false;

  /**
   * True, if measurement is stopped.
   */
  private boolean stopped = false;

  /**
   * Start the time measurement. If the measurement was paused, it will continue
   * measuring. If it was stopped before it will be reset.
   *
   * @return Self reference
   */
  public TimeMeasure start() {
    if (this.paused) {
      this.paused = false;
    } else {
      this.elapsed = 0L;
    }
    this.startTime = System.nanoTime();
    return this;
  }

  /**
   * Pause the time measurement.
   * @return Self reference
   */
  public TimeMeasure pause() {
    this.elapsed = getElapsedNanos();
    this.paused = true;
    return this;
  }

  /**
   * Start the time measurement.
   * @return Self reference
   */
  public TimeMeasure stop() {
    this.paused = false;
    this.stopped = true;
    this.elapsed += getElapsedNanos();
    return this;
  }

  /**
   * Get the elapsed nanoseconds between the stored start-time and the current
   * time.
   *
   * @return Elapsed nanoseconds
   */
  private long getElapsedNanos() {
    return System.nanoTime() - startTime;
  }

  /**
   * Get the elapsed seconds of the current measurement.
   *
   * @return elapsed seconds, or <tt>0</tt> if no time was recorded
   */
  public double getElapsedSeconds() {
    double nanos;
    if (!this.stopped) {
      nanos = this.elapsed + getElapsedNanos();
    } else {
      if (this.elapsed > 0) {
        nanos = this.elapsed;
      } else {
        nanos = 0d;
      }
    }
    return nanos > 0 ? nanos / 1000000000.0 : 0d;
  }

}
