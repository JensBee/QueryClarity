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
package de.unihildesheim.iw.util;

import java.util.concurrent.TimeUnit;

/**
 * Simple class to measure the elapsed time.
 */
public final class TimeMeasure {

  /**
   * Start time in nanoseconds of the current measurement.
   */
  private long startTime;

  /**
   * Overall time elapsed.
   */
  private long elapsed;

  /**
   * True, if measurement is paused.
   */
  private boolean paused;

  /**
   * True, if measurement is stopped.
   */
  private boolean stopped;

  /**
   * Get a string representation of the elapsed time formatted as <tt>DDd
   * HH:MM:SS</tt> string.
   *
   * @param nanos Nanoseconds to convert
   * @return Formatted elapsed time string
   */
  public static String getTimeString(final double nanos) {
    return getTimeString((long) (nanos / 1000000000.0));
  }

  /**
   * Start the time measurement. If the measurement was paused, it will continue
   * measuring. If it was stopped before it will be reset.
   *
   * @return Self reference
   */
  public TimeMeasure start() {
    if (!this.paused) {
      this.elapsed = 0L;
    }
    this.paused = false;
    this.stopped = false;
    this.startTime = System.nanoTime();
    return this;
  }

  /**
   * Pause the time measurement.
   *
   * @return Self reference
   */
  public TimeMeasure pause() {
    if (!this.paused && !this.stopped) {
      this.elapsed = getNanos();
    }
    this.paused = true;
    return this;
  }

  /**
   * Get the elapsed nanoseconds between the stored start-time and the current
   * time.
   *
   * @return Elapsed nanoseconds
   */
  private long getNanos() {
    return System.nanoTime() - startTime;
  }

  /**
   * Start the time measurement.
   *
   * @return Self reference
   */
  public TimeMeasure stop() {
    if (!this.stopped && !this.paused) {
      this.elapsed += getNanos();
    }
    this.paused = false;
    this.stopped = true;
    return this;
  }

  /**
   * Get the elapsed milliseconds of the current measurement.
   *
   * @return elapsed milliseconds, or <tt>0</tt> if no time was recorded
   */
  public double getElapsedMillis() {
    final double nanos = getElapsedNanos();
    return nanos > 0 ? nanos / 1000000.0 : 0d;
  }

  /**
   * Get the elapsed nanoseconds of the current measurement.
   *
   * @return elapsed nanoseconds, or <tt>0</tt> if no time was recorded
   */
  public double getElapsedNanos() {
    double nanos;
    if (!this.stopped) {
      nanos = this.elapsed + getNanos();
    } else {
      if (this.elapsed > 0) {
        nanos = this.elapsed;
      } else {
        nanos = 0d;
      }
    }
    return nanos;
  }

  /**
   * Get a string representation of the elapsed time formatted as <tt>DDd
   * HH:MM:SS</tt> string.
   *
   * @return Formatted elapsed time string
   */
  public String getTimeString() {
    return getTimeString((long) getElapsedSeconds());
  }

  /**
   * Get a string representation of the elapsed time formatted as <tt>DDd
   * HH:MM:SS</tt> string.
   *
   * @param elapsedTime Elapsed seconds to convert
   * @return Formatted elapsed time string
   */
  public static String getTimeString(final long elapsedTime) {
    final StringBuilder timeStr = new StringBuilder(20);

    final int day = (int) TimeUnit.SECONDS.toDays(elapsedTime);
    final long hours = TimeUnit.SECONDS.toHours(elapsedTime) - (day * 24L);
    final long minutes = TimeUnit.SECONDS.toMinutes(elapsedTime)
        - (TimeUnit.SECONDS.toHours(elapsedTime) * 60L);
    final long seconds = TimeUnit.SECONDS.toSeconds(elapsedTime)
        - (TimeUnit.SECONDS.toMinutes(elapsedTime) * 60L);

    if (day > 0) {
      timeStr.append(day).append("d ").append(hours).append("h ").append(
          minutes).append("m ").append(seconds).append('s');
    } else if (hours > 0) {
      timeStr.append(hours).append("h ").append(minutes).append("m ").append(
          seconds).append('s');
    } else if (minutes > 0) {
      timeStr.append(minutes).append("m ").append(seconds).append('s');
    } else {
      timeStr.append(seconds).append('s');
    }
    return timeStr.toString();
  }

  /**
   * Get the elapsed seconds of the current measurement.
   *
   * @return elapsed seconds, or <tt>0</tt> if no time was recorded
   */
  public double getElapsedSeconds() {
    final double nanos = getElapsedNanos();
    return nanos > 0 ? nanos / 1000000000.0 : 0d;
  }
}
