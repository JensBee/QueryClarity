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

import java.util.ArrayList;
import java.util.List;
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
   * Get a string representation of the elapsed time formatted as <tt>DDd
   * HH:MM:SS</tt> string.
   *
   * @param elapsedTime Elapsed seconds to convert
   * @return Formatted elapsed time string
   */
  public static String getTimeString(final long elapsedTime) {
    final StringBuilder timeStr = new StringBuilder(20);

    final int day = (int) TimeUnit.SECONDS.toDays(elapsedTime);
    final long hours =
        TimeUnit.SECONDS.toHours(elapsedTime) - ((long) day * 24L);
    final long minutes = TimeUnit.SECONDS.toMinutes(elapsedTime)
        - (TimeUnit.SECONDS.toHours(elapsedTime) * 60L);
    final long seconds = TimeUnit.SECONDS.toSeconds(elapsedTime)
        - (TimeUnit.SECONDS.toMinutes(elapsedTime) * 60L);

    final List<String> strParts = new ArrayList<>(4);

    if ((long) day > 0L) {
      strParts.add(day + "d");
    }
    if (hours > 0L) {
      strParts.add(hours + "h");
    }
    if (minutes > 0L) {
      strParts.add(minutes + "m");
    }
    if (seconds > 0L) {
      strParts.add(seconds + "s");
    }

    final int strPartsCount = strParts.size();

    if (strPartsCount == 0) {
      timeStr.append(0).append('s');
    } else {
      for (int i = 0; i < strPartsCount; i++) {
        timeStr.append(strParts.get(i));
        if (i + 1 < strPartsCount) {
          timeStr.append(' ');
        }
      }
    }

    return timeStr.toString();
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
    return System.nanoTime() - this.startTime;
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
    return nanos > 0d ? nanos / 1000000.0 : 0d;
  }

  /**
   * Get the elapsed nanoseconds of the current measurement.
   *
   * @return elapsed nanoseconds, or <tt>0</tt> if no time was recorded
   */
  public double getElapsedNanos() {
    final double nanos;
    if (this.stopped) {
      if (this.elapsed > 0L) {
        nanos = (double) this.elapsed;
      } else {
        nanos = 0d;
      }
    } else {
      nanos = (double) (this.elapsed + getNanos());
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
   * Get the elapsed seconds of the current measurement.
   *
   * @return elapsed seconds, or {@code 0} if no time was recorded
   */
  public double getElapsedSeconds() {
    final double nanos = getElapsedNanos();
    return nanos > 0d ? nanos / 1000000000.0 : 0d;
  }

  /**
   * Get the full (rounded) elapsed seconds of the current measurement.
   *
   * @return elapsed seconds, or {@code 0} if no time was recorded
   */
  public long getElapsedFullSeconds() {
    return Math.round(getElapsedSeconds());
  }
}
