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

import org.jetbrains.annotations.NotNull;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class TaskObserver
    implements Runnable, AutoCloseable {
  private final TaskObserverMessage tom;
  private long timeout = 15000L; // 15 sec
  private Thread t = new Thread(this);
  private TimeMeasure tm = new TimeMeasure();
  private boolean stop = false;

  public TaskObserver(@NotNull TaskObserverMessage tom) {
    this.tom = tom;
  }

  public TaskObserver setSchedule(final long sched) {
    this.timeout = sched;
    return this;
  }

  public TaskObserver start() {
    this.stop = false;
    t.start();
    tm.start();
    return this;
  }

  public TaskObserver stop() {
    this.stop = true;
    t.interrupt();
    tm.stop();
    return this;
  }

  @Override
  public void run() {
    try {
      while(!this.stop) {
        Thread.sleep(this.timeout);
        this.tom.call(this.tm);
      }
    } catch (final InterruptedException e) {
      // pass
    }
  }

  @Override
  public void close() {
    stop();
  }

  public static abstract class TaskObserverMessage {
    public abstract void call(@NotNull final TimeMeasure tm);
  }
}
