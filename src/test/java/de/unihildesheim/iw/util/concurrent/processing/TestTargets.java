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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Testing {@link Target} implementations simply counting the items being
 * provided by a {@link Source}.
 *
 * @author Jens Bertram
 */
public class TestTargets {

  /**
   * Plain {@link Target} implementation.
   *
   * @param <T> Type of items being processed
   */
  public final static class Plain<T>
      extends Target<T> {

    private final AtomicLong c;

    /**
     * Create a new {@link Target} with a specific {@link Source}.
     *
     * @param newSource <tt>Source</tt> to use
     */
    public Plain(final AtomicLong counter, final Source<T> newSource) {
      super(newSource);
      this.c = counter;
    }

    @Override
    public Target<T> newInstance() {
      return new Plain<>(this.c, getSource());
    }

    @Override
    public void runProcess()
        throws Exception {
      while (!isTerminating()) {
        final T data;
        try {
          data = getSource().next();
        } catch (ProcessingException.SourceHasFinishedException ex) {
          break;
        }

        this.c.incrementAndGet();
      }
    }
  }

  /**
   * Function call {@link Target}.
   *
   * @param <T> Type of items being processed
   */
  public final static class FuncCall<T>
      extends TargetFuncCall.TargetFunc<T> {

    private final AtomicLong c;

    public FuncCall(final AtomicLong counter) {
      this.c = counter;
    }

    @Override
    public void call(final T data) {
      if (data != null) {
        c.incrementAndGet();
      }
    }
  }
}
