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

import java.util.Objects;

/**
 * Simple processing target calling a single function on each object.
 *
 * @param <T> Type of data to process
 * @author Jens Bertram
 */
@SuppressWarnings("PublicInnerClass")
public final class TargetFuncCall<T>
    extends Target<T> {
  /**
   * Function to call for each element.
   */
  private final TargetFunc<T> tFunc;

  /**
   * Create a new {@link Processing} target invoking a function for each
   * element.
   *
   * @param newSource Source providing items
   * @param func Function to call
   */
  public TargetFuncCall(final Source<T> newSource, final TargetFunc<T> func) {
    super(newSource);
    this.tFunc = Objects.requireNonNull(func, "Function was null.");
  }

  @Override
  public Target<T> newInstance()
      throws TargetException {
    if (this.tFunc instanceof TargetFuncFactory) {
      return new TargetFuncCall<>(getSource(),
          ((TargetFuncFactory<T>) this.tFunc).newInstance());
    }
    return new TargetFuncCall<>(getSource(), this.tFunc);
  }

  @Override
  public void runProcess()
      throws Exception {
    while (!isTerminating()) {
      final T data;
      try {
        data = getSource().next();
      } catch (final SourceException.SourceHasFinishedException ex) {
        break;
      }
      this.tFunc.call(data);
    }
  }

  /**
   * Function implementation for {@link TargetFuncCall}.
   *
   * @param <T> Type to process
   */
  @SuppressWarnings("PublicInnerClass")
  public abstract static class TargetFunc<T> {

    /**
     * Gets called with the current item
     *
     * @param data Current item
     * @throws Exception Any exception from implementing class
     */
    public abstract void call(final T data)
        throws Exception;

    /**
     * Get the name of this class.
     *
     * @return Name
     */
    public final String getName() {
      return this.getClass().getSimpleName() + "-" + this.hashCode();
    }
  }

  public abstract static class TargetFuncFactory<T>
      extends TargetFunc<T> {
    public abstract TargetFunc<T> newInstance()
        throws TargetException;
  }
}
