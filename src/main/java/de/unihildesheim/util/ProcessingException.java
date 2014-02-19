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
 * Exceptions for {@link Processing.Source}s.
 */
public class ProcessingException extends Exception {

  /**
   * General exception with custom message.
   *
   * @param message Exception message
   */
  ProcessingException(final String message) {
    super(message);
  }

  /**
   * Exception to indicate that the {@link Processing.Source} is not ready to
   * serve items.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SourceNotReadyException
          extends ProcessingException {

    /**
     * Create a new exception with default message.
     */
    public SourceNotReadyException() {
      super("Source is not running.");
    }
  }

  /**
   * Exception to indicate that the {@link Processing.Source} has no more
   * items to process.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SourceHasFinishedException
          extends RuntimeException {

    /**
     * Create a new exception with default message.
     */
    public SourceHasFinishedException() {
      super("Source has no more items to process.");
    }
  }

  /**
   * Exception to indicate that the {@link Processing.Source} has already been
   * started.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SourceIsRunningException
          extends RuntimeException {

    /**
     * Create a new exception with default message.
     */
    public SourceIsRunningException() {
      super("Source has already been started.");
    }
  }
}
