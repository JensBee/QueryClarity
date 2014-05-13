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

/**
 * Exceptions for {@link Source}s.
 */
public class ProcessingException
    extends Exception {
  /**
   * Serialization id.
   */
  private static final long serialVersionUID = -6304188198808418922L;

  /**
   * General exception with custom message.
   *
   * @param message Exception message
   */
  ProcessingException(final String message) {
    super(message);
  }

  ProcessingException(final String message, final Throwable t) {
    super(message, t);
  }

  public static final class TargetFailedException
      extends ProcessingException {

    /**
     * General exception with custom message.
     *
     * @param message Exception message
     */
    public TargetFailedException(final String message) {
      super(message);
    }

    public TargetFailedException(final String message, final Throwable t) {
      super(message, t);
    }
  }

  public static final class SourceFailedException
      extends ProcessingException {

    /**
     * General exception with custom message.
     *
     * @param message Exception message
     */
    public SourceFailedException(final String message) {
      super(message);
    }

    public SourceFailedException(final String message, final Throwable t) {
      super(message, t);
    }
  }

  /**
   * Exception to indicate that the {@link Source} is not ready to serve items.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SourceNotReadyException
      extends ProcessingException {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -6669588774093776128L;

    /**
     * Create a new exception with default message.
     */
    public SourceNotReadyException() {
      super("Source is not running.");
    }
  }

  /**
   * Exception to indicate that the {@link Source} has no more items to
   * process.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SourceHasFinishedException
      extends RuntimeException {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -673519211370841154L;

    /**
     * Create a new exception with default message.
     */
    public SourceHasFinishedException() {
      super("Source has no more items to process.");
    }
  }

  /**
   * Exception to indicate that the {@link Source} has already been started.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SourceIsRunningException
      extends RuntimeException {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -2412421413054287724L;

    /**
     * Create a new exception with default message.
     */
    public SourceIsRunningException() {
      super("Source has already been started.");
    }
  }
}
