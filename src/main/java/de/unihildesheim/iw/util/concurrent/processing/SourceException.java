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
 * Custom {@link Exception}s for {@link Source}s.
 *
 * @author Jens Bertram
 */
public final class SourceException
    extends ProcessingException {
  private static final long serialVersionUID = -6432781390484045213L;

  /**
   * General exception with custom message.
   *
   * @param msg Message
   */
  SourceException(final String msg) {
    super(msg);
  }

  /**
   * Create a new exception with custom message and a {@link Throwable} for
   * forwarding.
   *
   * @param msg Message
   * @param t Throwable to forward
   */
  SourceException(final String msg, final Throwable t) {
    super(msg, t);
  }

  /**
   * Exception indicating that the {@link Source} has encountered an error and
   * cannot proceed.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SourceFailedException
      extends ProcessingException {

    private static final long serialVersionUID = -6894137346922047523L;

    /**
     * Create a new exception with custom message.
     *
     * @param msg Message
     */
    public SourceFailedException(final String msg) {
      super(msg);
    }

    /**
     * Create a new exception with custom message and a {@link Throwable} for
     * forwarding.
     *
     * @param msg Message
     * @param t Throwable to forward
     */
    public SourceFailedException(final String msg, final Throwable t) {
      super(msg, t);
    }
  }

  /**
   * Exception to indicate that the {@link Source} is not ready to serve items.
   * <br> Should be thrown, if the {@link Source} has not yet been initialized
   * and items or other information is being requested, but not available yet.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SourceNotReadyException
      extends ProcessingException {

    private static final long serialVersionUID = -6669588774093776128L;

    /**
     * Create a new exception with default message.
     */
    public SourceNotReadyException() {
      super("Source is not running.");
    }

    /**
     * Create a new exception with custom message.
     *
     * @param msg Message
     */
    public SourceNotReadyException(final String msg) {
      super(msg);
    }
  }

  /**
   * Exception to indicate that the {@link Source} has no more items to process.
   * <br> Should be thrown, if a {@link Target} requests an item, but the {@link
   * Source} has already been finished with providing items.
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

    /**
     * Create a new Exception with custom message.
     *
     * @param msg Message
     */
    public SourceHasFinishedException(final String msg) {
      super(msg);
    }
  }

  /**
   * Exception to indicate that the {@link Source} has already been started.
   * <br> Should be thrown, if the {@link Source} is tried to be started more
   * than once.
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

    /**
     * Create a new Exception with custom message.
     *
     * @param msg Message
     */
    public SourceIsRunningException(final String msg) {
      super(msg);
    }
  }
}
