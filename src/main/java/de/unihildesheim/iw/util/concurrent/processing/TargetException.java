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
 * Custom {@link Exception}s for {@link Target}s.
 *
 * @author Jens Bertram
 */
public class TargetException
    extends ProcessingException {

  private static final long serialVersionUID = -5712464107224942957L;

  /**
   * General exception with custom message.
   *
   * @param msg Message
   */
  public TargetException(final String msg) {
    super(msg);
  }

  /**
   * Create a new exception with custom message and a {@link Throwable} for
   * forwarding.
   *
   * @param msg Message
   * @param t Throwable to forward
   */
  public TargetException(final String msg, final Throwable t) {
    super(msg, t);
  }

  /**
   * Exception indicating a {@link Target} has failed.
   */
  public static final class TargetFailedException
      extends ProcessingException {

    private static final long serialVersionUID = 6307633346087284282L;

    /**
     * General exception with custom message.
     *
     * @param msg Message
     */
    public TargetFailedException(final String msg) {
      super(msg);
    }

    /**
     * Create a new exception with custom message and a {@link Throwable} for
     * forwarding.
     *
     * @param msg Message
     * @param t Throwable to forward
     */
    public TargetFailedException(final String msg, final Throwable t) {
      super(msg, t);
    }
  }
}
