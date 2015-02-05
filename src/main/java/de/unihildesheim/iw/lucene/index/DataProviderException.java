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

package de.unihildesheim.iw.lucene.index;

/**
 * Exceptions for {@link IndexDataProvider} implementations.
 *
 * @author Jens Bertram
 */
public class DataProviderException
    extends Exception {
  /**
   * Serialization id.
   */
  private static final long serialVersionUID = -149719728275656149L;

  /**
   * General Exception including a {@link Throwable}.
   *
   * @param msg Message
   * @param t Throwable
   */
  public DataProviderException(final String msg, final Throwable t) {
    super(msg, t);
  }

  /**
   * General Exception.
   *
   * @param msg Message
   */
  public DataProviderException(final String msg) {
    super(msg);
  }
}