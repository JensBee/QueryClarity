/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net
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
 * @author bhoerdzn
 */
public class DataProviderException
    extends Exception {
  public DataProviderException(final String msg, final Exception ex) {
    super(msg, ex);
  }

  public DataProviderException(final String msg) {
    super(msg);
  }

  public DataProviderException(final Exception ex) {
    super(ex);
  }

  public static final class CacheException
      extends DataProviderException {
    public CacheException(final String msg, final Exception ex) {
      super(msg, ex);
    }

    public CacheException(final String msg) {
      super(msg);
    }
  }
}