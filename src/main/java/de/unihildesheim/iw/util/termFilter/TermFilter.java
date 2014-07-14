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

package de.unihildesheim.iw.util.termFilter;

import de.unihildesheim.iw.ByteArray;

import java.io.Serializable;

/**
 * @author Jens Bertram
 */
public abstract class TermFilter
    implements Serializable {

  /**
   * Filters the term by modifying it, or ignoring it by returning {@code
   * null}.
   *
   * @param term Term to filter
   * @return The unmodified or modified term or null, to skip this term.
   */
  public abstract ByteArray filter(final ByteArray term);

  public abstract boolean equals(final TermFilter other);
}
