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

package de.unihildesheim.iw.storage.sql;

import org.jetbrains.annotations.NotNull;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public abstract class AbstractTable
    implements Table {
  /**
   * Check, if a field is present in the current tables fields list. Throws a
   * {@link IllegalArgumentException} if the field is not known (i.e. invalid).
   *
   * @param fld Field to check (toString will be called on the object)
   */
  protected void checkFieldIsValid(@NotNull final Object fld) {
    if (!getFields().stream()
        .filter(f -> f.getName().equalsIgnoreCase(fld.toString()))
        .findFirst().isPresent()) {
      throw new IllegalArgumentException("Unknown field '" + fld + '\'');
    }
  }
}
