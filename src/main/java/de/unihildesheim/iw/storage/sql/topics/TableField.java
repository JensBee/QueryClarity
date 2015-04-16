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

package de.unihildesheim.iw.storage.sql.topics;

import org.jetbrains.annotations.NotNull;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class TableField {
  /**
   * Field name.
   */
  private final String name;
  /**
   * SQL statement to create the field.
   */
  private final String sql;

  /**
   *
   * @param fieldName Field name
   * @param sql SQL statement to create the field
   */
  public TableField(
      @NotNull final String fieldName,
      @NotNull final String sql) {
    this.name = fieldName;
    this.sql = sql;
  }

  /**
   * Get the SQL statement to create the field.
   * @return SQL statement to create the field
   */
  public String getSql() {
    return this.sql;
  }

  /**
   * Get the field name.
   * @return Field name
   */
  public String getName() {
    return this.name;
  }
}
