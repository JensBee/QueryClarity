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

package de.unihildesheim.iw.fiz.storage.sql;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public interface Table {
  /**
   * Get a list of fields for this table. The returned list should not be
   * modified by the caller. Fields returned by this method are also used for
   * creating fields.
   *
   * @return List of fields
   */
  @NotNull
  List<TableField> getFields();

  /**
   * Get a list of fields that may contain content. The returned list should not
   * be modified by the caller. Fields returned by this method are used for
   * preparing insert statements. Any fields not used for content (e.g. foreign
   * key fields) should not be returned. The default implementation returns the
   * same content as {@link #getFields()}.
   *
   * @return List of fields
   */
  @NotNull
  default List<TableField> getContentFields() {
    return getFields();
  }

  /**
   * Get the name of the table.
   *
   * @return Table name
   */
  @NotNull
  String getName();

  /**
   * Get a list of columns that for a unique constraint.
   *
   * @return List of columns that for a unique constraint
   */
  Set<String> getUniqueColumns();

  /**
   * Add a field to the list of fields forming a unique constraint.
   *
   * @param fld Field name to add. {@code toString()} is called on the passed in
   * object.
   */
  void addFieldToUnique(@NotNull final Object fld);

  /**
   * Add all default fields to the list of fields forming a unique constraint.
   */
  void addDefaultFieldsToUnique();

  /**
   * Get a writer for this table (if the implementation supports this).
   *
   * @param con Database connection
   * @return Table writer instance for this table
   */
  default TableWriter getWriter(@NotNull final Connection con)
      throws SQLException {
    throw new UnsupportedOperationException();
  }
}
