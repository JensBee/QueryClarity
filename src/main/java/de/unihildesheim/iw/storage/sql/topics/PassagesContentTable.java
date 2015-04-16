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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class PassagesContentTable
    implements Table {

  /**
   * Fields belonging to this table.
   */
  private final List<TableField> fields;

  public enum Fields {
    ID, SOURCE, CONTENT;

    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  }

  /**
   * Default fields for this table.
   */
  public static final List<TableField> DEFAULT_FIELDS =
      Collections.unmodifiableList(Arrays.asList(
          new TableField(Fields.ID.toString(), Fields.ID +
              " integer primary key not null"),
          new TableField(Fields.SOURCE.toString(), Fields.SOURCE +
              " text not null"),
          new TableField(Fields.CONTENT.toString(), Fields.CONTENT +
              " text not null")));

  /**
   * Table name.
   */
  static final String TABLE_NAME = "PassagesContent";

  final Set<String> uniqueFields = new HashSet<>();

  /**
   * Create a new instance using the default fields.
   */
  public PassagesContentTable() {
    this(DEFAULT_FIELDS);
    addDefaultFieldsToUnique();
  }

  /**
   * Create a new instance using the specified fields.
   *
   * @param newFields Fields to use
   */
  public PassagesContentTable(
      @NotNull final Collection<TableField> newFields) {
    this.fields = new ArrayList<>(newFields.size());
    this.fields.addAll(newFields);
  }

  @Override
  public List<TableField> getFields() {
    return Collections.unmodifiableList(this.fields);
  }

  @Override
  public String getName() {
    return TABLE_NAME;
  }

  /**
   * Provides write access to the table.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Writer
      extends TableWriter {

    /**
     * Create a new writer using the default table name.
     *
     * @param con Database connection
     * @throws SQLException
     */
    public Writer(@NotNull final Connection con)
        throws SQLException {
      super(con, new PassagesContentTable());
    }

    /**
     * Create a new writer using the specified table name.
     *
     * @param con Database connection
     * @param tbl
     * @throws SQLException
     */
    public Writer(
        @NotNull final Connection con,
        @NotNull final Table tbl)
        throws SQLException {
      super(con, tbl);
    }
  }

  @Override
  public Set<String> getUniqueColumns() {
    return Collections.unmodifiableSet(this.uniqueFields);
  }

  @Override
  public void addFieldToUnique(@NotNull final Object fld) {
    final boolean validField = this.fields.stream()
        .filter(f -> f.getName().equals(fld.toString())).findFirst()
        .isPresent();
    if (!validField) {
      throw new IllegalArgumentException("Unknown field '" + fld + '\'');
    }
    this.uniqueFields.add(fld.toString());
  }

  @Override
  public void addDefaultFieldsToUnique() {
    this.uniqueFields.add(Fields.SOURCE.toString());
    this.uniqueFields.add(Fields.CONTENT.toString());
  }
}
