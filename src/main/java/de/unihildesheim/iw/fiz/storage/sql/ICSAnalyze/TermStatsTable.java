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

package de.unihildesheim.iw.fiz.storage.sql.ICSAnalyze;

import de.unihildesheim.iw.fiz.storage.sql.AbstractTable;
import de.unihildesheim.iw.fiz.storage.sql.Table;
import de.unihildesheim.iw.fiz.storage.sql.TableField;
import de.unihildesheim.iw.fiz.storage.sql.TableWriter;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class TermStatsTable extends AbstractTable {
  /**
   * Fields belonging to this table.
   */
  private final List<TableField> fields;
  /**
   * Table name.
   */
  public static final String TABLE_NAME = "term_stats";

  /**
   * Fields in this table.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum Fields {
    /**
     * Auto-generated id.
     */
    ID("id integer primary key not null"),
    /**
     * Term value.
     */
    TERM("term text not null"),
    /**
     * Relative document frequency.
     */
    DF_REL("df_rel real"),
    /**
     * Document frequency.
     */
    DF("df integer"),
    /**
     * Relative term frequency.
     */
    TF_REL("tf_rel real"),
    /**
     * Term frequency.
     */
    TF("tf integer");

    /**
     * SQL code to create this field.
     */
    private final String sqlStr;

    /**
     * Create a new field instance with the given SQL code to create the field
     * in the database.
     *
     * @param sql SQL code to create this field.
     */
    Fields(@NotNull final String sql) {
      this.sqlStr = sql;
    }

    @Override
    public String toString() {
      return this.name().toLowerCase();
    }

    /**
     * Get the current field as {@link TableField} instance.
     *
     * @return {@link TableField} instance for the current field
     */
    public TableField getAsTableField() {
      return new TableField(toString(), this.sqlStr);
    }
  }

  /**
   * Create a new instance using the default fields.
   */
  public TermStatsTable() {
    this.fields = Arrays.stream(Fields.values())
        .map(Fields::getAsTableField).collect(Collectors.toList());
  }

  @NotNull
  @Override
  public List<TableField> getFields() {
    return Collections.unmodifiableList(this.fields);
  }

  @NotNull
  @Override
  public String getName() {
    return TABLE_NAME;
  }

  @Override
  public Set<String> getUniqueColumns() {
    return Collections.emptySet();
  }

  @Override
  public void addFieldToUnique(@NotNull final Object fld) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addDefaultFieldsToUnique() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Writer getWriter(@NotNull final Connection con)
      throws SQLException {
    return new Writer(con);
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
     * @throws SQLException Thrown on low-level SQL errors
     */
    public Writer(@NotNull final Connection con)
        throws SQLException {
      super(con, new TermStatsTable());
    }

    /**
     * Create a new writer using the specified table name.
     *
     * @param con Database connection
     * @param tbl Table instance
     * @throws SQLException Thrown on low-level SQL errors
     */
    public Writer(
        @NotNull final Connection con,
        @NotNull final Table tbl)
        throws SQLException {
      super(con, tbl);
    }
  }
}

