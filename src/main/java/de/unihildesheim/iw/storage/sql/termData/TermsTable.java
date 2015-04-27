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

package de.unihildesheim.iw.storage.sql.termData;

import de.unihildesheim.iw.data.IPCCode.IPCRecord;
import de.unihildesheim.iw.storage.sql.AbstractTable;
import de.unihildesheim.iw.storage.sql.Table;
import de.unihildesheim.iw.storage.sql.TableField;
import de.unihildesheim.iw.storage.sql.TableWriter;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class TermsTable
    extends AbstractTable {
  /**
   * Fields belonging to this table.
   */
  private final List<TableField> fields;
  /**
   * Collection of fields that are required to contain unique values.
   */
  final Set<String> uniqueFields = new HashSet<>(Fields.values().length);
  /**
   * Table name.
   */
  public static final String TABLE_NAME = "terms";

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
     * Source field of this term.
     */
    FIELD("field text not null"),
    /**
     * Term as string.
     */
    TERM("term text not null"),
    /**
     * Language the entry belongs to.
     */
    LANG("lang char(2) not null"),
    /**
     * Relative document frequency value.
     */
    DOCFREQ_REL("docfreq_rel real not null"),
    /**
     * Absolute document frequency value.
     */
    DOCFREQ_ABS("docfreq_abs real not null");

    /**
     * SQL code to create this field.
     */
    private final String sqlStr;

    /**
     * Create a new field instance with the given SQL code to create the
     * field in the database.
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
     * @return {@link TableField} instance for the current field
     */
    public TableField getAsTableField() {
      return new TableField(toString(), this.sqlStr);
    }
  }

  /**
   * Optional fields in this table.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum FieldsOptional {
    /**
     * IPC code, if selection was restricted to any code.
     */
    IPC("ipc char(" + IPCRecord.MAX_LENGTH + ')');

    /**
     * SQL code to create this field.
     */
    private final String sqlStr;

    /**
     * Create a new field instance with the given SQL code to create the
     * field in the database.
     * @param sql SQL code to create this field.
     */
    FieldsOptional(@NotNull final String sql) {
      this.sqlStr = sql;
    }

    @Override
    public String toString() {
      return this.name().toLowerCase();
    }

    /**
     * Get the current field as {@link TableField} instance.
     * @return {@link TableField} instance for the current field
     */
    public TableField getAsTableField() {
      return new TableField(toString(), this.sqlStr);
    }
  }

  /**
   * Create a new instance using the default fields.
   */
  public TermsTable() {
    this.fields = Arrays.stream(Fields.values())
        .map(Fields::getAsTableField).collect(Collectors.toList());
    addDefaultFieldsToUnique();
  }

  /**
   * Create a new instance and add the given optional fields to the table.
   * @param optFields Optional fields to add to the {@link Fields default}
   * list of fields
   */
  public TermsTable(@NotNull final FieldsOptional... optFields) {
    this();
    for (final FieldsOptional fld : optFields) {
      if (fld == FieldsOptional.IPC) {
        this.uniqueFields.add(FieldsOptional.IPC.toString());
        this.fields.add(FieldsOptional.IPC.getAsTableField());
      }
    }
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
    return Collections.unmodifiableSet(this.uniqueFields);
  }

  @Override
  public void addFieldToUnique(@NotNull final Object fld) {
    checkFieldIsValid(fld);
    this.uniqueFields.add(fld.toString());
  }

  @Override
  public void addDefaultFieldsToUnique() {
    this.uniqueFields.add(Fields.FIELD.toString());
    this.uniqueFields.add(Fields.TERM.toString());
    this.uniqueFields.add(Fields.LANG.toString());
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
      super(con, new TermsTable());
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
