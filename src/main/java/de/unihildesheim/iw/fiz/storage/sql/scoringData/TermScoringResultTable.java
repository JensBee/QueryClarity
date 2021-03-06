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

package de.unihildesheim.iw.fiz.storage.sql.scoringData;

import de.unihildesheim.iw.data.IPCCode;
import de.unihildesheim.iw.fiz.storage.sql.AbstractTable;
import de.unihildesheim.iw.fiz.storage.sql.Table;
import de.unihildesheim.iw.fiz.storage.sql.TableField;
import de.unihildesheim.iw.fiz.storage.sql.TableWriter;
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
public final class TermScoringResultTable extends AbstractTable {
  /**
   * Fields belonging to this table.
   */
  private final List<TableField> fields;
  /**
   * Content fields belonging to this table.
   */
  private final List<TableField> contentFields;
  /**
   * Collection of fields that are required to contain unique values.
   */
  final Set<String> uniqueFields = new HashSet<>(Fields.values().length);
  /**
   * Table name.
   */
  public static final String TABLE_NAME = "scoring_terms_results";

  /**
   * Fields in this table.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum Fields {
    /**
     * Scorer implementation id.
     */
    IMPL("impl char(3) not null"),
    /**
     * Flag indicating, if result is empty.
     */
    IS_EMPTY("is_empty boolean default 0"),
    /**
     * If a result is empty this may contain a hint for the reason.
     */
    EMPTY_REASON("empty_reason text"),
    /**
     * Fields visible while scoring.
     */
    Q_FIELDS("q_fields text"),
    /**
     * IPC-filter set while scoring.
     */
    Q_IPC("q_ipc text(" + IPCCode.IPCRecord.MAX_LENGTH + ')'),
    /**
     * Scoring result value.
     */
    SCORE("score real not null"),
    /**
     * Term reference.
     */
    TERM_REF("term_ref integer not null"),
    /**
     * Term reference foreign key.
     */
    TERM_REF_FK("foreign key (" + TERM_REF + ") references " +
        TermScoringTable.TABLE_NAME + '(' + TermScoringTable.Fields.ID + ')'),
    /**
     * Number of feedback documents used.
     */
    FB_DOC_COUNT("fb_doc_count integer"),
    /**
     * Minimum number of query terms matched in any feedback document.
     */
    FB_TERMS_MIN("fb_terms_min integer"),
    /**
     * Maximum number of query terms matched in any feedback document.
     */
    FB_TERMS_MAX("fb_terms_max integer");

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
   * Optional fields in this table.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum FieldsOptional {
    /**
     * Term reference.
     */
    ICSCONF_REF("icsconf_ref integer not null"),
    /**
     * Term reference foreign key.
     */
    ICSCONF_REF_FK("foreign key (" + ICSCONF_REF + ") references " +
        ICSConfTable.TABLE_NAME + '(' + ICSConfTable.Fields.ID + ')');

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
  public TermScoringResultTable() {
    this.fields = Arrays.stream(Fields.values())
        .map(Fields::getAsTableField).collect(Collectors.toList());
    this.contentFields = Arrays.stream(Fields.values())
        .filter(f -> !f.toString().toLowerCase().endsWith("_fk"))
        .map(Fields::getAsTableField).collect(Collectors.toList());
    addDefaultFieldsToUnique();
  }

  /**
   * Create a new instance and add the given optional fields to the table.
   * @param optFields Optional fields to add to the {@link Fields default}
   * list of fields
   */
  public TermScoringResultTable(@NotNull final FieldsOptional... optFields) {
    this();
    for (final FieldsOptional fld : optFields) {
      if (!fld.toString().toLowerCase().endsWith("_fk")) {
        this.contentFields.add(fld.getAsTableField());
      }
      this.fields.add(fld.getAsTableField());
    }
  }

  @NotNull
  @Override
  public List<TableField> getFields() {
    return Collections.unmodifiableList(this.fields);
  }

  @NotNull
  @Override
  public List<TableField> getContentFields() {
    return Collections.unmodifiableList(this.contentFields);
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
    this.uniqueFields.add(Fields.IMPL.toString());
    this.uniqueFields.add(Fields.TERM_REF.toString());
    this.uniqueFields.add(Fields.Q_FIELDS.toString());
    this.uniqueFields.add(Fields.Q_IPC.toString());
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
      super(con, new TermScoringResultTable());
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
