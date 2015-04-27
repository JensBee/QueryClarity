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

package de.unihildesheim.iw.storage.sql.scoringData;

import de.unihildesheim.iw.storage.sql.Table;
import de.unihildesheim.iw.storage.sql.TableField;
import de.unihildesheim.iw.storage.sql.TableWriter;
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
public class SentenceScoringTable
    implements Table {
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
  public static final String TABLE_NAME = "scoring_terms";

  /**
   * Fields in this table.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum Fields {
    /**
     * Auto-generated id.
     */
    ID,
    /**
     * Term as string.
     */
    SENTENCE,
    /**
     * Language the entry belongs to.
     */
    LANG,
    /**
     * Term reference.
     */
    TERM_REF;

    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  }

  /**
   * Default fields for this table.
   */
  @SuppressWarnings("PublicStaticCollectionField")
  public static final List<TableField> DEFAULT_FIELDS =
      Collections.unmodifiableList(Arrays.asList(
          new TableField(Fields.ID.toString(), Fields.ID +
              " integer primary key not null"),
          new TableField(Fields.SENTENCE.toString(), Fields.SENTENCE +
              " text not null"),
          new TableField(Fields.LANG.toString(), Fields.LANG +
              " char(2) not null"),
          new TableField(Fields.TERM_REF.toString(),
              "foreign key (" + Fields.TERM_REF + ") references " +
                  TermScoringTable.TABLE_NAME +
                  '(' + TermScoringTable.Fields.ID + ')')));

  /**
   * Create a new instance using the default fields.
   */
  public SentenceScoringTable() {
    this(DEFAULT_FIELDS);
    addDefaultFieldsToUnique();
  }

  /**
   * Create a new instance using the specified fields.
   *
   * @param newFields Fields to use
   */
  public SentenceScoringTable(
      @NotNull final Collection<TableField> newFields) {
    this.fields = new ArrayList<>(newFields.size());
    this.fields.addAll(newFields);
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
    final boolean invalidField = !this.fields.stream()
        .filter(f -> f.getName().equals(fld.toString())).findFirst()
        .isPresent();
    if (invalidField) {
      throw new IllegalArgumentException("Unknown field '" + fld + '\'');
    }
    this.uniqueFields.add(fld.toString());
  }

  @Override
  public void addDefaultFieldsToUnique() {
    this.uniqueFields.add(Fields.SENTENCE.toString());
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
      super(con, new SentenceScoringTable());
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
