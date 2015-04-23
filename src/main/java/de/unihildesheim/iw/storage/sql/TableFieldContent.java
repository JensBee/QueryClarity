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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class TableFieldContent {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TableFieldContent.class);
  private static final Pattern QUOTE_SINGLE = Pattern.compile("'");
  private static final Pattern QUOTE_DOUBLE = Pattern.compile("\"");

  private final Table tbl;
  private final Map<String, String> content;

  public TableFieldContent(@NotNull final Table table) {
    this.tbl = table;

    final Collection<TableField> fields = this.tbl.getFields();
    this.content = new HashMap<>(fields.size());
    for (final TableField f : fields) {
      this.content.put(f.getName(), null);
    }
  }

  public Table getTable() {
    return this.tbl;
  }

  public TableFieldContent setValue(
      @NotNull final Object fieldName,
      @NotNull final Object value) {
    if (!this.content.containsKey(fieldName.toString())) {
      throw new IllegalArgumentException("Unknown field '" + fieldName + "'.");
    }
    this.content.put(fieldName.toString(), value.toString());
    return this;
  }

  public String getValue(@NotNull final String fieldName) {
    if (!this.content.containsKey(fieldName)) {
      throw new IllegalArgumentException("Unknown field '" + fieldName + "'.");
    }
    return this.content.get(fieldName);
  }

  public String getSQLInsertString() {
    final StringBuilder fld = new StringBuilder("(");
    final StringBuilder cnt = new StringBuilder("(");

    String fldPrefix = "";
    String cntPrefix = "";
    for (final Entry<String, String> ce : this.content.entrySet()) {
      if (ce.getValue() != null) {
        // column name
        fld.append(fldPrefix);
        final String colName = QUOTE_SINGLE.matcher(ce.getKey()).replaceAll("''");
        fld.append('\'').append(colName).append('\'');

        // column value
        cnt.append(cntPrefix);
        final String colVal = QUOTE_SINGLE.matcher(ce.getValue()).replaceAll("''");
        cnt.append('\'').append(colVal).append('\'');

        fldPrefix = ",";
        cntPrefix = ",";
      }
    }
    fld.append(')');
    cnt.append(')');
    return fld + " values " + cnt;
  }

  public String getSQLQueryString() {
    final StringBuilder sql = new StringBuilder();

    String prefix = "";
    for (final Entry<String, String> ce : this.content.entrySet()) {
      sql.append(prefix);

      // column name
      sql.append('"').append(ce.getKey()).append('"');

      // column value
      if (ce.getValue() != null) {
        final String colVal =
            QUOTE_SINGLE.matcher(ce.getValue()).replaceAll("''");
        sql.append("='").append(colVal).append('\'');
      } else {
        sql.append("is null");
      }

      prefix = " and ";
    }
    return sql.toString();
  }
}
