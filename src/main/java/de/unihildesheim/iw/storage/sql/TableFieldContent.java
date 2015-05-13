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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class TableFieldContent {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TableFieldContent.class);
  private final Table tbl;
  private final Map<String, String> content;

  public TableFieldContent(@NotNull final Table table) {
    this.tbl = table;

    final Collection<TableField> fields = this.tbl.getContentFields();
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

  public PreparedStatement prepareInsert(
      @NotNull final Connection con,
      @NotNull final String sql)
      throws SQLException {
    final StringBuilder finalSql = new StringBuilder(sql);
    final List<String> fieldNames = new ArrayList<>(this.content.keySet());
    Collections.sort(fieldNames);

    finalSql.append(" (");

    String fldPrefix = "";
    final StringBuilder valueStr = new StringBuilder("(");
    for (final String fieldName : fieldNames) {
      finalSql.append(fldPrefix).append(fieldName);
      valueStr.append(fldPrefix).append('?');
      fldPrefix = ",";
    }

    finalSql.append(") values ").append(valueStr).append(')');

    final PreparedStatement prep = con.prepareStatement(finalSql.toString());

    final int fieldCount = fieldNames.size();
    for (int i = 1; i <= fieldCount; i++) {
      prep.setString(i, this.content.get(fieldNames.get(i - 1)));
    }

    return prep;
  }
}
