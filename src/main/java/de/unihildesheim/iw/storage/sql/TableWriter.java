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
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public abstract class TableWriter
    implements AutoCloseable {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TableWriter.class);
  /**
   * Table.
   */
  private final Table tbl;
  /**
   * Database connection.
   */
  private final Connection con;
  /**
   * Database insert counter for committing.
   */
  private final AtomicLong insertCount = new AtomicLong();
  /**
   * Run a commit after n inserts, if autocommit is disabled.
   */
  private static final long COMMIT_COUNT = 500L;

  public TableWriter(
      @NotNull final Connection con,
      @NotNull final Table table)
      throws SQLException {
    this.tbl = table;
    this.con = con;
    this.con.setAutoCommit(false);
  }

  /**
   * Commit changes to the database for this table.
   * @throws SQLException Thrown on low-level SQL-errors
   */
  public void commit()
      throws SQLException {
    this.con.commit();
  }

  @Override
  public void close()
      throws SQLException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing down table {}.", this.tbl.getName());
    }
    if (!this.con.isClosed()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Commiting changes to database before closing table {}.",
            this.tbl.getName());
      }
      commit();
    }
  }

  /**
   * Calls {@link #addContent(TableFieldContent, boolean)} with ignore always
   * {@code true}.
   *
   * @param tfContent Content to insert
   * @return Statement after execution
   * @throws SQLException SQLException Declared for overriding implementations
   * @see #addContent(TableFieldContent, boolean)
   */
  public Statement addContent(final TableFieldContent tfContent)
      throws SQLException {
    return addContent(tfContent, true);
  }

  /**
   * Default implementation that only checks, if the passed in {@link
   * TableFieldContent} instance matches the required instance.
   *
   * @param tfContent Content to insert
   * @param ignore If true, constraint violations should be ignored (new row
   * data is dropped)
   * @return Statement after execution.
   * @throws SQLException Declared for overriding implementations
   */
  public Statement addContent(
      @NotNull final TableFieldContent tfContent,
      final boolean ignore)
      throws SQLException {
    if (!this.tbl.getClass().isInstance(tfContent.getTable())) {
      throw new IllegalArgumentException("Wrong table. Expected '" + this.tbl
          .getClass() + "' but got '" + tfContent.getTable().getClass() + "'.");
    }

    final StringBuilder sql = new StringBuilder("insert ");
    if (ignore) {
      sql.append("or ignore ");
    }
    sql.append("into ").append(getTableName());

    final PreparedStatement pStmt = tfContent.prepareInsert(
        this.con, sql.toString());

    pStmt.executeUpdate();

    if (this.insertCount.incrementAndGet() >= COMMIT_COUNT) {
      commit();
      this.insertCount.set(0L);
    }
    return pStmt;
  }

  public String getTableName() {
    return this.tbl.getName();
  }
}
