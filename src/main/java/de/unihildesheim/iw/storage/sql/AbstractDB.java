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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * Abstract database class as basis for specifc implementations.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public abstract class AbstractDB
    implements AutoCloseable {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractDB.class);

  /**
   * Get the current connection object for this database.
   *
   * @return Connection
   */
  public abstract Connection getConnection();

  /**
   * Get a collection of {@link Table} types allowed to be contained in this
   * database.
   *
   * @return Collection of allowed {@link Table} classes
   */
  protected abstract Collection<Class<? extends Table>> getAcceptedTables();

  /**
   * Constructor adding a shutdown hook to close the database on JVM exit.
   */
  protected AbstractDB() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @SuppressWarnings("PrivateMemberAccessBetweenOuterAndInnerClass")
      @Override
      public void run() {
        close();
      }
    });
  }

  /**
   * Check, if the given table exists in the database.
   *
   * @param tableName Table name to check for
   * @return True, if table exists
   * @throws SQLException Thrown on database errors
   */
  public boolean hasTable(@NotNull final String tableName)
      throws SQLException {
    final Statement stmt = getConnection().createStatement();
    return stmt.execute("SELECT name FROM sqlite_master WHERE type='table' " +
        "AND name='" + tableName + '\'');
  }

  public long getNumberOfRows(@NotNull final String tableName)
      throws SQLException {
    final Statement stmt = getConnection().createStatement();
    if (stmt.execute("SELECT count(*) from '" + tableName + '\'')) {
      return stmt.getResultSet().getLong(1);
    }
    return 0L;
  }

  /**
   * Create all required tables, if the do not exist already.
   *
   * @param tables List of tables to create
   * @throws SQLException Thrown on database errors
   */
  public void createTables(@NotNull final Table... tables)
      throws SQLException {
    createTables(false, tables);
  }

  /**
   * Create all required tables, if the do not exist already.
   *
   * @param force If true, deletes any table that already exists before creating
   * it
   * @param tables List of tables to create
   * @throws SQLException Thrown on database errors
   */
  @SuppressWarnings("ReuseOfLocalVariable")
  public void createTables(final boolean force, @NotNull final Table... tables)
      throws SQLException {
    if (tables.length == 0) {
      throw new IllegalArgumentException("No tables specified.");
    }
    final Statement stmt = getConnection().createStatement();
    final Collection<Class<? extends Table>> allowedTables =
        getAcceptedTables();

    for (final Table tbl : tables) {
      if (!allowedTables.contains(tbl.getClass())) {
        throw new IllegalArgumentException("Table type " + tbl.getClass() +
            " is not allowed in this database.");
      }
      @SuppressWarnings("ObjectAllocationInLoop")
      final StringBuilder tblSql = new StringBuilder(
          "create table if not exists ")
          .append(tbl.getName()).append(" (");

      String prefix = "";
      for (final TableField f : tbl.getFields()) {
        tblSql.append(prefix).append(f.getSql());
        prefix = ",";
      }

      if (!tbl.getUniqueColumns().isEmpty()) {
        tblSql.append(", unique(");
        prefix = "";
        for (final String uf : tbl.getUniqueColumns()) {
          tblSql.append(prefix).append(uf);
          prefix = ",";
        }
        tblSql.append(')');
      }

      tblSql.append(')');

      if (force) {
        stmt.executeUpdate("drop table if exists " + tbl.getName());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("createSQL: {}", tblSql);
      }
      stmt.executeUpdate(tblSql.toString());
    }
    stmt.close();
  }

  @Override
  public void close() {
    try {
      if (getConnection() != null && !getConnection().isClosed()) {
        LOG.info("Closing database connection.");
        getConnection().close();
        if (getConnection().isClosed()) {
          LOG.info("Database connection closed.");
        }
      }
    } catch (final SQLException e) {
      LOG.error("Error closing database connection.", e);
    }
  }
}
