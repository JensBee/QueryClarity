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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteErrorCode;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

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
   * @throws SQLException Thrown on low-level database errors
   */
  public boolean hasTable(@NotNull final String tableName)
      throws SQLException {
    final Statement stmt = getConnection().createStatement();
    // Simply try to select from column. SQLite throws an exception, if it does
    // not exist.
    try {
      stmt.execute("select * from " + tableName + " limit 1");
    } catch (final SQLException e) {
      final int eCode = e.getErrorCode();

      if (eCode == SQLiteErrorCode.SQLITE_ERROR.code) {
        // thrown, if column does not exist
        return false;
      } else {
        // any other error is unhandled
        LOG.error("Error checking for table existence.", e);
        throw e;
      }
    }
    final ResultSet rs = stmt.getResultSet();
    return rs.next();
  }

  /**
   * Get the number of row available in the named table.
   *
   * @param tableName Table to count rows for
   * @return Number of rows in table
   * @throws SQLException Thrown on low-level database errors
   */
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
   * @throws SQLException Thrown on low-level database errors
   */
  public void createTables(@NotNull final Table... tables)
      throws SQLException {
    createTables(false, tables);
  }

  /**
   * Create all required tables, if the table do not exist already.
   *
   * @param force If true, deletes any table that already exists before creating
   * it
   * @param tables List of tables to create
   * @throws SQLException Thrown on low-level database errors
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

      final String staticPrefix = ",";
      final AtomicBoolean usePrefix = new AtomicBoolean(false);
      // normal fields
      tbl.getFields().stream()
          .filter(f -> !f.getName().toLowerCase().endsWith("_fk"))
          .forEach(f -> {
            if (usePrefix.get()) {
              tblSql.append(staticPrefix);
            }
            tblSql.append(f.getSql());
            usePrefix.set(true);
          });
      // foreign keys
      tbl.getFields().stream()
          .filter(f -> f.getName().toLowerCase().endsWith("_fk"))
          .forEach(f -> {
            if (usePrefix.get()) {
              tblSql.append(staticPrefix);
            }
            tblSql.append(f.getSql());
            usePrefix.set(true);
          });

      String prefix = "";
//      for (final TableField f : tbl.getFields()) {
//        tblSql.append(prefix).append(f.getSql());
//        prefix = ",";
//      }

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
      if (LOG.isTraceEnabled()) {
        LOG.trace("createSQL: {}", tblSql);
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

  /**
   * Check, if a column is available in the given table.
   * @param tableName Table name to check
   * @param con Connection to the database
   * @param field Field/Column to check for
   * @return True, if it exists
   * @throws SQLException Thrown on low-level SQL errors
   */
  public static boolean hasTableField(
      @NotNull final String tableName,
      @NotNull final Connection con,
      @NotNull final Object field)
      throws SQLException {
    final Statement stmt = con.createStatement();
    // Simply try to select from column. SQLite throws an exception, if it does
    // not exist.
    try {
      stmt.execute("select " + field + " from " + tableName + " limit 1");
    } catch (final SQLException e) {
      final int eCode = e.getErrorCode();

      if (eCode == SQLiteErrorCode.SQLITE_ERROR.code) {
        // thrown, if column does not exist
        return false;
      } else {
        // any other error is unhandled
        LOG.error("Error checking table column existence.", e);
        throw e;
      }
    }
    final ResultSet rs = stmt.getResultSet();
    return rs.next();
  }

  /**
   * Check, if a column is available in the given table.
   * @param tableName Table name to check
   * @param field Field/Column to check for
   * @return True, if it exists
   * @throws SQLException Thrown on low-level SQL errors
   * @see #hasTableField(String, Connection, Object)
   */
  public boolean hasTableField(
      @NotNull final String tableName,
      @NotNull final Object field)
      throws SQLException {
    return hasTableField(tableName, this.getConnection(), field);
  }
}
