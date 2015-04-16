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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class TopicsDB {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TopicsDB.class);
  /**
   * Database connection.
   */
  final Connection connection;

  /**
   * New instance.
   *
   * @param dbFile SQLite database file
   * @throws ClassNotFoundException Thrown, if the {@code org.sqlite.JDBC} JDBC
   * driver could not be loaded.
   * @throws SQLException Thrown, if connection to the database has failed
   */
  public TopicsDB(@NotNull final File dbFile)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.sqlite.JDBC");

    this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          if (TopicsDB.this.connection != null &&
              !TopicsDB.this.connection.isClosed()) {
            LOG.info("Closing database connection.");
            TopicsDB.this.connection.close();
            if (TopicsDB.this.connection.isClosed()) {
              LOG.info("Database connection closed.");
            }
          }
        } catch (final SQLException e) {
          LOG.error("Error closing database connection.", e);
        }
      }
    });
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
  public void createTables(final boolean force, @NotNull final Table... tables)
      throws SQLException {
    if (tables.length == 0) {
      throw new IllegalArgumentException("No tables specified.");
    }
    final Statement stmt = this.connection.createStatement();

    for (final Table tbl : tables) {
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
      LOG.debug("createSQL: {}", tblSql);
      stmt.executeUpdate(tblSql.toString());
    }
    stmt.close();
  }

  public Connection getConnection() {
    return this.connection;
  }
}
