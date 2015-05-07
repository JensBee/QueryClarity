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

package de.unihildesheim.iw.storage.sql.IPCStats;

import de.unihildesheim.iw.storage.sql.AbstractDB;
import de.unihildesheim.iw.storage.sql.MetaTable;
import de.unihildesheim.iw.storage.sql.Table;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class IPCStatsDB
    extends AbstractDB {
  /**
   * Database connection.
   */
  final Connection connection;
  /**
   * Tables allowed to be created in this database.
   */
  private static final Collection<Class<? extends Table>> ACCEPTED_TABLES =
      Collections.unmodifiableList(Arrays.asList(
          MetaTable.class,
          AllIPCTable.class,
          IPCDistributionTable.class,
          IPCSectionsTable.class,
          IPCPerDocumentTable.class));

  /**
   * New instance.
   *
   * @param dbFile SQLite database file
   * @throws ClassNotFoundException Thrown, if the {@code org.sqlite.JDBC}
   * JDBC driver could not be loaded.
   * @throws SQLException Thrown, if connection to the database has failed
   */
  public IPCStatsDB(@NotNull final File dbFile)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.sqlite.JDBC");
    this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile);
  }

  @Override
  public Connection getConnection() {
    return this.connection;
  }

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  @Override
  protected Collection<Class<? extends Table>> getAcceptedTables() {
    return ACCEPTED_TABLES;
  }
}
