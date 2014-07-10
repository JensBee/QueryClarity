/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net>
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

package de.unihildesheim.iw.mapdb;

import org.mapdb.DBMaker;

/**
 * @author Jens Bertram
 */
public final class DBMakerUtils {

  /**
   * Creates a new compressed temporary file database without transactions that
   * gets deleted when closed or on JVM shutdown.
   *
   * @return pre-configured DBMaker instance
   * @see #newTempFileDB()
   */
  public static DBMaker newCompressedTempFileDB() {
    return newTempFileDB().compressionEnable();
  }

  /**
   * Creates a new temporary file database without transactions that gets
   * deleted when closed or on JVM shutdown.
   *
   * @return pre-configured DBMaker instance
   */
  public static DBMaker newTempFileDB() {
    return DBMaker.newTempFileDB()
        .closeOnJvmShutdown()
        .deleteFilesAfterClose()
        .mmapFileEnableIfSupported()
        .transactionDisable();
  }

  public static DBMaker newCompressedMemoryDirectDB() {
    return newMemoryDirectDB()
        .compressionEnable();
  }

  public static DBMaker newMemoryDirectDB() {
    return DBMaker.newMemoryDirectDB()
        .transactionDisable();
  }
}
