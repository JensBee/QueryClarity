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

package de.unihildesheim.iw;

import de.unihildesheim.iw.util.RandomValue;
import org.mapdb.DB;
import org.mapdb.DBMaker;

/**
 * Base class for MapDB performance tests.
 *
 * @author Jens Bertram
 */
public class PerformanceTestCase
    extends TestCase {
  /**
   * Number of iterations to perform for each test.
   */
  protected static final int RUNS = 3;

  /**
   * Node-sizes being tested for BTree {@link DB.BTreeMapMaker#nodeSize(int)
   * Maps} and {@link DB.BTreeSetMaker#nodeSize(int) Sets}.
   */
  protected static final int[] NODE_SIZES = {6, 18, 32, 64, 120};

  /**
   * Create a new temporary file database using a small cache size, to simulate
   * much larger store with relatively small cache.
   *
   * @return Database instance
   */
  protected static DB getTempDB() {
    return DBMaker.newTempFileDB()
        .deleteFilesAfterClose()
        .closeOnJvmShutdown()
        .transactionDisable()
        .cacheSize(10)
        .compressionEnable()
        .make();
  }

  /**
   * Creates a random testing term.
   *
   * @return Term
   */
  protected static ByteArray getTerm() {
    return new ByteArray(RandomValue.getString(2, 15).getBytes());
  }

  /**
   * Map configuration types.
   */
  @SuppressWarnings("ProtectedInnerClass")
  protected enum MapNodeType {
    /**
     * Values stored inside nodes.
     */
    INSIDE,
    /**
     * Values stored outside of nodes.
     */
    OUTSIDE
  }
}
