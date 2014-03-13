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
package de.unihildesheim.lucene.index;

import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.Tuple;
import java.util.Collection;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link ExternalDocTermDataManager}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class ExternalDocTermDataManagerTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          ExternalDocTermDataManagerTest.class);
  /**
   * Database used for storing values.
   */
  private DB db;

  /**
   * Instance used during a test run.
   */
  private ExternalDocTermDataManager instance;

  public ExternalDocTermDataManagerTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
    DBMaker dbMkr = DBMaker.newTempFileDB();
    this.db = dbMkr.make();
    this.instance = getInstance();
  }

  @After
  public void tearDown() throws Exception {
  }

  /**
   * Get an instance ready for running tests.
   *
   * @return initialized instance
   */
  private ExternalDocTermDataManager getInstance() {
    final String prefix = RandomValue.getString(1, 10);
    return new ExternalDocTermDataManager(db, prefix);
  }

  /**
   * Test of clear method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testClear() throws Exception {
    LOG.info("Test clear");
    instance.clear();

    String prefix = RandomValue.getString(1, 10);
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);
    instance.loadPrefix(prefix);
    Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>> testData;
    testData = IndexTestUtils.generateTermData(null, docId, key, 100);

    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> data : testData) {
      instance.setData(prefix, data.a, data.b, data.c, data.d);
    }

    instance.getData(prefix, docId, key);

    instance.clear();
    instance.getData(prefix, docId, key);
  }

  /**
   * Test of loadPrefix method, of class ExternalDocTermDataManager.
   */
  @Test
  public void testLoadPrefix() {
    LOG.info("Test loadPrefix");
    String prefix = RandomValue.getString(1, 10);
    instance.loadPrefix(prefix);
  }

  /**
   * Test of setData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetData() throws Exception {
    LOG.info("Test setData");
    String prefix = RandomValue.getString(1, 10);
    instance.loadPrefix(prefix);
    Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>> testData;
    testData = IndexTestUtils.generateTermData(null, 100);

    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> data : testData) {
      instance.setData(prefix, data.a, data.b, data.c, data.d);
    }
  }

  /**
   * Test of getData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetData_3args() throws Exception {
    LOG.info("Test getData 3args");
    String prefix = RandomValue.getString(1, 10);
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);
    instance.loadPrefix(prefix);
    Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>> testData;
    testData = IndexTestUtils.generateTermData(null, docId, key, 100);

    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> data : testData) {
      instance.setData(prefix, data.a, data.b, data.c, data.d);
    }

    Map<BytesWrap, Object> result = instance.getData(prefix, docId, key);
    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> data : testData) {
      assertEquals("Value not restored.", data.d, result.get(data.b));
    }
  }

  /**
   * Test of getData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetData_4args() throws Exception {
    LOG.info("Test getData 4args");
    String prefix = RandomValue.getString(1, 10);
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);
    instance.loadPrefix(prefix);
    Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>> testData;
    testData = IndexTestUtils.generateTermData(null, docId, key, 100);

    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> data : testData) {
      instance.setData(prefix, data.a, data.b, data.c, data.d);
    }

    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> data : testData) {
      assertEquals("Value not restored.", data.d, instance.getData(prefix,
              docId, data.b, key));
    }
  }

}
