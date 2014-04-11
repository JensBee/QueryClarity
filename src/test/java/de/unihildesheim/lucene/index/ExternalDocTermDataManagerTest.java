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

import de.unihildesheim.ByteArray;
import de.unihildesheim.Tuple;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.ProcessingException;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.util.Collection;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link ExternalDocTermDataManager}.
 *
 * @author Jens Bertram
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

  /**
   * Run before each test starts.
   */
  @Before
  public void setUp() {
    DBMaker dbMkr = DBMaker.newTempFileDB();
    this.db = dbMkr.make();
    this.instance = getInstance();
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
  @Ignore
  public void testClear() throws Exception {
    LOG.info("Test clear");
    instance.clear();

    String prefix = RandomValue.getString(1, 10);
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);
    instance.loadPrefix(prefix);
    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
    testData = IndexTestUtil.generateTermData(null, docId, key, 100);

    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
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
  @Ignore
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

    DBMaker dbMkr = DBMaker.newTempFileDB();
    this.db = dbMkr.transactionDisable()
            .asyncWriteEnable()
            .checksumEnable()
            .asyncWriteFlushDelay(100)
            .mmapFileEnableIfSupported()
            .closeOnJvmShutdown()
            .make();
    final String gPrefix = RandomValue.getString(1, 10);
    instance = new ExternalDocTermDataManager(db, gPrefix);
    final String prefix = RandomValue.getString(1, 10);
    instance.loadPrefix(prefix);

    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
    testData = IndexTestUtil.generateTermData(null, 1000000);

    Processing p;
    LOG.debug("Concurrent put..");
    p = new Processing(new TermDataTarget(
            new CollectionSource<>(testData), instance, prefix));
    p.process();

    LOG.debug("Single update..");
    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      instance.setData(prefix, data.a, data.b, data.c, data.d);
    }

    LOG.debug("Concurrent update..");
    p = new Processing(new TermDataTarget(
            new CollectionSource<>(testData), instance, prefix));
    p.process();
  }

  /**
   * Test of getData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  public void testGetData_3args() throws Exception {
    LOG.info("Test getData 3args");
    String prefix = RandomValue.getString(1, 10);
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);
    instance.loadPrefix(prefix);
    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
    testData = IndexTestUtil.generateTermData(null, docId, key, 100);

    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      instance.setData(prefix, data.a, data.b, data.c, data.d);
    }

    Map<ByteArray, Object> result = instance.getData(prefix, docId, key);
    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      assertEquals("Value not restored.", data.d, result.get(data.b));
    }
  }

  /**
   * Test of getData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  public void testGetData_4args() throws Exception {
    LOG.info("Test getData 4args");
    String prefix = RandomValue.getString(1, 10);
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);
    instance.loadPrefix(prefix);
    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
    testData = IndexTestUtil.generateTermData(null, docId, key, 100);

    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      instance.setData(prefix, data.a, data.b, data.c, data.d);
    }

    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      assertEquals("Value not restored.", data.d, instance.getData(prefix,
              docId, data.b, key));
    }
  }

  /**
   * Processing target to fill an {@link IndexDataProvider} instance with
   * test-termData.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class TermDataTarget extends Target<Tuple.Tuple4<
        Integer, ByteArray, String, Integer>> {

    /**
     * Prefix to use for storing values.
     */
    private static String prefix;
    private static ExternalDocTermDataManager dtMan;

    /**
     * Initialize the target.
     *
     * @param newSource Source to use
     * @param newDataTarget Target {@link IndexDataProvider}
     * @param newPrefix Prefix to use for adding data
     */
    public TermDataTarget(
            final Source<Tuple.Tuple4<
                    Integer, ByteArray, String, Integer>> newSource,
            final ExternalDocTermDataManager dtm,
            final String newPrefix) {
      super(newSource);
      prefix = newPrefix;
      dtMan = dtm;
    }

    /**
     * Factory instance creator
     *
     * @param newSource Source to use
     */
    private TermDataTarget(
            final Source<Tuple.Tuple4<
                    Integer, ByteArray, String, Integer>> newSource) {
      super(newSource);
    }

    @Override
    public Target<Tuple.Tuple4<
        Integer, ByteArray, String, Integer>> newInstance() {
      return new TermDataTarget(this.getSource());
    }

    @Override
    public void runProcess() throws Exception {
      while (!isTerminating()) {
        Tuple.Tuple4<Integer, ByteArray, String, Integer> t4;
        try {
          t4 = getSource().next();
        } catch (ProcessingException.SourceHasFinishedException ex) {
          break;
        }
        if (t4 != null) {
          try {
            if (dtMan.setData(prefix, t4.a, t4.b, t4.c, t4.d) != null) {
              LOG.warn("A termData value was already set.");
            }
          } catch (Exception ex) {
            LOG.error("EX p={} a={} b={} c={} d={}", prefix, t4.a, t4.b, t4.c,
                    t4.d);
          }
        }
      }
    }

  }
}
