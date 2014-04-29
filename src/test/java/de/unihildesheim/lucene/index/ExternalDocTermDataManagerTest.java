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
import de.unihildesheim.TestMethodInfo;
import de.unihildesheim.Tuple;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.Target;
import java.util.Collection;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

/**
 * Test for {@link ExternalDocTermDataManager}.
 *
 * @author Jens Bertram
 */
public final class ExternalDocTermDataManagerTest {

  /**
   * Log test methods.
   */
  @Rule
  public final TestMethodInfo watcher = new TestMethodInfo();

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
  public void testClear() throws Exception {
    instance.clear();

    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);

    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
    testData = IndexTestUtil.generateTermData(null, docId, key, 100);

    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      instance.setData(data.a, data.b, data.c, data.d);
    }

    instance.getData(docId, key);

    instance.clear();
    instance.getData(docId, key);
  }

  /**
   * Test of setData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetData() throws Exception {
    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
    testData = IndexTestUtil.generateTermData(null, 10000);

    new Processing(
            new Target.TargetFuncCall<>(
                    new CollectionSource<>(testData),
                    new TermDataTarget(instance)
            )).process(testData.size());

    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      instance.setData(data.a, data.b, data.c, data.d);
    }

    new Processing(
            new Target.TargetFuncCall<>(
                    new CollectionSource<>(testData),
                    new TermDataTarget(instance)
            )).process(testData.size());
  }

  /**
   * Test of getData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetData_int_String() throws Exception {
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);

    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
    testData = IndexTestUtil.generateTermData(null, docId, key, 100);

    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      instance.setData(data.a, data.b, data.c, data.d);
    }

    Map<ByteArray, Object> result = instance.getData(docId, key);
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
  public void testGetData_3args() throws Exception {
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);
    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
    testData = IndexTestUtil.generateTermData(null, docId, key, 100);

    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      instance.setData(data.a, data.b, data.c, data.d);
    }

    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> data : testData) {
      assertEquals("Value not restored.", data.d, instance.getData(docId,
              data.b, key));
    }
  }

  /**
   * Processing target to fill an {@link IndexDataProvider} instance with
   * test-termData.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class TermDataTarget
          extends Target.TargetFunc<
          Tuple.Tuple4<Integer, ByteArray, String, Integer>> {

    private final ExternalDocTermDataManager dtMan;

    /**
     * Initialize the target.
     *
     * @param dtm DocTerm data manager
     */
    public TermDataTarget(final ExternalDocTermDataManager dtm) {
      this.dtMan = dtm;
    }

    @Override
    public void call(
            Tuple.Tuple4<Integer, ByteArray, String, Integer> data) {
      if (data == null) {
        return;
      }
      dtMan.setData(data.a, data.b, data.c, data.d);
    }

  }
}
