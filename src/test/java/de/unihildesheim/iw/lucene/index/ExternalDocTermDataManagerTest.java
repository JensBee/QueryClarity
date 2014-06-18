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
package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.junit.Assert;
import org.junit.Test;
import org.mapdb.DBMaker;

import java.util.Collection;
import java.util.Map;

/**
 * Test for {@link ExternalDocTermDataManager}.
 *
 * @author Jens Bertram
 */
public final class ExternalDocTermDataManagerTest
    extends TestCase {

  /**
   * Test of clear method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testClear()
      throws Exception {
    ExternalDocTermDataManager instance = null;
    try {
      instance = getInstance();
      instance.clear();

      final String key = RandomValue.getString(1, 10);
      final Integer docId = RandomValue.getInteger(0, 100);

      final Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>>
          testData;
      testData = IndexTestUtils.generateTermData(null, docId, key, 100);

      for (final Tuple.Tuple4<Integer, ByteArray, String,
          Integer> data : testData) {
        instance.setData(data.a, data.b, data.c, data.d);
      }

      instance.getData(docId, key);

      instance.clear();
      instance.getData(docId, key);
    } finally {
      if (instance != null) {
        instance.getDb().close();
      }
    }
  }

  /**
   * Get an instance ready for running tests.
   *
   * @return initialized instance
   */
  private static ExternalDocTermDataManager getInstance() {
    final String prefix = RandomValue.getString(1, 10);
    return new ExternalDocTermDataManager(
        DBMaker.newTempFileDB()
            .deleteFilesAfterClose()
            .closeOnJvmShutdown()
            .transactionDisable()
            .make(),
        prefix);
  }

  /**
   * Test of setData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetData()
      throws Exception {
    final Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>>
        testData;
    testData = IndexTestUtils.generateTermData(null, 10000);

    ExternalDocTermDataManager instance = null;
    try {
      instance = getInstance();
      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(testData),
              new TermDataTarget(instance)
          )
      ).process(testData.size());

      for (final Tuple.Tuple4<Integer, ByteArray, String,
          Integer> data : testData) {
        instance.setData(data.a, data.b, data.c, data.d);
      }

      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(testData),
              new TermDataTarget(instance)
          )
      ).process(testData.size());
    } finally {
      if (instance != null) {
        instance.getDb().close();
      }
    }
  }

  /**
   * Test of getData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetData_int_String()
      throws Exception {
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);

    final Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>>
        testData;
    testData = IndexTestUtils.generateTermData(null, docId, key, 100);

    ExternalDocTermDataManager instance = null;
    try {
      instance = getInstance();
      for (final Tuple.Tuple4<Integer, ByteArray, String,
          Integer> data : testData) {
        instance.setData(data.a, data.b, data.c, data.d);
      }

      final Map<ByteArray, Object> result = instance.getData(docId, key);
      for (final Tuple.Tuple4<Integer, ByteArray, String,
          Integer> data : testData) {
        Assert.assertEquals("Value not restored.", data.d, result.get(data.b));
      }
    } finally {
      if (instance != null) {
        instance.getDb().close();
      }
    }
  }

  /**
   * Test of getData method, of class ExternalDocTermDataManager.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetData_3args()
      throws Exception {
    final String key = RandomValue.getString(1, 10);
    final Integer docId = RandomValue.getInteger(0, 100);
    final Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>>
        testData;
    testData = IndexTestUtils.generateTermData(null, docId, key, 100);

    ExternalDocTermDataManager instance = null;
    try {
      instance = getInstance();
      for (final Tuple.Tuple4<Integer, ByteArray, String,
          Integer> data : testData) {
        instance.setData(data.a, data.b, data.c, data.d);
      }

      for (final Tuple.Tuple4<Integer, ByteArray, String,
          Integer> data : testData) {
        Assert.assertEquals("Value not restored.", data.d,
            instance.getData(docId, data.b, key));
      }
    } finally {
      if (instance != null) {
        instance.getDb().close();
      }
    }
  }

  /**
   * Processing target to fill an {@link IndexDataProvider} instance with
   * test-termData.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class TermDataTarget
      extends TargetFuncCall.TargetFunc<
      Tuple.Tuple4<Integer, ByteArray, String, Integer>> {

    /**
     * DataManager reference.
     */
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
    public void call(final Tuple.Tuple4<Integer, ByteArray, String,
        Integer> data) {
      if (data == null) {
        return;
      }
      this.dtMan.setData(data.a, data.b, data.c, data.d);
    }

  }
}
