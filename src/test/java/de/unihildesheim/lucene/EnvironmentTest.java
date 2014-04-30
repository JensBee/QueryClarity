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
package de.unihildesheim.lucene;

import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.IndexTestUtil;
import de.unihildesheim.util.RandomValue;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Test for {@link Environment}.
 *
 * @author Jens Bertram
 */
public class EnvironmentTest extends MultiIndexDataProviderTestCase {

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public EnvironmentTest(
          final Class<? extends IndexDataProvider> dataProv,
          final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Test of getIndexGeneration method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetIndexGeneration() throws Exception {
    final Long gen = Environment.getIndexGeneration();
    assertNotNull(msg("Generation was null."), gen);
    assertTrue(msg("Generation < 0."), gen >= 0);
  }

  /**
   * Test of isTestRun method, of class Environment.
   */
  @Test
  public void testIsTestRun() {
    final boolean expResult = true;
    final boolean result = Environment.isTestRun();
    assertEquals(msg("Test run should be true."), expResult, result);
  }

  /**
   * Test of getStopwords method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetStopwords() throws Exception {
    final Collection<String> expResult = IndexTestUtil.getRandomStopWords(
            index);
    IndexTestUtil.createInstance(index, getDataProviderClass(), null,
            expResult);

    final Collection<String> result = Environment.getStopwords();
    assertEquals(msg("Stopword count mismatch."), expResult.size(), result.
            size());
    assertTrue(msg("Stopword list content mismatch."), result.
            containsAll(expResult));
  }

  /**
   * Test of isInitialized method, of class Environment.
   */
  @Test
  public void testIsInitialized() {
    assertEquals(msg("Environment shoud be initialized."), true, Environment.
            isInitialized());

    Environment.clear();
    assertEquals(msg("Environment shoud not be initialized after clear()."),
            false, Environment.isInitialized());
  }

  /**
   * Test of getFields method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetFields() throws Exception {
    final Collection<String> expResult = IndexTestUtil.getRandomFields(index);
    IndexTestUtil.createInstance(index, getDataProviderClass(), expResult,
            null);

    final Collection<String> result = Arrays.asList(Environment.getFields());
    assertEquals(msg("Fields count mismatch."), expResult.size(), result.
            size());
    assertTrue(msg("Fields list content mismatch."), result.containsAll(
            expResult));
  }

  /**
   * Test of getDataProvider method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetDataProvider() {
    assertEquals(msg("DataProvider class differs."), Environment.
            getDataProvider().getClass(), getDataProviderClass());
  }

  /**
   * Test of getIndexReader method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetIndexReader() throws Exception {
    assertNotNull(msg("IndexReader was null."), Environment.getIndexReader());
  }

  /**
   * Test of getIndexPath method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetIndexPath() throws Exception {
    final String result = Environment.getIndexPath();

    assertFalse(msg("IndexPath was empty."), result.isEmpty());
    final File f = new File(result);
    assertTrue(msg("IndexPath does not exist."), f.exists());
  }

  /**
   * Test of getDataPath method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetDataPath() throws Exception {
    final String result = Environment.getDataPath();

    assertFalse(msg("DataPath was empty."), result.isEmpty());
    final File f = new File(result);
    assertTrue(msg("DataPath does not exist."), f.exists());
  }

  /**
   * Test functions that should fail, if the {@link Environment} is not
   * initialized.
   *
   * @throws Exception Any exception indicates an error
   */
  private void testUninitialized() throws Exception {
    try {
      Environment.getDataPath();
      fail(msg("Expected an Exception."));
    } catch (IllegalStateException ex) {
    }

    try {
      Environment.getDataProvider();
      fail(msg("Expected an Exception."));
    } catch (IllegalStateException ex) {
    }

    try {
      Environment.getFields();
      fail(msg("Expected an Exception."));
    } catch (IllegalStateException ex) {
    }

    try {
      Environment.getIndexGeneration();
      fail(msg("Expected an Exception."));
    } catch (IllegalStateException ex) {
    }

    try {
      Environment.getIndexPath();
      fail(msg("Expected an Exception."));
    } catch (IllegalStateException ex) {
    }

    try {
      Environment.getProperties(RandomValue.getString(10));
      fail(msg("Expected an Exception."));
    } catch (IllegalStateException ex) {
    }

    try {
      Environment.getProperty(RandomValue.getString(10), RandomValue.
              getString(10));
      fail(msg("Expected an Exception."));
    } catch (IllegalStateException ex) {
    }

    try {
      Environment.getProperty(RandomValue.getString(10), RandomValue.
              getString(10), RandomValue.getString(10));
      fail(msg("Expected an Exception."));
    } catch (IllegalStateException ex) {
    }

    assertTrue(msg("Expected empty stopwords."), Environment.getStopwords().
            isEmpty());
  }

  /**
   * Test of clear method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testClear() throws Exception {
    assertTrue(msg("Environment not initialized."), Environment.
            isInitialized());
    Environment.clear();
    testUninitialized();
  }

  /**
   * Test of saveProperties method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testSaveProperties() throws Exception {
    Environment.saveProperties();
  }

  /**
   * Test of setProperty method, of class Environment.
   */
  @Test
  public void testSetProperty() {
    final String prefix = RandomValue.getString(1, 100);
    final String key = RandomValue.getString(1, 100);
    final String value = RandomValue.getString(1, 100);
    Environment.setProperty(prefix, key, value);
  }

  /**
   * Test of removeProperty method, of class Environment.
   */
  @Test
  public void testRemoveProperty() {
    final String prefix = RandomValue.getString(1, 100);
    final String key = RandomValue.getString(1, 100);
    final String value = RandomValue.getString(1, 100);

    Environment.setProperty(prefix, key, value);
    final String result = (String) Environment.removeProperty(prefix, key);
    assertEquals(msg("Removed property value differs."), value, result);
  }

  /**
   * Test of getProperty method, of class Environment.
   */
  @Test
  public void testGetProperty_String_String() {
    final String prefix = RandomValue.getString(1, 100);
    final String key = RandomValue.getString(1, 100);
    final String value = RandomValue.getString(1, 100);
    Environment.setProperty(prefix, key, value);
    final String result = Environment.getProperty(prefix, key);
    assertEquals(msg("Retrieved property value differs."), value, result);
  }

  /**
   * Compare two data maps.
   *
   * @param expResult Map with expected data
   * @param result Map with result data
   */
  private void compareDataMap(final Map<String, String> expResult,
          final Map<String, ?> result) {
    assertEquals(msg("Value map size differs."), expResult.size(), result.
            size());
    assertTrue(msg("Value map key list differs."), result.keySet().
            containsAll(
                    expResult.keySet()));

    for (Entry<String, String> entry : expResult.entrySet()) {
      final String key = entry.getKey();
      assertEquals(msg("Value differs. key=" + key), result.get(key), result.
              get(
                      key));
    }
  }

  /**
   * Test of getProperties method, of class Environment.
   */
  @Test
  public void testGetProperties() {
    final String prefix = RandomValue.getString(1, 100);
    final int amount = RandomValue.getInteger(10, 1000);
    final Map<String, String> expResult = new HashMap<>(amount);

    for (int i = 0; i < amount;) {
      if (expResult.put(RandomValue.getString(1, 100), RandomValue.
              getString(1,
                      100)) == null) {
        i++;
      }
    }

    for (Entry<String, String> entry : expResult.entrySet()) {
      Environment.setProperty(prefix, entry.getKey(), entry.getValue());
    }

    final Map<String, Object> result = Environment.getProperties(prefix);
    compareDataMap(expResult, result);
  }

  /**
   * Test of clearProperties method, of class Environment.
   */
  @Test
  public void testClearProperties() {
    final String firstPrefix = RandomValue.getString(1, 100);
    String secondPrefix = RandomValue.getString(1, 100);
    final int amount = RandomValue.getInteger(10, 1000);
    final Map<String, String> firstExpResult = new HashMap<>(amount);
    final Map<String, String> secondExpResult = new HashMap<>(amount);

    // ensure unique names
    while (firstPrefix.equals(secondPrefix)) {
      secondPrefix = RandomValue.getString(1, 100);
    }

    // fill first map
    for (int i = 0; i < amount;) {
      if (firstExpResult.put(RandomValue.getString(1, 100), RandomValue.
              getString(1, 100)) == null) {
        i++;
      }
    }
    for (Entry<String, String> entry : firstExpResult.entrySet()) {
      Environment.setProperty(firstPrefix, entry.getKey(), entry.getValue());
    }

    // fill second map
    for (int i = 0; i < amount;) {
      if (secondExpResult.put(RandomValue.getString(1, 100), RandomValue.
              getString(1, 100)) == null) {
        i++;
      }
    }
    for (Entry<String, String> entry : secondExpResult.entrySet()) {
      Environment.setProperty(secondPrefix, entry.getKey(), entry.getValue());
    }

    // check first data
    compareDataMap(firstExpResult, Environment.getProperties(firstPrefix));

    // check second data
    compareDataMap(secondExpResult, Environment.getProperties(secondPrefix));

    Environment.clearProperties(firstPrefix);

    // check first data (deleted)
    assertTrue(msg("Data map should be empty."), Environment.getProperties(
            firstPrefix).isEmpty());

    // check second data (should be still there)
    compareDataMap(secondExpResult, Environment.getProperties(secondPrefix));
  }

  /**
   * Test of clearAllProperties method, of class Environment.
   */
  @Test
  public void testClearAllProperties() {
    final String firstPrefix = RandomValue.getString(1, 100);
    String secondPrefix = RandomValue.getString(1, 100);
    final int amount = RandomValue.getInteger(10, 1000);
    final Map<String, String> firstExpResult = new HashMap<>(amount);
    final Map<String, String> secondExpResult = new HashMap<>(amount);

    // ensure unique names
    while (firstPrefix.equals(secondPrefix)) {
      secondPrefix = RandomValue.getString(1, 100);
    }

    // fill first map
    for (int i = 0; i < amount;) {
      if (firstExpResult.put(RandomValue.getString(1, 100), RandomValue.
              getString(1, 100)) == null) {
        i++;
      }
    }
    for (Entry<String, String> entry : firstExpResult.entrySet()) {
      Environment.setProperty(firstPrefix, entry.getKey(), entry.getValue());
    }

    // fill second map
    for (int i = 0; i < amount;) {
      if (secondExpResult.put(RandomValue.getString(1, 100), RandomValue.
              getString(1, 100)) == null) {
        i++;
      }
    }
    for (Entry<String, String> entry : secondExpResult.entrySet()) {
      Environment.setProperty(secondPrefix, entry.getKey(), entry.getValue());
    }

    // check first data
    compareDataMap(firstExpResult, Environment.getProperties(firstPrefix));

    // check second data
    compareDataMap(secondExpResult, Environment.getProperties(secondPrefix));

    Environment.clearAllProperties();

    // check first data (deleted)
    assertTrue(msg("Data map should be empty."), Environment.getProperties(
            firstPrefix).isEmpty());

    // check second data (deleted)
    assertTrue(msg("Data map should be empty."), Environment.getProperties(
            secondPrefix).isEmpty());
  }

  /**
   * Test of getProperty method, of class Environment.
   */
  @Test
  public void testGetProperty_3args() {
    final String firstPrefix = RandomValue.getString(1, 100);
    String secondPrefix = RandomValue.getString(1, 100);
    final int amount = RandomValue.getInteger(10, 1000);
    final Collection<String> firstKeys = new HashSet<>(amount);
    final Collection<String> secondKeys = new HashSet<>(amount);
    final Map<String, String> firstExpResult = new HashMap<>(amount);
    final Map<String, String> secondExpResult = new HashMap<>(amount);

    // ensure unique names
    while (firstPrefix.equals(secondPrefix)) {
      secondPrefix = RandomValue.getString(1, 100);
    }

    // fill first map
    for (int i = 0; i < amount;) {
      final String key = RandomValue.getString(1, 100);
      if (RandomValue.getBoolean()) {
        firstExpResult.put(key, RandomValue.getString(1, 100));
      }
      if (firstKeys.add(key)) {
        i++;
      }
    }
    for (Entry<String, String> entry : firstExpResult.entrySet()) {
      Environment.setProperty(firstPrefix, entry.getKey(), entry.getValue());
    }

    // fill second map
    for (int i = 0; i < amount;) {
      final String key = RandomValue.getString(1, 100);
      if (RandomValue.getBoolean()) {
        secondExpResult.put(key, RandomValue.getString(1, 100));
      }
      if (secondKeys.add(key)) {
        i++;
      }
    }
    for (Entry<String, String> entry : secondExpResult.entrySet()) {
      Environment.setProperty(secondPrefix, entry.getKey(), entry.getValue());
    }

    // check first data
    compareDataMap(firstExpResult, Environment.getProperties(firstPrefix));

    // check second data
    compareDataMap(secondExpResult, Environment.getProperties(secondPrefix));

    // check first map entries
    final Map<String, Object> firstResult = Environment.getProperties(
            firstPrefix);
    for (String key : firstKeys) {
      final String value = (String) firstResult.get(key);
      final String dValue = RandomValue.getString(1, 100);
      if (value == null) {
        assertEquals(msg("Expected default value."), dValue, Environment.
                getProperty(firstPrefix, key, dValue));
      } else {
        assertEquals(msg("Expected defined value."), value, Environment.
                getProperty(firstPrefix, key, dValue));
      }
    }

    // check second map entries
    final Map<String, Object> secondResult = Environment.getProperties(
            secondPrefix);
    for (String key : secondKeys) {
      final String value = (String) secondResult.get(key);
      final String dValue = RandomValue.getString(1, 100);
      if (value == null) {
        assertEquals(msg("Expected default value."), dValue, Environment.
                getProperty(secondPrefix, key, dValue));
      } else {
        assertEquals(msg("Expected defined value."), value, Environment.
                getProperty(secondPrefix, key, dValue));
      }
    }
  }

  /**
   * Test of shutdown method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testShutdown() throws Exception {
    Environment.shutdown();
    testUninitialized();
  }
}
