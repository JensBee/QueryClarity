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

import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.Tuple;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.IndexNotFoundException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Setup a working environment.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class EnvironmentTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          EnvironmentTest.class);

  /**
   * Temporary Lucene memory index.
   */
  private static TestIndex index;

  /**
   * General rule to catch expected exceptions.
   */
  @Rule
  @java.lang.SuppressWarnings("PublicField")
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    // create the test index
    index = new TestIndex(TestIndex.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    Environment.clear();
  }

  /**
   * Create an instance using the 0 argument parameter.
   *
   * @return A new instance
   * @throws Exception Any exception thrown indicates an error
   */
  private Environment getInstance0Arg() throws Exception {
    final String tmpDataDir = tmpDir.newFolder().getPath();
    Environment instance = new Environment(index.getIndexDir(), tmpDataDir);
    instance.create();
    return instance;
  }

  /**
   * Create an instance using the 1 argument parameter.
   *
   * @return A new instance
   * @throws Exception Any exception thrown indicates an error
   */
  private Environment getInstance1Arg() throws Exception {
    final String tmpDataDir = tmpDir.newFolder().getPath();
    Environment instance = new Environment(index.getIndexDir(), tmpDataDir);
    instance.create(index);
    return instance;
  }

  /**
   * Test instantiation of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("UnusedAssignment")
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DLS_DEAD_LOCAL_STORE")
  public void testInstantiation() throws Exception {
    LOG.info("Test instantiation");
    Environment instance;
    final String tmpDataDir = tmpDir.newFolder().getPath();
    instance = new Environment(index.getIndexDir(), tmpDataDir);

    // illegal directory which contains no index data
    try {
      instance = new Environment(tmpDataDir + File.separatorChar
              + RandomValue.getString(5), tmpDataDir);
      instance.create();
      fail("Expected an exception to be thrown.");
    } catch (IndexNotFoundException ex) {
    }

    // test illegal parameter sets
    final Collection<Tuple.Tuple2<String, String>> testParams
            = new ArrayList<>(4);
    testParams.add(Tuple.tuple2("", tmpDataDir));
    testParams.add(Tuple.tuple2((String) null, tmpDataDir));
    testParams.add(Tuple.tuple2(index.getIndexDir(), ""));
    testParams.add(Tuple.tuple2(index.getIndexDir(), (String) null));
    for (Tuple.Tuple2<String, String> params : testParams) {
      try {
        instance = new Environment(params.a, params.b);
        fail("Expected an exception to be thrown.");
      } catch (IllegalArgumentException ex) {
      }
    }
  }

  /**
   * Test of create method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCreate_0args() throws Exception {
    LOG.info("Test create 0args");
    getInstance0Arg();
  }

  /**
   * Test of create method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCreate_IndexDataProvider() throws Exception {
    LOG.info("Test create 1arg");
    final Environment instance = getInstance1Arg();
  }

  /**
   * Test of initialized method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testInitialized() throws Exception {
    LOG.info("Test initialized");

    // should not be initialized
    assertFalse("Environment should not be initialized.", Environment.
            isInitialized());

    // initialize it
    getInstance1Arg();
    // should succeed
    assertTrue("Environment should be initialized.", Environment.
            isInitialized());

    // clear instance
    Environment.clear();
    // should fail now
    assertFalse("Environment should not be initialized.", Environment.
            isInitialized());

    // initialize again
    getInstance0Arg();
    // should succeed
    assertTrue("Environment should be initialized.", Environment.
            isInitialized());
  }

  /**
   * Test of getFields method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFields() throws Exception {
    LOG.info("Test getFields");
    @SuppressWarnings("MismatchedReadAndWriteOfArray")
    final Collection<String> expResult = index.test_getActiveFields();
    Collection<String> resultColl;

    // not initialized
    try {
      Environment.getFields();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    getInstance1Arg();
    resultColl = Arrays.asList(Environment.getFields());
    assertEquals("Index fields mismatch.", expResult.size(), resultColl.size());
    assertTrue("Index fields mismatch.", resultColl.containsAll(expResult));

    Environment.clear();

    // not initialized
    try {
      Environment.getFields();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    getInstance0Arg();
    resultColl = Arrays.asList(Environment.getFields());
    assertEquals("Index fields mismatch.", expResult.size(), resultColl.size());
    assertTrue("Index fields mismatch.", resultColl.containsAll(expResult));
  }

  /**
   * Test of getDataProvider method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDataProvider() throws Exception {
    LOG.info("Test getDataProvider");

    // not initialized
    try {
      Environment.getDataProvider();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    getInstance1Arg();
    Environment.getDataProvider();

    Environment.clear();

    // not initialized
    try {
      Environment.getDataProvider();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    getInstance0Arg();
    Environment.getDataProvider();
  }

  /**
   * Test of getIndexReader method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetIndexReader() throws Exception {
    LOG.info("Test getIndexReader");

    // not initialized
    try {
      Environment.getIndexReader();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    getInstance1Arg();
    Environment.getIndexReader();

    Environment.clear();

    // not initialized
    try {
      Environment.getIndexReader();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    getInstance0Arg();
    Environment.getIndexReader();
  }

  /**
   * Test of getIndexPath method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetIndexPath() throws Exception {
    LOG.info("Test getIndexPath");
    String result;

    // not initialized
    try {
      Environment.getIndexPath();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    getInstance1Arg();
    result = Environment.getIndexPath();
    assertEquals("Index path differs.", index.getIndexDir(), result);

    Environment.clear();

    // not initialized
    try {
      Environment.getIndexPath();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    getInstance0Arg();
    result = Environment.getIndexPath();
    assertEquals("Index path differs.", index.getIndexDir(), result);
  }

  /**
   * Test of getDataPath method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDataPath() throws Exception {
    LOG.info("Test getDataPath");
    String result;
    Environment instance;

    final String tmpDataDir = tmpDir.newFolder().getPath();

    // not initialized
    try {
      Environment.getDataPath();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    instance = new Environment(index.getIndexDir(), tmpDataDir);
    instance.create(index);
    result = Environment.getDataPath();
    assertEquals("Index path differs.", tmpDataDir, result);

    Environment.clear();

    // not initialized
    try {
      Environment.getDataPath();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    // initialize it
    instance = new Environment(index.getIndexDir(), tmpDataDir);
    instance.create();
    result = Environment.getDataPath();
    assertEquals("Index path differs.", tmpDataDir, result);
  }

  /**
   * Test of clear method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testClear() throws Exception {
    LOG.info("Test clear");

    // not initialized
    assertFalse("Environment should not be initialized.", Environment.
            isInitialized());

    // initialize it
    getInstance1Arg();
    assertTrue("Environment should be initialized.", Environment.
            isInitialized());

    Environment.clear();

    // not initialized
    assertFalse("Environment should not be initialized.", Environment.
            isInitialized());

    // initialize it
    getInstance0Arg();
    assertTrue("Environment should be initialized.", Environment.
            isInitialized());
  }

  /**
   * Test of saveProperties method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSaveProperties() throws Exception {
    LOG.info("Test saveProperties");
    getInstance0Arg();
    Environment.saveProperties();
  }

  /**
   * Test of setProperty method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetProperty() throws Exception {
    LOG.info("Test setProperty");

    // not initialized
    try {
      Environment.setProperty("test", RandomValue.getString(1, 10),
              RandomValue.
              getString(1, 10));
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    getInstance0Arg();

    final int propCount = RandomValue.getInteger(1, 100);
    for (int i = 0; i < propCount; i++) {
      Environment.setProperty("test", RandomValue.getString(1, 10),
              RandomValue.getString(1, 10));
    }
  }

  /**
   * Test of getProperty method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetProperty_String_String() throws Exception {
    LOG.info("Test getProperty 2args");
    final String prefix = "test";
    final int propCount = RandomValue.getInteger(1, 100);
    final Map<String, String> props = new HashMap<>(propCount);

    // not initialized
    try {
      Environment.getProperty("foo", "bar");
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    getInstance0Arg();

    for (int i = 0; i < propCount; i++) {
      props.put(RandomValue.getString(1, 10), RandomValue.getString(1, 10));
    }

    // add properties
    for (Map.Entry<String, String> propEntry : props.entrySet()) {
      Environment.setProperty(
              prefix, propEntry.getKey(), propEntry.getValue());
    }

    // read properties
    for (Map.Entry<String, String> propEntry : props.entrySet()) {
      assertEquals("Property not restored.", propEntry.getValue(),
              Environment.getProperty(prefix, propEntry.getKey()));
    }

    // test illegal parameter sets
    final Collection<Tuple.Tuple2<String, String>> testParams
            = new ArrayList<>(4);
    testParams.add(Tuple.tuple2("", (String) null));
    testParams.add(Tuple.tuple2((String) null, ""));
    testParams.add(Tuple.tuple2("foo", (String) null));
    testParams.add(Tuple.tuple2("foo", ""));
    testParams.add(Tuple.tuple2("", "foo"));
    testParams.add(Tuple.tuple2((String) null, (String) null));
    for (Tuple.Tuple2<String, String> params : testParams) {
      try {
        Environment.getProperty(params.a, params.b);
        fail("Expected an exception to be thrown.");
      } catch (IllegalArgumentException ex) {
      }
    }
  }

  /**
   * Test of clearProperties method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testClearProperties() throws Exception {
    LOG.info("Test clearProperties");
    final String prefix = "test";
    final int propCount = RandomValue.getInteger(1, 100);
    final Map<String, String> props = new HashMap<>(propCount);

    // not initialized
    try {
      Environment.clearProperties(prefix);
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    getInstance0Arg();

    for (int i = 0; i < propCount; i++) {
      props.put(RandomValue.getString(1, 10), RandomValue.getString(1, 10));
    }

    // add properties
    for (Map.Entry<String, String> propEntry : props.entrySet()) {
      Environment.setProperty(
              prefix, propEntry.getKey(), propEntry.getValue());
    }

    // read properties
    for (Map.Entry<String, String> propEntry : props.entrySet()) {
      assertEquals("Property not restored.", propEntry.getValue(),
              Environment.getProperty(prefix, propEntry.getKey()));
    }

    // this should not be deleted
    final String intPrefix = "solid";
    final String intKey = "i should";
    final String intVal = "survive";
    Environment.setProperty(intPrefix, intKey, intVal);

    Environment.clearProperties(prefix);

    // read properties
    for (String propKey : props.keySet()) {
      assertEquals("Property restored, but should be deleted.", null,
              Environment.getProperty(prefix, propKey));
    }

    assertEquals("Internal value not restored.", intVal, Environment.
            getProperty(intPrefix, intKey));
  }

  /**
   * Test of getProperty method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetProperty_3args() throws Exception {
    LOG.info("Test getProperty 3args");

    final String prefix = "test";
    final int propCount = RandomValue.getInteger(1, 100);
    final Map<String, Tuple.Tuple2<String, String>> props = new HashMap<>(
            propCount);

    // not initialized
    try {
      Environment.getProperty(prefix, "foo", "bar");
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    getInstance0Arg();

    for (int i = 0; i < propCount; i++) {
      props.put(RandomValue.getString(1, 10), Tuple.tuple2(RandomValue.
              getString(1, 10), RandomValue.getString(1, 10)));
    }

    // add properties
    for (Map.Entry<String, Tuple.Tuple2<String, String>> propEntry : props.
            entrySet()) {
      Environment.setProperty(prefix, propEntry.getKey(),
              propEntry.getValue().a);
    }

    // read properties
    for (Map.Entry<String, Tuple.Tuple2<String, String>> propEntry : props.
            entrySet()) {
      assertEquals("Property not restored.", propEntry.getValue().a,
              Environment.
              getProperty(prefix, propEntry.getKey(), propEntry.getValue().b));
      assertEquals("Property not restored (default).", propEntry.getValue().b,
              Environment.getProperty(prefix, propEntry.getKey() + "fuzz",
                      propEntry.getValue().b));
    }
  }

  /**
   * Test of clearAllProperties method, of class Environment.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testClearAllProperties() throws Exception {
    LOG.info("Test clearAllProperties");
    final String prefix = "test";
    final int propCount = RandomValue.getInteger(1, 100);
    final Map<String, String> props = new HashMap<>(propCount);

    // not initialized
    try {
      Environment.clearAllProperties();
      fail("Expected an exception to be thrown.");
    } catch (IllegalStateException ex) {
    }

    getInstance0Arg();

    for (int i = 0; i < propCount; i++) {
      props.put(RandomValue.getString(1, 10), RandomValue.getString(1, 10));
    }

    // add properties
    for (Map.Entry<String, String> propEntry : props.entrySet()) {
      Environment.setProperty(
              prefix, propEntry.getKey(), propEntry.getValue());
    }

    // read properties
    for (Map.Entry<String, String> propEntry : props.entrySet()) {
      assertEquals("Property not restored.", propEntry.getValue(),
              Environment.getProperty(prefix, propEntry.getKey()));
    }
  }
}
