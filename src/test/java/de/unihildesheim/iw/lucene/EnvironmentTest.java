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
package de.unihildesheim.iw.lucene;

import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexTestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

/**
 * Test for {@link Environment}.
 *
 * @author Jens Bertram
 */
@RunWith(Parameterized.class)
public final class EnvironmentTest
    extends MultiIndexDataProviderTestCase {

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
  public void testGetIndexGeneration()
      throws Exception {
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
  public void testGetStopwords()
      throws Exception {
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
    assertTrue(msg("Environment shoud be initialized."), Environment.
        isInitialized());

    Environment.clear();
    assertFalse(msg("Environment shoud not be initialized after clear()."),
        Environment.isInitialized());
  }

  /**
   * Test of getFields method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetFields()
      throws Exception {
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
  public void testGetIndexReader()
      throws Exception {
    assertNotNull(msg("IndexReader was null."), Environment.getIndexReader());
  }

  /**
   * Test of getIndexPath method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetIndexPath()
      throws Exception {
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
  public void testGetDataPath()
      throws Exception {
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
  @SuppressWarnings({"checkstyle:emptyblock", "checkstyle:magicnumber"})
  private void testUninitialized()
      throws Exception {
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

    assertTrue(msg("Expected empty stopwords."), Environment.getStopwords().
        isEmpty());
  }

  /**
   * Test of clear method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testClear()
      throws Exception {
    assertTrue(msg("Environment not initialized."), Environment.
        isInitialized());
    Environment.clear();
    testUninitialized();
  }

  /**
   * Test of shutdown method, of class Environment.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testShutdown()
      throws Exception {
    Environment.shutdown();
    testUninitialized();
  }
}
