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

import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.concurrent.processing.Source;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DirectIndexDataProvider}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DirectIndexDataProviderTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DirectIndexDataProviderTest.class);

  /**
   * DataProvider instance used during the test.
   */
  private static DirectIndexDataProvider instance = null;

  /**
   * Temporary Lucene memory index.
   */
  private static TestIndex index;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    // create the test index
    index = new TestIndex();
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());
    instance = new DirectIndexDataProvider("test", index.getReader(), index.getFields(), true);
  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static void tearDownClass() {
    instance.dispose();
  }

  /**
   * Test of getTermFrequency method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermFrequency_0args() throws Exception {
    IndexDataProviderTest.testGetTermFrequency_0args(index, instance);
  }

  /**
   * Test of getTermFrequency method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermFrequency_BytesWrap() {
    IndexDataProviderTest.testGetTermFrequency_BytesWrap(index, instance);
  }

  /**
   * Test of getRelativeTermFrequency method, of class
   * DirectIndexDataProvider.
   */
  @Test
  public void testGetRelativeTermFrequency() {
    IndexDataProviderTest.testGetRelativeTermFrequency(index, instance);
  }

  /**
   * Test of dispose method, of class DirectIndexDataProvider.
   */
  @Test
  @Ignore
  public void testDispose() {
    System.out.println("dispose");
    DirectIndexDataProvider instance = null;
    instance.dispose();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getTargetFields method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTargetFields() {
    LOG.info("Test getTargetFields");
    Collection<String> expResult = Arrays.asList(index.getFields());
    Collection<String> result = Arrays.asList(instance.getFields());

    assertEquals("Different number of fields reported.", expResult.size(),
            result.size());
    assertTrue("Reported fields differ.", result.containsAll(expResult));
  }

  /**
   * Test of getTermsIterator method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermsIterator() {
    IndexDataProviderTest.testGetTermsIterator(index, instance);
  }

  /**
   * Test of getTermsSource method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermsSource() {
    IndexDataProviderTest.testGetTermsSource(index, instance);
  }

  /**
   * Test of getDocumentIdIterator method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetDocumentIdIterator() {
    IndexDataProviderTest.testGetDocumentIdIterator(index, instance);
  }

  /**
   * Test of getDocumentIdSource method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetDocumentIdSource() {
    IndexDataProviderTest.testGetDocumentIdSource(index, instance);
  }

  /**
   * Test of getUniqueTermsCount method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetUniqueTermsCount() {
    IndexDataProviderTest.testGetUniqueTermsCount(index, instance);
  }

  /**
   * Test of setTermData method, of class DirectIndexDataProvider.
   */
  @Test
  public void testSetTermData() {
    IndexDataProviderTest.testSetTermData(index, instance);
  }

  /**
   * Test of getTermData method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermData_4args() {
    IndexDataProviderTest.testGetTermData_4args(index, instance);
  }

  /**
   * Test of getTermData method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermData_3args() {
    IndexDataProviderTest.testGetTermData_3args(index, instance);
  }

  /**
   * Test of clearTermData method, of class DirectIndexDataProvider.
   */
  @Test
  @Ignore
  public void testClearTermData() {
    System.out.println("clearTermData");
    DirectIndexDataProvider instance = null;
    instance.clearTermData();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getDocumentModel method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetDocumentModel() {
    IndexDataProviderTest.testGetDocumentModel(index, instance);
  }

  /**
   * Test of addDocument method, of class DirectIndexDataProvider.
   */
  @Test
  @Ignore
  public void testAddDocument() {
    System.out.println("addDocument");
    DocumentModel docModel = null;
    DirectIndexDataProvider instance = null;
    boolean expResult = false;
    boolean result = instance.addDocument(docModel);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of hasDocument method, of class DirectIndexDataProvider.
   */
  @Test
  public void testHasDocument() {
    IndexDataProviderTest.testHasDocument(index, instance);
  }

  /**
   * Test of getDocumentsTermSet method, of class DirectIndexDataProvider.
   */
  @Test
  @Ignore
  public void testGetDocumentsTermSet() {
    System.out.println("getDocumentsTermSet");
    Collection<Integer> docIds = null;
    DirectIndexDataProvider instance = null;
    Collection<BytesWrap> expResult = null;
    Collection<BytesWrap> result = instance.getDocumentsTermSet(docIds);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getDocumentCount method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetDocumentCount() {
    IndexDataProviderTest.testGetDocumentCount(index, instance);
  }

  /**
   * Test of setProperty method, of class DirectIndexDataProvider.
   */
  @Test
  @Ignore
  public void testSetProperty() {
    System.out.println("setProperty");
    String prefix = "";
    String key = "";
    String value = "";
    DirectIndexDataProvider instance = null;
    instance.setProperty(prefix, key, value);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getProperty method, of class DirectIndexDataProvider.
   */
  @Test
  @Ignore
  public void testGetProperty_String_String() {
    System.out.println("getProperty");
    String prefix = "";
    String key = "";
    DirectIndexDataProvider instance = null;
    String expResult = "";
    String result = instance.getProperty(prefix, key);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of clearProperties method, of class DirectIndexDataProvider.
   */
  @Test
  @Ignore
  public void testClearProperties() {
    System.out.println("clearProperties");
    DirectIndexDataProvider instance = null;
    instance.clearProperties();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getProperty method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetProperty_3args() {
    IndexDataProviderTest.testGetProperty_3args(index, instance);
  }

  /**
   * Test of documentContains method, of class DirectIndexDataProvider.
   */
  @Test
  @Ignore
  public void testDocumentContains() {
    IndexDataProviderTest.testDocumentContains(index, instance);
  }

}
