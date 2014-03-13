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

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.util.RandomValue;
import java.util.Arrays;
import java.util.Collection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DirectIndexDataProvider}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DirectIndexDataProviderTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DirectIndexDataProviderTest.class);

  /**
   * DataProvider instance used during the test.
   */
  private DirectIndexDataProvider instance;

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
    index = new TestIndex(TestIndex.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());
  }

  /**
   * Run after each test has finished.
   */
  @After
  public void tearDown() {
    this.instance.dispose();
  }

  /**
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public void setUp() throws Exception {
    Environment.clear();
    index.setupEnvironment(DirectIndexDataProvider.class);
    this.instance = (DirectIndexDataProvider) Environment.getDataProvider();
  }

  /**
   * Test of getTermFrequency method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermFrequency_0args() throws Exception {
    IndexDataProviderTestMethods.testGetTermFrequency_0args(index,
            this.instance);
  }

  /**
   * Test of getTermFrequency method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermFrequency_BytesWrap() {
    IndexDataProviderTestMethods.testGetTermFrequency_BytesWrap(index,
            this.instance);
  }

  /**
   * Test of getRelativeTermFrequency method, of class
   * DirectIndexDataProvider.
   */
  @Test
  public void testGetRelativeTermFrequency() {
    IndexDataProviderTestMethods.testGetRelativeTermFrequency(index,
            this.instance);
  }

  /**
   * Test of dispose method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testDispose() throws Exception {
    LOG.info("Test dispose");
    this.instance.dispose();

    assertFalse("FieldsChangedListener should already be removed.",
            Environment.removeFieldsChangedListener(this.instance));
  }

  /**
   * Test of getTermsIterator method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermsIterator() {
    IndexDataProviderTestMethods.testGetTermsIterator(index, this.instance);
  }

  /**
   * Test of getTermsSource method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermsSource() {
    IndexDataProviderTestMethods.testGetTermsSource(index, this.instance);
  }

  /**
   * Test of getDocumentIdIterator method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetDocumentIdIterator() {
    IndexDataProviderTestMethods.testGetDocumentIdIterator(index,
            this.instance);
  }

  /**
   * Test of getDocumentIdSource method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetDocumentIdSource() {
    IndexDataProviderTestMethods.testGetDocumentIdSource(index, this.instance);
  }

  /**
   * Test of getUniqueTermsCount method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetUniqueTermsCount() {
    IndexDataProviderTestMethods.testGetUniqueTermsCount(index, this.instance);
  }

  /**
   * Test of setTermData method, of class DirectIndexDataProvider.
   */
  @Test
  public void testSetTermData() {
    IndexDataProviderTestMethods.testSetTermData(index, this.instance);
  }

  /**
   * Test of getTermData method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermData_4args() {
    IndexDataProviderTestMethods.testGetTermData_4args(index, this.instance);
  }

  /**
   * Test of getTermData method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetTermData_3args() {
    IndexDataProviderTestMethods.testGetTermData_3args(index, this.instance);
  }

  /**
   * Test of clearTermData method, of class DirectIndexDataProvider.
   */
  @Test
  public void testClearTermData() {
    IndexDataProviderTestMethods.testClearTermData(index, this.instance);
  }

  /**
   * Test of getDocumentModel method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetDocumentModel() {
    IndexDataProviderTestMethods.testGetDocumentModel(index, this.instance);
  }

  /**
   * Test of hasDocument method, of class DirectIndexDataProvider.
   */
  @Test
  public void testHasDocument() {
    IndexDataProviderTestMethods.testHasDocument(index, this.instance);
  }

  /**
   * Test of getDocumentsTermSet method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetDocumentsTermSet() {
    IndexDataProviderTestMethods.testGetDocumentsTermSet(index, this.instance);
  }

  /**
   * Test of getDocumentCount method, of class DirectIndexDataProvider.
   */
  @Test
  public void testGetDocumentCount() {
    IndexDataProviderTestMethods.testGetDocumentCount(index, this.instance);
  }

  /**
   * Test of documentContains method, of class DirectIndexDataProvider.
   */
  @Test
  public void testDocumentContains() {
    IndexDataProviderTestMethods.testDocumentContains(index, this.instance);
  }

  /**
   * Test of registerPrefix method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testRegisterPrefix() throws Exception {
    IndexDataProviderTestMethods.testRegisterPrefix(index, instance);
  }

  /**
   * Test of fieldsChanged method, of class DirectIndexDataProvider.
   */
  @Test
  public void testFieldsChanged() {
    LOG.info("Test fieldsChanged");

    // test with some fields enabled
    final int[] fieldState = index.getFieldState();
    int[] newFieldState = new int[fieldState.length];
    final Collection<String> oldFields = index.test_getActiveFields();

    if (fieldState.length > 1) {
      // toggle some fields
      newFieldState = fieldState.clone();
      // ensure both states are not the same
      while (Arrays.equals(newFieldState, fieldState)) {
        for (int i = 0; i < fieldState.length; i++) {
          newFieldState[i] = RandomValue.getInteger(0, 1);
        }
      }

      // pre-check equality
      assertEquals(index.getTermFrequency(), instance.getTermFrequency());
      final long oldTf = index.getTermFrequency();

      index.setFieldState(newFieldState);
      final Collection<String> newFields = index.test_getActiveFields();
      instance.fieldsChanged(oldFields.toArray(new String[oldFields.size()]),
              newFields.toArray(new String[newFields.size()]));

      Environment.setFields(newFields.toArray(new String[newFields.size()]));

      assertEquals("Wron overall term frequency count, after field change.",
              index.getTermFrequency(), instance.getTermFrequency());
      assertNotEquals(
              "Term frequency value not changed after changing fields.", oldTf,
              instance.getTermFrequency());
    } else {
      LOG.warn("Skip test section. Field count == 1.");
    }
  }
}
