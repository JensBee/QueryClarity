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
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DirectIndexDataProvider}.
 *
 *
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
  private static TestIndexDataProvider index;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    // create the test index
    index = new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
            isInitialized());
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
    this.instance.warmUp();
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
   * Test of getTermFrequency method, of class DirectIndexDataProvider. Using
   * stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermFrequency_0args__stopped() throws Exception {
    IndexDataProviderTestMethods.testGetTermFrequency_0args__stopped(index,
            this.instance);
  }

  /**
   * Test of getTermFrequency method, of class DirectIndexDataProvider. Using
   * random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermFrequency_0args__randFields() throws Exception {
    IndexDataProviderTestMethods.testGetTermFrequency_0args__randFields(index,
            this.instance);
  }

  /**
   * Test of getTermFrequency method, of class DirectIndexDataProvider.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermFrequency_ByteArray() throws Exception {
    IndexDataProviderTestMethods.testGetTermFrequency_ByteArray(index,
            this.instance);
  }

  /**
   * Test of getTermFrequency method, of class DirectIndexDataProvider. Using
   * stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermFrequency_ByteArray__stopped() throws Exception {
    IndexDataProviderTestMethods.
            testGetTermFrequency_ByteArray__stopped(index,
                    this.instance);
  }

  /**
   * Test of getRelativeTermFrequency method, of class
   * DirectIndexDataProvider.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetRelativeTermFrequency() throws Exception {
    IndexDataProviderTestMethods.testGetRelativeTermFrequency(index,
            this.instance);
  }

  /**
   * Test of getRelativeTermFrequency method, of class
   * DirectIndexDataProvider. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetRelativeTermFrequency__stopped() throws Exception {
    IndexDataProviderTestMethods.testGetRelativeTermFrequency__stopped(index,
            this.instance);
  }

  /**
   * Test of getRelativeTermFrequency method, of class
   * DirectIndexDataProvider. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetRelativeTermFrequency__randFields() throws Exception {
    IndexDataProviderTestMethods.testGetRelativeTermFrequency__randFields(
            index,
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

//    assertFalse("EventListener should already be removed.",
//            Environment.removeEventListener(this.instance));
  }

  /**
   * Test of getTermsIterator method, of class DirectIndexDataProvider.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermsIterator() throws Exception {
    IndexDataProviderTestMethods.testGetTermsIterator(index, this.instance);
  }

  /**
   * Test of getTermsIterator method, of class DirectIndexDataProvider. Using
   * stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermsIterator__stopped() throws Exception {
    IndexDataProviderTestMethods.testGetTermsIterator__stopped(index,
            this.instance);
  }

  /**
   * Test of getTermsIterator method, of class DirectIndexDataProvider. Using
   * random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermsIterator__randField() throws Exception {
    IndexDataProviderTestMethods.testGetTermsIterator__randField(index,
            this.instance);
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
   * Test of getUniqueTermsCount method, of class DirectIndexDataProvider.
   * Using stopwords.
   */
  @Test
  public void testGetUniqueTermsCount__stopped() {
    IndexDataProviderTestMethods.testGetUniqueTermsCount__stopped(index,
            this.instance);
  }

  /**
   * Test of getUniqueTermsCount method, of class DirectIndexDataProvider.
   * Using random fields.
   */
  @Test
  public void testGetUniqueTermsCount__randFields() {
    IndexDataProviderTestMethods.testGetUniqueTermsCount__randFields(index,
            this.instance);
  }

  /**
   * Test of setTermData method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception indicating a failure
   */
  @Test
  public void testSetTermData() throws Exception {
    IndexDataProviderTestMethods.testSetTermData(index, this.instance);
  }

  /**
   * Test of getTermData method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception indicating a failure
   */
  @Test
  public void testGetTermData_4args() throws Exception {
    IndexDataProviderTestMethods.testGetTermData_4args(index, this.instance);
  }

  /**
   * Test of getTermData method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception indicating a failure
   */
  @Test
  public void testGetTermData_3args() throws Exception {
    IndexDataProviderTestMethods.testGetTermData_3args(index, this.instance);
  }

  /**
   * Test of clearTermData method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception indicating a failure
   */
  @Test
  public void testClearTermData() throws Exception {
    IndexDataProviderTestMethods.testClearTermData(index, this.instance);
  }

  /**
   * Test of getDocumentModel method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentModel() throws Exception {
    IndexDataProviderTestMethods.testGetDocumentModel(index, this.instance);
  }

  /**
   * Test of getDocumentModel method, of class DirectIndexDataProvider. Using
   * stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentModel__stopped() throws Exception {
    IndexDataProviderTestMethods.testGetDocumentModel__stopped(index,
            this.instance);
  }

  /**
   * Test of getDocumentModel method, of class DirectIndexDataProvider. Using
   * random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentModel__randField() throws Exception {
    IndexDataProviderTestMethods.testGetDocumentModel__randField(index,
            this.instance);
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
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentsTermSet() throws Exception {
    IndexDataProviderTestMethods.testGetDocumentsTermSet(index, this.instance);
  }

  /**
   * Test of getDocumentsTermSet method, of class DirectIndexDataProvider.
   * Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentsTermSet__stopped() throws Exception {
    IndexDataProviderTestMethods.testGetDocumentsTermSet__stopped(index,
            this.instance);
  }

  /**
   * Test of getDocumentsTermSet method, of class DirectIndexDataProvider.
   * Using random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentsTermSet__randField() throws Exception {
    IndexDataProviderTestMethods.testGetDocumentsTermSet__randField(index,
            this.instance);
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
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testDocumentContains() throws Exception {
    IndexDataProviderTestMethods.testDocumentContains(this.instance);
  }

  /**
   * Test of documentContains method, of class DirectIndexDataProvider. Using
   * stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testDocumentContains__stopped() throws Exception {
    IndexDataProviderTestMethods.testDocumentContains__stopped(index,
            this.instance);
  }

  /**
   * Test of documentContains method, of class DirectIndexDataProvider. Using
   * random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testDocumentContains__randField() throws Exception {
    IndexDataProviderTestMethods.testDocumentContains__randField(index,
            this.instance);
  }

  /**
   * Test of registerPrefix method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testRegisterPrefix() throws Exception {
    IndexDataProviderTestMethods.testRegisterPrefix(instance);
  }

  /**
   * Test of fieldsChanged method, of class DirectIndexDataProvider.
   */
  @Test
  public void testFieldsChanged() {
    IndexDataProviderTestMethods.testFieldsChanged(index, instance);
  }

  /**
   * Test of getDocumentFrequency method, of class DirectIndexDataProvider.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentFrequency() throws Exception {
    LOG.info("Test getDocumentFrequency");
    IndexDataProviderTestMethods.testGetDocumentFrequency(index, instance);
  }

  /**
   * Test of getDocumentFrequency method, of class DirectIndexDataProvider.
   * Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentFrequency__stopped() throws Exception {
    LOG.info("Test getDocumentFrequency");
    IndexDataProviderTestMethods.testGetDocumentFrequency(index, instance);
  }

  /**
   * Test of getDocumentFrequency method, of class DirectIndexDataProvider.
   * Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentFrequency__randField() throws Exception {
    LOG.info("Test getDocumentFrequency");
    IndexDataProviderTestMethods.testGetDocumentFrequency__randField(index,
            instance);
  }

  /**
   * Test of warmUp method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testWarmUp() throws Exception {
    IndexDataProviderTestMethods.testWarmUp(index, instance);
  }

  /**
   * Test of wordsChanged method, of class DirectIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testWordsChanged() throws Exception {
    IndexDataProviderTestMethods.testWordsChanged(index, instance);
  }
}
