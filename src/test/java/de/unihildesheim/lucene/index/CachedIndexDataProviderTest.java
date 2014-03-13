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
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.DocumentModelException;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.RandomValue;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link CachedIndexDataProvider}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
@SuppressWarnings({"DM_DEFAULT_ENCODING", "DM_DEFAULT_ENCODING"})
public final class CachedIndexDataProviderTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          CachedIndexDataProviderTest.class);

  /**
   * DataProvider instance used during the test.
   */
  private static CachedIndexDataProvider instance = null;

  /**
   * Temporary Lucene memory index.
   */
  private static TestIndex index;

  /**
   * Amount of test term-data to generate.
   */
  private static final int TEST_TERMDATA_AMOUNT = 10000;

  /**
   * Flag, indicating if the index has changed.
   */
  private static boolean indexChanged = false;

  /**
   * General rule to catch expected exceptions.
   */
  @Rule
  @java.lang.SuppressWarnings("PublicField")
  public ExpectedException exception = ExpectedException.none();

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
    newInstance();
  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static void tearDownClass() {
    instance.dispose();
  }

  /**
   * Setup, run before each test.
   */
  @Before
  public void setUp() {
    // clear any external data stored to index
    reCreateInstanceIndex();
  }

  /**
   * Run after each test.
   */
  @After
  public void tearDown() {
    // close instance
    index.clearTermData();
    if (instance.isStorageInitialized()) {
      instance.clearTermData();
    }
    Environment.clear();
  }

  /**
   * Re-create the {@link CachedIndexDataProvider} instance used for testing.
   */
  private static void newInstance() {
    // create a temporary instance of the data provider
    try {
      if (instance != null) {
        instance.dispose();
      }
      instance = getNewInstance();
    } catch (Exception ex) {
      fail("Failed to create a new instance for testing.");
      LOG.error("StackTrace", ex);
    }
    reCreateInstanceIndex();
  }

  /**
   * Get a new {@link CachedIndexDataProvider} instance.
   *
   * @return New instance
   * @throws Exception Any exception thrown indicates an error
   */
  private static CachedIndexDataProvider getNewInstance() throws Exception {
    return new CachedIndexDataProvider("test", true);
  }

  /**
   * Re-Created the document index of the DataProvider instance.
   */
  private static void reCreateInstanceIndex() {
    if (!instance.isStorageInitialized() || indexChanged) {
      LOG.info("Re-creating instance index (initialized={} changed={})",
              instance.isStorageInitialized(), indexChanged);
      try {
        instance.recalculateData(true);
        indexChanged = false;
      } catch (IOException | DocumentModelException ex) {
        fail("Failed to re-create instance index.");
        LOG.error("StackTrace", ex);
      }
    }
  }

  /**
   * Test all methods on an uninitialized index.
   *
   * @throws Exception Any exception indicating a failure
   */
  @Test
  @SuppressWarnings("DM_DEFAULT_ENCODING")
  public void testUninitialized() throws Exception {
    final CachedIndexDataProvider anInstance = getNewInstance();

    final DocumentModel docModel = new DocumentModel.DocumentModelBuilder(1).
            getModel();
    final BytesWrap term = new BytesWrap("foo".getBytes());
    final Collection<BytesWrap> terms = new ArrayList(1);
    terms.add(term);

    exception.expect(IllegalStateException.class);
    anInstance.addDocument(docModel);
    anInstance.addToTermFreqValue(term, 1);
    anInstance.addToTermFrequency(1L);
    anInstance.clearTermData();
    anInstance.commitHook();
    anInstance.documentContains(1, term);
    anInstance.getDocumentCount();
    anInstance.getDocumentIdIterator();
    anInstance.getDocumentIdSource();
    anInstance.getDocumentModel(1);
    anInstance.getKnownPrefixCount();
    anInstance.getKnownPrefixes();
    anInstance.getRelativeTermFrequency(term);
    anInstance.getTermData("foo", 1, "bar");
    anInstance.getTermData("foo", 1, term, "bar");
    anInstance.getTermFrequency();
    anInstance.getTermFrequency(term);
    anInstance.getTermsIterator();
    anInstance.getTermsSource();
    anInstance.getUniqueTermsCount();
    anInstance.hasDocument(1);
    anInstance.registerPrefix("foo");
    anInstance.setTermData("foo", 1, term, "bar", "baz");
    anInstance.setTermFreqValue(term, 0.0);
    anInstance.transactionHookRelease();
    anInstance.transactionHookRequest();
    anInstance.transactionHookRoolback();
//    anInstance.updateDocument(docModel);
  }

  /**
   * Test of getKnownPrefixes method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetKnownPrefixes() {
    LOG.info("Test getKnownPrefixes");
    Collection<String> result;
    result = instance.getKnownPrefixes();
    assertTrue("There should be no prefixes known.", result.isEmpty());

    instance.registerPrefix("test");
    result = instance.getKnownPrefixes();
    assertEquals("There should be exactly one known prefix.", 1, result.size());
  }

  /**
   * Test of tryGetStoredData method, of class CachedIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception indicating a failure
   */
  @Test
  public void testTryGetStoredData() throws Exception {
    LOG.info("Test tryGetStoredData");
    final CachedIndexDataProvider anInstance = getNewInstance();
    // now data should be there
    assertFalse("There should be no data to load.", anInstance.
            tryGetStoredData());
  }

  /**
   * Test of recalculateData method, of class CachedIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception indicating a failure
   */
  @Test
  public void testRecalculateData() throws Exception {
    LOG.info("Test recalculateData");

    instance.recalculateData(true);
  }

  /**
   * Test of commitHook method, of class CachedIndexDataProvider.
   */
  @Test
  public void testCommitHook() {
    LOG.info("Test commitHook");
    instance.commitHook();
  }

  /**
   * Test of getUniqueTermsCount method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetUniqueTermsCount() {
    IndexDataProviderTestMethods.testGetUniqueTermsCount(index, instance);
  }

  /**
   * Test of addDocument method, of class CachedIndexDataProvider.
   *
   * @throws java.io.IOException Any exception indicating a failure
   */
  @Test
  public void testAddDocument() throws Exception {
    LOG.info("Test addDocument");
    final CachedIndexDataProvider anInstance = getNewInstance();
    anInstance.tryGetStoredData();
    Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final DocumentModel docModel = index.getDocumentModel(docIdIt.next());
      // should be true, because model is not known
      assertTrue("Failed to add new document model.", anInstance.addDocument(
              docModel));
      // should be false, because model is already there
      assertFalse("Document model was added twice.", anInstance.addDocument(
              docModel));
    }
  }

  /**
   * Test of hasDocument method, of class CachedIndexDataProvider.
   */
  @Test
  public void testHasDocument() {
    IndexDataProviderTestMethods.testHasDocument(index, instance);
  }

  /**
   * Test of getTermFrequency method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetTermFrequency_0args() {
    IndexDataProviderTestMethods.testGetTermFrequency_0args(index, instance);
  }

  /**
   * Test of getTermFrequency method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetTermFrequency_BytesWrap() {
    IndexDataProviderTestMethods.testGetTermFrequency_BytesWrap(index,
            instance);
  }

  /**
   * Test of getRelativeTermFrequency method, of class
   * CachedIndexDataProvider.
   */
  @Test
  public void testGetRelativeTermFrequency() {
    IndexDataProviderTestMethods.testGetRelativeTermFrequency(index, instance);
  }

  /**
   * Test of getTermsIterator method, of class CachedIndexDataProvider.
   */
  @Test
  @SuppressWarnings("DM_DEFAULT_ENCODING")
  public void testGetTermsIterator() {
    IndexDataProviderTestMethods.testGetTermsIterator(index, instance);
  }

  /**
   * Test of getDocumentCount method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetDocumentCount() {
    IndexDataProviderTestMethods.testGetDocumentCount(index, instance);
  }

  /**
   * Test of addToTermFreqValue method, of class CachedIndexDataProvider.
   */
  @Test
  @SuppressWarnings({"ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
  public void testAddToTermFreqValue() {
    LOG.info("Test addToTermFreqValue");
    final BytesWrap term = new BytesWrap("testTerm".getBytes());
    Long termFreq;
    long oldValue;

    termFreq = (long) RandomValue.getInteger(50, 1000);
    oldValue = instance.getTermFrequency();

    instance.addToTermFreqValue(term, termFreq);

    assertEquals("Updated overall term frequency value differs.", termFreq
            + oldValue, instance.getTermFrequency());

    oldValue = instance.getTermFrequency();
    termFreq = -10L;
    instance.addToTermFreqValue(term, termFreq);
    assertEquals("Updated overall term frequency value differs.",
            oldValue - 10, instance.getTermFrequency());

    indexChanged = true;
  }

  /**
   * Test of setTermFreqValue method, of class CachedIndexDataProvider.
   */
  @Test
  @SuppressWarnings({"ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
  public void testSetTermFreqValue() {
    LOG.info("Test setTermFreqValue");

    final Iterator<BytesWrap> idxTermsIt = instance.getTermsIterator();
    while (idxTermsIt.hasNext()) {
      final BytesWrap idxTerm = idxTermsIt.next();
      final double value = RandomValue.getDouble();
      instance.setTermFreqValue(idxTerm, value);
      assertEquals("Relative term frequency update failed.", value, instance.
              getRelativeTermFrequency(idxTerm), 0);
    }

    indexChanged = true;
  }

  /**
   * Test of getDocumentModel method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetDocumentModel() {
    IndexDataProviderTestMethods.testGetDocumentModel(index, instance);
  }

  /**
   * Test of getDocumentIdIterator method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetDocumentIdIterator() {
    IndexDataProviderTestMethods.testGetDocumentIdIterator(index, instance);
  }

  /**
   * Test of setTermData method, of class CachedIndexDataProvider.
   */
  @Test
  public void testSetTermData() {
    IndexDataProviderTestMethods.testSetTermData(index, instance);
  }

  /**
   * Test of debugGetInternalTermDataMap method, of class
   * CachedIndexDataProvider.
   */
  @Test
  public void testDebugGetInternalTermDataMap() {
    LOG.info("Test debugGetInternalTermDataMap");
    final Map<Fun.Tuple3<Integer, String, BytesWrap>, Object> result
            = instance.debugGetInternalTermDataMap();

    exception.expect(UnsupportedOperationException.class);
    result.clear();
  }

  /**
   * Test of debugGetKnownPrefixes method, of class CachedIndexDataProvider.
   */
  @Test
  public void testDebugGetKnownPrefixes() {
    LOG.info("Test debugGetKnownPrefixes");
    Collection<String> result;
    result = instance.debugGetKnownPrefixes();
    assertTrue("There should be no prefixes known.", result.isEmpty());

    instance.registerPrefix("test");
    result = instance.debugGetKnownPrefixes();
    assertEquals("There should be exactly one known prefix.", 1, result.size());

    exception.expect(UnsupportedOperationException.class);
    result.clear();
  }

  /**
   * Test of getKnownPrefixCount method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetKnownPrefixCount() {
    LOG.info("Test getKnownPrefixCount");
    int result;
    result = instance.getKnownPrefixCount();
    assertEquals("There should be no prefix known.", 0, result);

    instance.registerPrefix("foo");
    result = instance.getKnownPrefixCount();
    assertEquals("There should one prefix known.", 1, result);
  }

  /**
   * Test of debugGetPrefixMap method, of class CachedIndexDataProvider.
   */
  @Test
  @Ignore
  public void testDebugGetPrefixMap() {
    System.out.println("debugGetPrefixMap");
    String prefix = "";
    CachedIndexDataProvider instance = null;
    Map<Fun.Tuple2<String, String>, Object> expResult = null;
    Map<Fun.Tuple2<String, String>, Object> result
            = instance.debugGetPrefixMap(prefix);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getTermData method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetTermData_4args() {
    IndexDataProviderTestMethods.testGetTermData_4args(index, instance);
  }

  /**
   * Test of getTermData method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetTermData_3args() {
    IndexDataProviderTestMethods.testGetTermData_3args(index, instance);
  }

  /**
   * Test of transactionHookRequest method, of class CachedIndexDataProvider.
   */
  @Test
  public void testTransactionHookRequest() {
    LOG.info("transactionHookRequest");
    assertTrue("Transaction hook request failed.", instance.
            transactionHookRequest());
    assertFalse("Transaction hook granted two times.", instance.
            transactionHookRequest());
    instance.transactionHookRelease();
    assertTrue("Transaction hook request failed after release.", instance.
            transactionHookRequest());
    instance.transactionHookRelease();
  }

  /**
   * Test of transactionHookRelease method, of class CachedIndexDataProvider.
   */
  @Test
  public void testTransactionHookRelease() {
    LOG.info("Test transactionHookRelease");
    assertTrue("Transaction hook request failed.", instance.
            transactionHookRequest());
    assertFalse("Transaction hook granted two times.", instance.
            transactionHookRequest());
    instance.transactionHookRelease();
    assertTrue("Transaction hook request failed after release.", instance.
            transactionHookRequest());
    instance.transactionHookRelease();
  }

  /**
   * Test of transactionHookRoolback method, of class CachedIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception indicating a failure
   */
  @Test
  public void testTransactionHookRoolback() throws Exception {
    LOG.info("Test transactionHookRoolback");
    final CachedIndexDataProvider anInstance = getNewInstance();
    anInstance.tryGetStoredData();
    final long expResult = instance.getDocumentCount();
    assertTrue("Transaction hook request failed.", instance.
            transactionHookRequest());
    Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final DocumentModel docModel = index.getDocumentModel(docIdIt.next());
      // should be true, because model is not known
      assertTrue("Failed to add new document model.", anInstance.addDocument(
              docModel));
    }
    instance.transactionHookRoolback();
    assertEquals("Number of documents differ after transaction rollback.",
            expResult, instance.getDocumentCount());
  }

  /**
   * Test of documentContains method, of class CachedIndexDataProvider.
   */
  @Test
  public void testDocumentContains() {
    IndexDataProviderTestMethods.testDocumentContains(index, instance);
  }

  /**
   * Test of getDocumentIdSource method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetDocumentIdSource() {
    IndexDataProviderTestMethods.testGetDocumentIdSource(index, instance);
  }

  /**
   * Test of getTermsSource method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetTermsSource() {
    IndexDataProviderTestMethods.testGetTermsSource(index, instance);
  }

  /**
   * Test of registerPrefix method, of class CachedIndexDataProvider.
   */
  @Test
  public void testRegisterPrefix() {
    IndexDataProviderTestMethods.testRegisterPrefix(index, instance);
  }

  /**
   * Test of isStorageInitialized method, of class CachedIndexDataProvider.
   */
  @Test
  @Ignore
  public void testIsStorageInitialized() {
    System.out.println("isStorageInitialized");
    CachedIndexDataProvider instance = null;
    boolean expResult = false;
    boolean result = instance.isStorageInitialized();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of addToTermFrequency method, of class CachedIndexDataProvider.
   */
  @Test
  @Ignore
  public void testAddToTermFrequency() {
    System.out.println("addToTermFrequency");
    long mod = 0L;
    CachedIndexDataProvider instance = null;
    instance.addToTermFrequency(mod);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getDocumentsTermSet method, of class CachedIndexDataProvider.
   */
  @Test
  public void testGetDocumentsTermSet() {
    IndexDataProviderTestMethods.testGetDocumentsTermSet(index, instance);
  }

  /**
   * Test of checkStorage method, of class CachedIndexDataProvider.
   */
  @Test
  @Ignore
  public void testCheckStorage() {
    System.out.println("checkStorage");
    CachedIndexDataProvider instance = null;
    instance.checkStorage();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of dispose method, of class CachedIndexDataProvider.
   */
  @Test
  @Ignore
  public void testDispose() {
    System.out.println("dispose");
    CachedIndexDataProvider instance = null;
    instance.dispose();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of clearTermData method, of class CachedIndexDataProvider.
   */
  @Test
  public void testClearTermData() {
    IndexDataProviderTestMethods.testClearTermData(index, this.instance);
  }

  /**
   * Test of documentsContaining method, of class CachedIndexDataProvider.
   */
  @Test
  @Ignore
  public void testDocumentsContaining() {
    System.out.println("documentsContaining");
    BytesWrap term = null;
    CachedIndexDataProvider instance = null;
    Collection<Integer> expResult = null;
    Collection<Integer> result = instance.documentsContaining(term);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

}
