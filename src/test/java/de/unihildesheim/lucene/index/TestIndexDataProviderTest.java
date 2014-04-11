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
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.concurrent.processing.Source;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.lucene.index.MultiFields;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link TestIndexDataProvider}.
 *
 
 */
public final class TestIndexDataProviderTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          TestIndexDataProviderTest.class);

  /**
   * Temporary Lucene memory index.
   */
  private static TestIndexDataProvider index;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    index = new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
            isInitialized());
  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static void tearDownClass() {
    index.dispose();
  }

  /**
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public void setUp() throws Exception {
    Environment.clear();
    index.clearTermData();
    Environment.clearAllProperties();
    index.setupEnvironment();
    Environment.getDataProvider().warmUp();
  }

  /**
   * Test of getDocumentFrequency method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetDocumentFrequency() {
    LOG.info("Test getDocumentFrequency");
    final Collection<ByteArray> idxTerms = index.getTermSet();

    for (ByteArray term : idxTerms) {
      int docCount = 0;
      final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
      while (docIdIt.hasNext()) {
        final int docId = docIdIt.next();
        if (index.documentContains(docId, term)) {
          docCount++;
        }
      }
      assertEquals("Document frequency differs.", docCount, index.
              getDocumentFrequency(term));
    }
  }

  /**
   * Test of getIndexDir method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetIndexDir() {
    LOG.info("GTest getIndexDir");
    assertTrue("Index dir does not exist.", new File(index.getIndexDir()).
            exists());
  }

  /**
   * Test of getQueryString method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  @SuppressWarnings("UnnecessaryUnboxing")
  public void testGetQueryString_0args() throws Exception {
    LOG.info("Test getQueryString");
    final String qString = index.getQueryString();
    final Collection<ByteArray> qTerms = QueryUtils.getUniqueQueryTerms(
            qString);
    final Collection<String> sWordsStr = Environment.getStopwords();
    final Collection<ByteArray> sWords = new ArrayList<>(sWordsStr.size());
    for (String sWord : sWordsStr) {
      sWords.add(new ByteArray(sWord.getBytes("UTF-8")));
    }
    for (ByteArray qTerm : qTerms) {
      if (sWords.contains(qTerm)) {
        assertEquals("Stopword has term frequency >0.", 0L, index.
                getTermFrequency(qTerm).longValue());
      } else {
        assertNotEquals(this, index.getTermFrequency(qTerm));
      }
    }
  }

  /**
   * Test of getQueryObj method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  @SuppressWarnings("UnnecessaryUnboxing")
  public void testGetQueryObj_0args() throws Exception {
    LOG.info("Test getQueryObj");
    // stopwords are already removed
    final Collection<String> qTerms = index.getQueryObj().getQueryTerms();
    for (String term : qTerms) {
      assertNotEquals("Term frequency was 0 for search term.", 0L,
              index.getTermFrequency(new ByteArray(term.getBytes("UTF-8"))).
              longValue());
    }
  }

  /**
   * Test of getUniqueQueryString method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetUniqueQueryString() throws Exception {
    LOG.info("Test getUniqueQueryString");
    String result = index.getUniqueQueryString();
    Collection<ByteArray> qTerms = QueryUtils.getAllQueryTerms(result);
    Set<ByteArray> qTermsUnique = new HashSet<>(qTerms);
    assertEquals("Query string was not made of unique terms.", qTerms.size(),
            qTermsUnique.size());
  }

  /**
   * Test of getQueryString method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetQueryString_StringArr() throws Exception {
    LOG.info("Test getQueryString [terms]");
    final String oQueryStr = index.getQueryString();
    final Collection<ByteArray> oQueryTerms = QueryUtils.getAllQueryTerms(
            oQueryStr);
    final Collection<String> oQueryTermsStr = new ArrayList<>(oQueryTerms.
            size());
    for (ByteArray qTerm : oQueryTerms) {
      oQueryTermsStr.add(ByteArrayUtil.utf8ToString(qTerm));
    }

    String result = index.getQueryString(oQueryTermsStr.toArray(
            new String[oQueryTermsStr.size()]));
    assertEquals(oQueryStr, result);
  }

  /**
   * Test of getQueryObj method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  @SuppressWarnings("UnnecessaryUnboxing")
  public void testGetQueryObj_StringArr() throws Exception {
    LOG.info("Test getQueryObj [terms]");
    final String oQueryStr = index.getQueryString();
    final Collection<ByteArray> oQueryTerms = QueryUtils.getAllQueryTerms(
            oQueryStr);
    final Collection<String> oQueryTermsStr = new ArrayList<>(oQueryTerms.
            size());
    for (ByteArray qTerm : oQueryTerms) {
      oQueryTermsStr.add(ByteArrayUtil.utf8ToString(qTerm));
    }
    // stopwords are already removed
    final Collection<String> qTerms = index.getQueryObj(oQueryTermsStr.
            toArray(new String[oQueryTermsStr.size()])).getQueryTerms();
    for (String term : qTerms) {
      assertNotEquals("Term frequency was 0 for search term.", 0L,
              index.getTermFrequency(new ByteArray(term.getBytes("UTF-8"))).
              longValue());
      assertTrue("Unknown term found.", oQueryTermsStr.contains(term));
    }

    qTerms.removeAll(oQueryTermsStr);
    if (!qTerms.isEmpty()) {
      final Collection<String> stopwords = Environment.getStopwords();
      for (String term : qTerms) {
        assertTrue("Found term - expected stopword.", stopwords.contains(term));
      }
    }
  }

  /**
   * Test of getRandomFields method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetRandomFields() throws Exception {
    LOG.info("Test getRandomFields");
    Collection<String> result = index.getRandomFields();
    final Iterator<String> fields = MultiFields.getFields(index.getIndex().
            getReader()).iterator();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<String> fieldNames = new ArrayList<>();
    while (fields.hasNext()) {
      fieldNames.add(fields.next());
    }

    for (String field : result) {
      assertTrue("Unknown field. f=" + field, fieldNames.contains(field));
    }
  }

  /**
   * Test of enableAllFields method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testEnableAllFields() throws Exception {
    LOG.info("Test enableAllFields");
    final Iterator<String> fields = MultiFields.getFields(index.getIndex().
            getReader()).iterator();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<String> fieldNames = new ArrayList<>();
    while (fields.hasNext()) {
      fieldNames.add(fields.next());
    }

    index.enableAllFields();
    final Collection<String> knownFields = index.getActiveFieldNames();

    assertEquals("Field count differs.", fieldNames.size(), knownFields.size());
    assertTrue("Not all fields in result set.", knownFields.containsAll(
            fieldNames));
  }

  /**
   * Test of setupEnvironment method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testSetupEnvironment_Class() throws Exception {
    LOG.info("setupEnvironment [class]");
    Environment.clear();
    Class<? extends IndexDataProvider> dataProv = FakeIndexDataProvider.class;
    index.setupEnvironment(dataProv);
    assertTrue("Expected environment to be initialized.", Environment.
            isInitialized());
    assertTrue("IndexDataProvider is not of expected type.", Environment.
            getDataProvider() instanceof FakeIndexDataProvider);
  }

  /**
   * Test of setupEnvironment method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testSetupEnvironment_IndexDataProvider() throws Exception {
    LOG.info("Test setupEnvironment [instance]");
    Environment.clear();
    IndexDataProvider dataProv = new FakeIndexDataProvider();
    index.setupEnvironment(dataProv);
    assertTrue("Expected environment to be initialized.", Environment.
            isInitialized());
    assertTrue("IndexDataProvider is not of expected type.", Environment.
            getDataProvider() instanceof FakeIndexDataProvider);
  }

  /**
   * Test of setupEnvironment method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testSetupEnvironment_0args() throws Exception {
    LOG.info("Test setupEnvironment");
    Environment.clear();
    index.setupEnvironment();
    assertTrue("Expected environment to be initialized.", Environment.
            isInitialized());
    assertTrue("IndexDataProvider is not of expected type.", Environment.
            getDataProvider() instanceof TestIndexDataProvider);
  }

  /**
   * Test of getActiveFieldNames method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetActiveFieldNames() throws Exception {
    LOG.info("Test getActiveFieldNames");
    final Collection<String> aFields = IndexTestUtil.setRandomFields(index);
    final Collection<String> result = index.getActiveFieldNames();

    assertEquals("Number of fields differs.", aFields.size(), result.size());
    assertTrue("Not all fields found.", result.containsAll(aFields));
    assertNotEquals("Expected fields >0.", result.size(),
            0);
  }

  /**
   * Test of getDocumentTermFrequencyMap method, of class
   * TestIndexDataProvider.
   */
  @Test
  public void testGetDocumentTermFrequencyMap() {
    LOG.info("Test getDocumentTermFrequencyMap");
    for (int i = 0; i < index.getDocumentCount(); i++) {
      final DocumentModel docModel = index.getDocumentModel(i);
      final Map<ByteArray, Long> dtfMap = index.getDocumentTermFrequencyMap(i);
      assertEquals(
              "Document model term list size != DocumentTermFrequencyMap.size().",
              dtfMap.size(), docModel.termFreqMap.size());
      assertTrue("Document model term list != DocumentTermFrequencyMap keys.",
              dtfMap.keySet().containsAll(docModel.termFreqMap.keySet()));
      for (Entry<ByteArray, Long> e : dtfMap.entrySet()) {
        assertEquals("Frequency value mismatch.", e.getValue(),
                docModel.termFreqMap.get(e.getKey()));
      }
    }
  }

  /**
   * Test of getDocumentTermSet method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetDocumentTermSet() {
    LOG.info("Test getDocumentTermSet");
    for (int i = 0; i < index.getDocumentCount(); i++) {
      final DocumentModel docModel = index.getDocumentModel(i);
      final Map<ByteArray, Long> dtfMap = index.getDocumentTermFrequencyMap(i);
      assertEquals(
              "Document model term list size != DocumentTermFrequencyMap.size().",
              dtfMap.size(), docModel.termFreqMap.size());
      assertTrue("Document model term set != DocumentTermFrequencyMap keys.",
              dtfMap.keySet().containsAll(docModel.termFreqMap.keySet()));
      for (Entry<ByteArray, Long> e : dtfMap.entrySet()) {
        assertEquals("Frequency value mismatch.", e.getValue(),
                docModel.termFreqMap.get(e.getKey()));
      }
    }
  }

  /**
   * Test of getTermList method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetTermList() throws Exception {
    LOG.info("Test getTermList");
    final Collection<ByteArray> result = index.getTermList();
    final Collection<ByteArray> tSet = new HashSet<>(result);

    assertEquals("Overall term frequency and term kist size differs.", index.
            getTermFrequency(), result.size());

    long overAllFreq = 0;
    for (ByteArray term : tSet) {
      overAllFreq += index.getTermFrequency(term);
    }

    assertEquals("Calculated overall frequency != term list size.",
            overAllFreq, result.size());
  }

  /**
   * Test of getTermSet method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetTermSet() throws Exception {
    LOG.info("Test getTermSet");
    Collection<ByteArray> result = index.getTermSet();

    assertFalse("Empty terms set.", result.isEmpty());
    assertEquals("Different count of unique terms reported.", index.
            getUniqueTermsCount(), result.size());
    assertEquals("Unique term count differs.", result.size(), index.
            getUniqueTermsCount());
  }

  /**
   * Test of clearTermData method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testClearTermData() throws Exception {
    IndexDataProviderTestMethods.testClearTermData(index, index);
  }

  /**
   * Test of getTermFrequency method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetTermFrequency_0args() {
    LOG.info("Test getTermFrequency []");
    assertEquals("Term frequency and term list size differs.", index.
            getTermFrequency(), index.getTermList().size());
  }

  /**
   * Test of getTermFrequency method, of class TestIndexDataProvider.
   */
  @Test
  @SuppressWarnings("UnnecessaryUnboxing")
  public void testGetTermFrequency_ByteArray() {
    LOG.info("Test getTermFrequency [term]");
    final Collection<ByteArray> terms = index.getTermList();
    for (ByteArray term : terms) {
      assertNotNull("Expected frequency value for term.", index.
              getTermFrequency(term));
      assertNotEquals("Expected frequency value >0 for term.", index.
              getTermFrequency(term).longValue(), 0L);
    }
  }

  /**
   * Test of getRelativeTermFrequency method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetRelativeTermFrequency() {
    LOG.info("Test getRelativeTermFrequency");
    final Collection<ByteArray> terms = index.getTermList();
    for (ByteArray term : terms) {
      assertNotEquals("Expected frequency value >0 term.", index.
              getRelativeTermFrequency(term), 0);
      assertEquals(
              "Calculated and reported relative term frequency differs. tf="
              + index.getTermFrequency(term) + " cf=" + index.
              getTermFrequency(),
              (index.getTermFrequency(term).doubleValue() / Long.valueOf(
                      index.getTermFrequency()).doubleValue()),
              index.getRelativeTermFrequency(term), 0d);
    }
  }

  /**
   * Test of getTermsIterator method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetTermsIterator() throws Exception {
    IndexDataProviderTestMethods.testGetTermsIterator(index, index);
  }

  /**
   * Test of getTermsIterator method, of class TestIndexDataProvider. Using
   * stopwords.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetTermsIterator__stopped() throws Exception {
    IndexDataProviderTestMethods.testGetTermsIterator__stopped(index, index);
  }

  /**
   * Test of getTermsIterator method, of class TestIndexDataProvider. Using
   * random fields.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetTermsIterator__randField() throws Exception {
    IndexDataProviderTestMethods.testGetTermsIterator__randField(index, index);
  }

  /**
   * Test of getTermsSource method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetTermsSource() {
    IndexDataProviderTestMethods.testGetTermsSource(index, index);
  }

  /**
   * Test of getDocumentIdIterator method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetDocumentIdIterator() {
    LOG.info("Test getDocumentIdIterator");
    final List<Integer> docIdList = new ArrayList<>((int) index.
            getDocumentCount());
    for (int i = 0; i < index.getDocumentCount(); i++) {
      docIdList.add(i);
    }
    Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      assertTrue("Unknown document-id found.", docIdList.
              remove(docIdIt.next()));
    }
    assertTrue("Document-ids left after iteration.", docIdList.isEmpty());
  }

  /**
   * Test of getDocumentIdSource method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetDocumentIdSource() {
    IndexDataProviderTestMethods.testGetDocumentIdSource(index, index);
  }

  /**
   * Test of getUniqueTermsCount method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetUniqueTermsCount() {
    IndexDataProviderTestMethods.testGetUniqueTermsCount(index, index);
  }

  /**
   * Test of getUniqueTermsCount method, of class TestIndexDataProvider. Using
   * stopwords.
   */
  @Test
  public void testGetUniqueTermsCount__stopped() {
    IndexDataProviderTestMethods.
            testGetUniqueTermsCount__stopped(index, index);
  }

  /**
   * Test of getUniqueTermsCount method, of class TestIndexDataProvider. Using
   * random fields.
   */
  @Test
  public void testGetUniqueTermsCount__randFields() {
    IndexDataProviderTestMethods.
            testGetUniqueTermsCount__randFields(index, index);
  }

  /**
   * Test of setTermData method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetTermData() throws Exception {
    IndexDataProviderTestMethods.testSetTermData(index, index);
  }

  /**
   * Test of getTermData method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermData_4args() throws Exception {
    IndexDataProviderTestMethods.testGetTermData_4args(index, index);
  }

  /**
   * Test of getTermData method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTermData_3args() throws Exception {
    IndexDataProviderTestMethods.testGetTermData_3args(index, index);
  }

  /**
   * Test of getDocumentModel method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetDocumentModel() throws Exception {
    IndexDataProviderTestMethods.testGetDocumentModel(index, index);
  }

  /**
   * Test of getDocumentModel method, of class TestIndexDataProvider. Using
   * stopwords.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetDocumentModel__stopped() throws Exception {
    IndexDataProviderTestMethods.testGetDocumentModel__stopped(index, index);
  }

  /**
   * Test of getDocumentModel method, of class TestIndexDataProvider. Using
   * random fields.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetDocumentModel__randField() throws Exception {
    IndexDataProviderTestMethods.testGetDocumentModel__randField(index, index);
  }

  /**
   * Test of hasDocument method, of class TestIndexDataProvider.
   */
  @Test
  public void testHasDocument() {
    IndexDataProviderTestMethods.testHasDocument(index, index);
  }

  /**
   * Test of getDocumentCount method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetDocumentCount() throws Exception {
    LOG.info("Test getDocumentCount");
    assertNotEquals("No documents found.", 0, index.getDocumentCount());
    assertEquals("Document count mismatch.", index.getIndex().getReader().
            maxDoc(), index.getDocumentCount());
  }

  /**
   * Test of documentContains method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testDocumentContains() throws Exception {
    IndexDataProviderTestMethods.testDocumentContains(index);
  }

  /**
   * Test of documentContains method, of class TestIndexDataProvider. Using
   * stopwords.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testDocumentContains__stopped() throws Exception {
    IndexDataProviderTestMethods.testDocumentContains__stopped(index, index);
  }

  /**
   * Test of documentContains method, of class TestIndexDataProvider. Using
   * random fields.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testDocumentContains__randField() throws Exception {
    IndexDataProviderTestMethods.testDocumentContains__randField(index, index);
  }

  /**
   * Test of dispose method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testDispose() throws Exception {
    LOG.info("Test dispose");
    index.dispose(); // expect nothing special here
  }

  /**
   * Test of getDocumentsTermSet method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetDocumentsTermSet() throws Exception {
    LOG.info("Test getDocumentsTermSet");
    final int docCount = RandomValue.getInteger(2, (int) index.
            getDocumentCount());
    final Collection<Integer> docIds = new ArrayList<>(docCount);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<ByteArray> expResult = new HashSet<>();

    // collect some random document ids
    for (int i = 0; i < docCount; i++) {
      docIds.add(RandomValue.getInteger(0,
              (int) index.getDocumentCount() - 1));
    }

    // get terms from those random documents
    for (Integer docId : docIds) {
      expResult.addAll(index.getDocumentTermSet(docId));
    }

    // get the termSet for those random documents
    final Collection<ByteArray> result = index.getDocumentsTermSet(docIds);

    assertEquals("Term count mismatch.", expResult.size(), result.size());
    assertTrue("Terms mismatch.", result.containsAll(expResult));
  }

//  /**
//   * Test of fieldsChanged method, of class TestIndexDataProvider.
//   *
//   * @throws Exception Any exception indicates an error
//   */
//  @Test
//  public void testFieldsChanged() throws Exception {
//    LOG.info("Test fieldsChanged");
//    // this should trigger fieldsChanged
//    final Collection<String> fld = IndexTestUtil.setRandomFields(index);
//    Collection<String> cFld = index.getActiveFieldNames();
//    assertEquals("Field sizes differ.", fld.size(), cFld.size());
//    assertTrue("Not all fields present.", cFld.containsAll(fld));
//
//    index.fieldsChanged(new String[0]);
//    cFld = index.getActiveFieldNames();
//    assertEquals("Field sizes differ.", fld.size(), cFld.size());
//    assertTrue("Not all fields present.", cFld.containsAll(fld));
//
//    index.fieldsChanged(null);
//    cFld = index.getActiveFieldNames();
//    assertEquals("Field sizes differ.", fld.size(), cFld.size());
//    assertTrue("Not all fields present.", cFld.containsAll(fld));
//  }
//
//  /**
//   * Test of wordsChanged method, of class TestIndexDataProvider.
//   *
//   * @throws Exception Any exception indicates an error
//   */
//  @Test
//  public void testWordsChanged() throws Exception {
//    LOG.info("Test wordsChanged");
//    IndexTestUtil.setRandomStopWords(index);
//    final Collection<String> words = Environment.getStopwords();
//
//    Collection<String> cWords = index.testGetStopwords();
//    assertEquals("Stopword list size differs.", words.size(), cWords.size());
//    assertTrue("Not all stopwords present.", cWords.containsAll(words));
//
//    index.wordsChanged(Collections.<String>emptyList());
//    cWords = index.testGetStopwords();
//    assertEquals("Stopword list size differs.", words.size(), cWords.size());
//    assertTrue("Not all stopwords present.", cWords.containsAll(words));
//
//    index.wordsChanged(null);
//    cWords = index.testGetStopwords();
//    assertEquals("Stopword list size differs.", words.size(), cWords.size());
//    assertTrue("Not all stopwords present.", cWords.containsAll(words));
//  }
  /**
   * Test of warmUp method, of class TestIndexDataProvider.
   */
  @Test
  public void testWarmUp() {
    LOG.info("Test warmUp()");
    index.warmUp(); // expect to do nothing
  }

  /**
   * Test of isInitialized method, of class TestIndexDataProvider.
   */
  @Test
  public void testIsInitialized() {
    LOG.info("Test isInitialized");
    assertTrue("Index should be initialized.", TestIndexDataProvider.
            isInitialized());
  }

  /**
   * Test of registerPrefix method, of class TestIndexDataProvider.
   */
  @Test
  public void testRegisterPrefix() {
    IndexDataProviderTestMethods.testRegisterPrefix(index);
  }

  /**
   * Test of testGetStopwords method, of class TestIndexDataProvider.
   */
  @Test
  public void testTestGetStopwords() {
    LOG.info("testGetStopwords");
    final Collection<String> words = Environment.getStopwords();
    Collection<String> cWords = index.testGetStopwords();
    assertEquals("Stopword list size differs.", words.size(), cWords.size());
    assertTrue("Not all stopwords present.", cWords.containsAll(words));
  }

  /**
   * Test of testGetFieldNames method, of class TestIndexDataProvider.
   */
  @Test
  public void testTestGetFieldNames() {
    LOG.info("testGetFieldNames");
    final Collection<String> fld = IndexTestUtil.setRandomFields(index);
    Collection<String> cFld = index.testGetFieldNames();
    assertEquals("Field sizes differ.", fld.size(), cFld.size());
    assertTrue("Not all fields present.", cFld.containsAll(fld));
  }

  /**
   * No-Operation data-provider for testing purposes.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class FakeIndexDataProvider implements IndexDataProvider {

    @Override
    public long getTermFrequency() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void warmUp() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Long getTermFrequency(ByteArray term) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getDocumentFrequency(ByteArray term) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public double getRelativeTermFrequency(ByteArray term) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void dispose() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterator<ByteArray> getTermsIterator() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Source<ByteArray> getTermsSource() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterator<Integer> getDocumentIdIterator() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Source<Integer> getDocumentIdSource() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getUniqueTermsCount() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void registerPrefix(String prefix) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object setTermData(String prefix, int documentId, ByteArray term,
            String key, Object value) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getTermData(String prefix, int documentId, ByteArray term,
            String key) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<ByteArray, Object> getTermData(String prefix, int documentId,
            String key) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clearTermData() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DocumentModel getDocumentModel(int docId) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasDocument(Integer docId) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Collection<ByteArray> getDocumentsTermSet(
            Collection<Integer> docIds) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getDocumentCount() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean documentContains(int documentId, ByteArray term) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Collection<String> testGetStopwords() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Collection<String> testGetFieldNames() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

  }

  /**
   * Test of getIndex method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetIndex() {
    LOG.info("Test getIndex");
    index.getIndex();
  }
}
