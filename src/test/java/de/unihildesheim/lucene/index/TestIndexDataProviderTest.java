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
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.lucene.index.MultiFields;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Test for {@link TestIndexDataProvider}.
 * <p>
 * Some test methods are kept here (being empty) to satisfy Netbeans automatic
 * test creation routine.
 *
 * @author Jens Bertram
 */
public final class TestIndexDataProviderTest
        extends IndexDataProviderTestCase {

  /**
   * {@link IndexDataProvider} class being tested.
   */
  private static final Class<? extends IndexDataProvider> DATAPROV_CLASS
          = TestIndexDataProvider.class;

  /**
   * Initialize the test.
   *
   * @throws Exception Any exception indicates an error
   */
  public TestIndexDataProviderTest() throws Exception {
    super(new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL),
            DATAPROV_CLASS);
  }

  /**
   * Initialize the {@link Environment}.
   *
   * @param fields Fields to use (may be null to use all)
   * @param stopwords Stopwords to use (may be null)
   * @throws Exception Any exception indicates an error
   */
  private void initEnvironment(final Collection<String> fields,
          final Collection<String> stopwords) throws Exception {
    index.setupEnvironment(fields, stopwords);
    Environment.getDataProvider().warmUp();
  }

  /**
   * Test of getIndexDir method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetIndexDir() throws Exception {
    initEnvironment(null, null);
    assertTrue("Index dir does not exist.", new File(index.getIndexDir()).
            exists());
  }

  /**
   * Test of getQueryString method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetQueryString_0args() throws Exception {
    initEnvironment(null, null);
    final String qString = index.getQueryString();
    final Collection<ByteArray> qTerms = QueryUtils.getUniqueQueryTerms(
            qString);
    final Collection<ByteArray> sWords = IndexTestUtil.
            getStopwordBytesFromEnvironment();

    if (sWords != null) { // test only, if stopwords are set
      for (ByteArray qTerm : qTerms) {
        if (sWords.contains(qTerm)) {
          @SuppressWarnings("UnnecessaryUnboxing")
          final long result = index.getTermFrequency(qTerm).longValue();
          assertEquals("Stopword has term frequency >0.", 0L, result);
        } else {
          assertNotEquals(this, index.getTermFrequency(qTerm));
        }
      }
    }
  }

  /**
   * Test of getQueryObj method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetQueryObj_0args() throws Exception {
    initEnvironment(null, null);
    // stopwords are already removed
    final Collection<String> qTerms = index.getQueryObj().getQueryTerms();
    for (String term : qTerms) {
      @SuppressWarnings("UnnecessaryUnboxing")
      final long result = index.getTermFrequency(new ByteArray(term.getBytes(
              "UTF-8"))).longValue();
      assertNotEquals("Term frequency was 0 for search term.", 0L, result);
    }
  }

  /**
   * Test of getUniqueQueryString method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetUniqueQueryString() throws Exception {
    initEnvironment(null, null);
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
    initEnvironment(null, null);
    final String exStr = index.getQueryString();
    final String[] exArr = exStr.split(" ");
    final Collection<String> exColl = Arrays.asList(exArr);

    final String resStr = index.getQueryString(exArr);
    final String[] resArr = resStr.split(" ");
    final Collection<String> resColl = Arrays.asList(resArr);

    assertEquals("Query terms size differs.", exArr.length, resArr.length);
    assertEquals("Query terms size differs.", exColl.size(), resColl.size());
    assertTrue("Query terms content differs.", resColl.containsAll(exColl));
  }

  /**
   * Test of getQueryObj method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetQueryObj_StringArr() throws Exception {
    initEnvironment(null, null);
    final String exStr = index.getQueryString();
    final Collection<ByteArray> oQueryTerms = QueryUtils.getAllQueryTerms(
            exStr);
    final Collection<String> oQueryTermsStr = new ArrayList<>(oQueryTerms.
            size());
    for (ByteArray qTerm : oQueryTerms) {
      oQueryTermsStr.add(ByteArrayUtil.utf8ToString(qTerm));
    }
    // stopwords are already removed
    final Collection<String> qTerms = index.getQueryObj(oQueryTermsStr.
            toArray(new String[oQueryTermsStr.size()])).getQueryTerms();
    for (String term : qTerms) {
      @SuppressWarnings("UnnecessaryUnboxing")
      final long result = index.getTermFrequency(new ByteArray(term.getBytes(
              "UTF-8"))).longValue();
      assertNotEquals("Term frequency was 0 for search term.", 0L, result);
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
    initEnvironment(null, null);
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
   * Test of setupEnvironment method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testSetupEnvironment_Class() throws Exception {
    Class<? extends IndexDataProvider> dataProv = FakeIndexDataProvider.class;
    index.setupEnvironment(dataProv, null, null);
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
    IndexDataProvider dataProv = new FakeIndexDataProvider();
    index.setupEnvironment(dataProv, null, null);
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
    initEnvironment(null, null);
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
    final Collection<String> aFields = IndexTestUtil.getRandomFields(index);
    initEnvironment(aFields, null);
    final Collection<String> result = index.getActiveFieldNames();

    assertEquals("Number of fields differs.", aFields.size(), result.size());
    assertTrue("Not all fields found.", result.containsAll(aFields));
    assertNotEquals("Expected fields >0.", result.size(),
            0);
  }

  /**
   * Test of getDocumentTermFrequencyMap method, of class
   * TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetDocumentTermFrequencyMap() throws Exception {
    initEnvironment(null, null);
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
   * Test of getTermList method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetTermList() throws Exception {
    initEnvironment(null, null);
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
    initEnvironment(null, null);
    Collection<ByteArray> result = index.getTermSet();

    assertFalse("Empty terms set.", result.isEmpty());
    assertEquals("Different count of unique terms reported.", index.
            getUniqueTermsCount(), result.size());
    assertEquals("Unique term count differs.", result.size(), index.
            getUniqueTermsCount());
  }

  /**
   * Test of isInitialized method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testIsInitialized() throws Exception {
    initEnvironment(null, null);
    assertTrue("Index should be initialized.", TestIndexDataProvider.
            isInitialized());
  }

  /**
   * Test of getIndex method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetIndex() {
    index.getIndex();
  }

  /**
   * Test of setupEnvironment method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testSetupEnvironment_3args_1() throws Exception {
    index.setupEnvironment(FakeIndexDataProvider.class, null, null);
  }

  /**
   * Test of setupEnvironment method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testSetupEnvironment_3args_2() throws Exception {
    index.setupEnvironment(new FakeIndexDataProvider(), null, null);
  }

  /**
   * Test of setupEnvironment method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testSetupEnvironment_Collection_Collection() throws Exception {
    index.setupEnvironment(null, null);
  }

  /**
   * Test of getDocumentTermSet method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetDocumentTermSet() {
    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final Collection<ByteArray> result = index.getDocumentTermSet(docId);
      final Collection<ByteArray> exp = index.getDocumentTermFrequencyMap(
              docId).keySet();
      assertEquals("Term count mismatch.", exp.size(), result.size());
      assertTrue("Term list content mismatch.", result.containsAll(exp));
    }
  }

  /**
   * Test of loadCache method, of class TestIndexDataProvider.
   *
   * @see #testLoadCache__plain()
   */
  public void testLoadCache() {
    // implemented in super class
  }

  /**
   * Test of loadOrCreateCache method, of class TestIndexDataProvider.
   *
   * @see #testLoadOrCreateCache__plain()
   */
  public void testLoadOrCreateCache() {
    // implemented in super class
  }

  /**
   * Test of createCache method, of class TestIndexDataProvider.
   *
   * @see #testCreateCache__plain()
   */
  public void testCreateCache() {
    // implemented in super class
  }

  /**
   * Test of getDocumentFrequency method, of class TestIndexDataProvider.
   *
   * @see #testGetDocumentFrequency__plain()
   */
  public void testGetDocumentFrequency() {
    // implemented in super class
  }

  /**
   * Test of getTermFrequency method, of class TestIndexDataProvider.
   *
   * @see #testGetTermFrequency_0args__plain()
   */
  public void testGetTermFrequency_0args() {
    // implemented in super class
  }

  /**
   * Test of getTermFrequency method, of class TestIndexDataProvider.
   *
   * @see #testGetTermFrequency_ByteArray__plain()
   */
  public void testGetTermFrequency_ByteArray() {
    // implemented in super class
  }

  /**
   * Test of getRelativeTermFrequency method, of class TestIndexDataProvider.
   *
   * @see #testGetRelativeTermFrequency__plain()
   */
  public void testGetRelativeTermFrequency() {
    // implemented in super class
  }

  /**
   * Test of getTermsIterator method, of class TestIndexDataProvider.
   *
   * @see #testGetTermsIterator__plain()
   */
  public void testGetTermsIterator() {
    // implemented in super class
  }

  /**
   * Test of getTermsSource method, of class TestIndexDataProvider.
   *
   * @see #testGetTermsSource__plain()
   */
  public void testGetTermsSource() {
    // implemented in super class
  }

  /**
   * Test of getDocumentIdIterator method, of class TestIndexDataProvider.
   *
   * @see #testGetDocumentIdIterator__plain()
   */
  public void testGetDocumentIdIterator() {
    // implemented in super class
  }

  /**
   * Test of getDocumentIdSource method, of class TestIndexDataProvider.
   *
   * @see #testGetDocumentIdSource__plain()
   */
  public void testGetDocumentIdSource() {
    // implemented in super class
  }

  /**
   * Test of getUniqueTermsCount method, of class TestIndexDataProvider.
   *
   * @see #testGetUniqueTermsCount__plain()
   */
  public void testGetUniqueTermsCount() {
    // implemented in super class
  }

  /**
   * Test of getDocumentModel method, of class TestIndexDataProvider.
   *
   * @see #testGetDocumentModel__plain()
   */
  public void testGetDocumentModel() {
    // implemented in super class
  }

  /**
   * Test of hasDocument method, of class TestIndexDataProvider.
   *
   * @see #testHasDocument__plain()
   */
  public void testHasDocument() {
    // implemented in super class
  }

  /**
   * Test of getDocumentCount method, of class TestIndexDataProvider.
   *
   * @see #testGetDocumentCount__plain()
   */
  public void testGetDocumentCount() {
    // implemented in super class
  }

  /**
   * Test of documentContains method, of class TestIndexDataProvider.
   *
   * @see #testDocumentContains__plain()
   */
  public void testDocumentContains() {
    // implemented in super class
  }

  /**
   * Test of dispose method, of class TestIndexDataProvider.
   *
   * @see #testDispose__plain()
   */
  public void testDispose() {
    // implemented in super class
  }

  /**
   * Test of getDocumentsTermSet method, of class TestIndexDataProvider.
   *
   * @see #testGetDocumentsTermSet__plain()
   */
  public void testGetDocumentsTermSet() {
    // implemented in super class
  }

  /**
   * Test of warmUp method, of class TestIndexDataProvider.
   *
   * @see #testWarmUp__plain()
   */
  public void testWarmUp() {
    // implemented in super class
  }

}
