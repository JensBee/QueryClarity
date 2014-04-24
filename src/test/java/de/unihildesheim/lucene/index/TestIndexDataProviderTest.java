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
import de.unihildesheim.util.concurrent.processing.Source;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.lucene.index.MultiFields;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link TestIndexDataProvider}.
 *
 * @author Jens Bertram
 */
public final class TestIndexDataProviderTest
        extends IndexDataProviderTestMethods {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          TestIndexDataProviderTest.class);

  /**
   * Temporary Lucene memory index.
   */
  private static TestIndexDataProvider index;

  private static final Class<? extends IndexDataProvider> DATAPROV_CLASS
          = TestIndexDataProvider.class;

  public TestIndexDataProviderTest() {
    super(index, DATAPROV_CLASS);
  }

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
   * Run before each test.
   */
  @Before
  public void setUp() {
    Environment.clear();
    Environment.clearAllProperties();
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
  @SuppressWarnings("UnnecessaryUnboxing")
  public void testGetQueryString_0args() throws Exception {
    initEnvironment(null, null);
    final String qString = index.getQueryString();
    final Collection<ByteArray> qTerms = QueryUtils.getUniqueQueryTerms(
            qString);
    final Collection<ByteArray> sWords = IndexTestUtil.
            getStopwordBytesFromEnvironment();

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
    initEnvironment(null, null);
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
    initEnvironment(null, null);
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
    public void loadCache(String name) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void loadOrCreateCache(String name) throws Exception {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void createCache(String name) throws Exception {
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
