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
package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.util.ByteArrayUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link TestIndexDataProvider}.
 *
 * @author Jens Bertram
 */
public final class TestIndexDataProviderTest
    extends IndexDataProviderTestCase {

  /**
   * Term whitespace split pattern.
   */
  private static final Pattern WS_SPLIT = Pattern.compile(" ");

  /**
   * Initialize the test.
   *
   * @throws Exception Any exception indicates an error
   */
  public TestIndexDataProviderTest()
      throws Exception {
    super(new TestIndexDataProvider(TestIndexDataProvider.DEFAULT_INDEX_SIZE));
  }

  /**
   * Test of getIndexDir method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetIndexDir()
      throws Exception {
    initEnvironment(null, null);
    assertTrue("Index dir does not exist.",
        new File(TestIndexDataProvider.reference.getIndexDir()).exists());
  }

  /**
   * Initialize the test environment.
   *
   * @param fields Fields to use (may be null to use all)
   * @param stopwords Stopwords to use (may be null)
   * @throws Exception Any exception indicates an error
   */
  private void initEnvironment(final Set<String> fields,
      final Set<String> stopwords)
      throws Exception {
    this.referenceIndex.prepareTestEnvironment(fields, stopwords);
  }

  /**
   * Test of getQueryString method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetQueryString_0args()
      throws Exception {
    initEnvironment(null, null);
    final String qString = TestIndexDataProvider.util.getQueryString();
    final Collection<ByteArray> qTerms;
    qTerms = new QueryUtils(
        this.referenceIndex.getAnalyzer(),
        this.referenceIndex.getIndexReader(),
        this.referenceIndex.getDocumentFields()).getUniqueQueryTerms(qString);
    final Collection<ByteArray> sWords = TestIndexDataProvider.reference
        .getStopwords();

    if (sWords != null) { // test only, if stopwords are set
      for (final ByteArray qTerm : qTerms) {
        if (sWords.contains(qTerm)) {
          final long result = this.referenceIndex.getTermFrequency(qTerm);
          assertEquals("Stopword has term frequency >0.", 0L, result);
        } else {
          assertNotEquals(this, this.referenceIndex.getTermFrequency(qTerm));
        }
      }
    }
  }

  /**
   * Test of getSTQueryObj method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
  public void testGetQueryObj_0args()
      throws Exception {
    initEnvironment(null, null);
    // stopwords are already removed
    final Collection<String> qTerms = TestIndexDataProvider.util
        .getQueryObj().getQueryTerms();
    for (final String term : qTerms) {
      final long result = this.referenceIndex.getTermFrequency(new ByteArray
          (term.getBytes("UTF-8")));
      assertNotEquals("Term frequency was 0 for search term.", 0L, result);
    }
  }

  /**
   * Test of getUniqueQueryString method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetUniqueQueryString()
      throws Exception {
    initEnvironment(null, null);
    final String result = TestIndexDataProvider.util.getUniqueQueryString();
    final Collection<ByteArray> qTerms =
        new QueryUtils(
            this.referenceIndex.getAnalyzer(),
            this.referenceIndex.getIndexReader(),
            this.referenceIndex.getDocumentFields()).getAllQueryTerms(result);
    final Collection<ByteArray> qTermsUnique = new HashSet<>(qTerms);
    assertEquals("Query string was not made of unique terms.",
        (long) qTerms.size(), (long) qTermsUnique.size());
  }

  /**
   * Test of getQueryString method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetQueryString_StringArr()
      throws Exception {
    initEnvironment(null, null);
    final String exStr = TestIndexDataProvider.util.getQueryString();
    final String[] exArr = WS_SPLIT.split(exStr);
    final Collection<String> exColl = Arrays.asList(exArr);

    final String resStr = TestIndexDataProvider.util.getQueryString(exArr);
    final String[] resArr = WS_SPLIT.split(resStr);
    final Collection<String> resColl = Arrays.asList(resArr);

    assertEquals("Query terms size differs.",
        (long) exArr.length, (long) resArr.length);
    assertEquals("Query terms size differs.",
        (long) exColl.size(), (long) resColl.size());
    assertTrue("Query terms content differs.", resColl.containsAll(exColl));
  }

  /**
   * Test of getSTQueryObj method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
  public void testGetQueryObj_StringArr()
      throws Exception {
    initEnvironment(null, null);
    final String exStr = TestIndexDataProvider.util.getQueryString();
    final Collection<ByteArray> oQueryTerms = new QueryUtils(
        this.referenceIndex.getAnalyzer(),
        this.referenceIndex.getIndexReader(),
        this.referenceIndex.getDocumentFields()).getAllQueryTerms(exStr);
    final Collection<String> oQueryTermsStr = new ArrayList<>(oQueryTerms.
        size());
    for (final ByteArray qTerm : oQueryTerms) {
      oQueryTermsStr.add(ByteArrayUtils.utf8ToString(qTerm));
    }
    // stopwords are already removed
    final Collection<String> qTerms = TestIndexDataProvider.util.getSTQueryObj
        (oQueryTermsStr.toArray(new String[oQueryTermsStr.size()]))
        .getQueryTerms();
    for (final String term : qTerms) {
      final long result =
          this.referenceIndex.getTermFrequency(new ByteArray(term.getBytes(
              "UTF-8")));
      assertNotEquals("Term frequency was 0 for search term.", 0L, result);
      assertTrue("Unknown term found.", oQueryTermsStr.contains(term));
    }

    qTerms.removeAll(oQueryTermsStr);
    if (!qTerms.isEmpty()) {
      final Collection<String> stopwords = this.referenceIndex.getStopwords();
      for (final String term : qTerms) {
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
  public void testGetRandomFields()
      throws Exception {
    initEnvironment(null, null);
    final Collection<String> result = TestIndexDataProvider.util
        .getRandomFields();
    final Iterator<String> fields =
        MultiFields.getFields(this.referenceIndex.getIndex().
            getReader()).iterator();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<String> fieldNames = new ArrayList<>();
    while (fields.hasNext()) {
      fieldNames.add(fields.next());
    }

    for (final String field : result) {
      assertTrue(
          "Unknown field. f=" + field,
          fieldNames.contains(field));
    }
  }

  /**
   * Test of getDocumentFields method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetActiveFieldNames()
      throws Exception {
    final Set<String> aFields = this.referenceIndex.util.getRandomFields();
    initEnvironment(aFields, null);
    final Collection<String> result = this.referenceIndex.getDocumentFields();

    assertEquals(
        "Number of fields differs.",
        aFields.size(), result.size());
    assertTrue(
        "Not all fields found.",
        result.containsAll(aFields));
    assertNotEquals(
        "Expected fields >0.",
        result.size(), 0);
  }

  /**
   * Test of getDocumentTermFrequencyMap method, of class
   * TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetDocumentTermFrequencyMap()
      throws Exception {
    initEnvironment(null, null);
    for (int i = 0; i < this.referenceIndex.getDocumentCount(); i++) {
      final DocumentModel docModel = this.referenceIndex.getDocumentModel(i);
      final Map<ByteArray, Long> dtfMap = TestIndexDataProvider.reference
          .getDocumentTermFrequencyMap(i);
      assertEquals(
          "Document model term list size != DocumentTermFrequencyMap.size().",
          dtfMap.size(), docModel.termFreqMap.size());
      assertTrue(
          "Document model term list != DocumentTermFrequencyMap keys.",
          dtfMap.keySet().containsAll(docModel.termFreqMap.keySet()));
      for (final Entry<ByteArray, Long> e : dtfMap.entrySet()) {
        assertEquals(
            "Frequency value mismatch.",
            e.getValue(),
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
  public void testGetTermList()
      throws Exception {
    initEnvironment(null, null);
    final Collection<ByteArray> result = TestIndexDataProvider.reference
        .getTermList();
    final Collection<ByteArray> tSet = new HashSet<>(result);

    assertEquals(
        "Overall term frequency and term list size differs.",
        this.referenceIndex.getTermFrequency(), result.size()
    );

    long overAllFreq = 0;
    for (final ByteArray term : tSet) {
      overAllFreq += this.referenceIndex.getTermFrequency(term);
    }

    assertEquals(
        "Calculated overall frequency != term list size.",
        overAllFreq, result.size());
  }

  /**
   * Test of getTermSet method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetTermSet()
      throws Exception {
    initEnvironment(null, null);
    final Collection<ByteArray> result = TestIndexDataProvider.reference
        .getTermSet();

    assertFalse("Empty terms set.", result.isEmpty());
    assertEquals(
        "Different count of unique terms reported.",
        this.referenceIndex.getUniqueTermsCount(), result.size());
    assertEquals(
        "Unique term count differs.", result.size(),
        this.referenceIndex.getUniqueTermsCount());
  }

  /**
   * Test of isInitialized method, of class TestIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testIsInitialized()
      throws Exception {
    initEnvironment(null, null);
    assertTrue("Index should be initialized.",
        TestIndexDataProvider.isInitialized());
  }

  /**
   * Test of getIndex method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetIndex() {
    this.referenceIndex.getIndex();
  }

  /**
   * Test of getDocumentTermSet method, of class TestIndexDataProvider.
   */
  @Test
  public void testGetDocumentTermSet() {
    final Iterator<Integer> docIdIt = this.referenceIndex
        .getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final Collection<ByteArray> result = TestIndexDataProvider.reference
          .getDocumentTermSet(docId, this.referenceIndex);
      final Collection<ByteArray> exp = TestIndexDataProvider.reference
          .getDocumentTermFrequencyMap(docId).keySet();
      assertEquals("Term count mismatch.", exp.size(), result.size());
      assertTrue("Term list content mismatch.", result.containsAll(exp));
    }
  }

  @Override
  protected Class<? extends IndexDataProvider> getInstanceClass() {
    return TestIndexDataProvider.class;
  }

  @Override
  protected IndexDataProvider createInstance(final String dataDir,
      final IndexReader reader, final Set<String> fields,
      final Set<String> stopwords)
      throws Exception {
    return new TestIndexDataProvider(TestIndexDataProvider.DEFAULT_INDEX_SIZE);
  }
}
