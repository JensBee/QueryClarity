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
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.concurrent.processing.Processing;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared test functions for {@link IndexDataProvider} implementing classes.
 *
 *
 */
final class IndexDataProviderTestMethods {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          IndexDataProviderTestMethods.class);

  /**
   * Amount of test term-data to generate.
   */
  private static final int TEST_TERMDATA_AMOUNT = 10000;

  /**
   * Private empty constructor for utility class.
   */
  private IndexDataProviderTestMethods() {
    // empty
  }

  /**
   * Test method for getTermFrequency method. Using stopwords.
   */
  protected static void testGetTermFrequency_0args__stopped(
          final TestIndexDataProvider index, final IndexDataProvider instance) {
    LOG.info("Test getTermFrequency 0arg (stopped)");
    final long unfilteredTf = index.getTermFrequency();

    // check with stopwords
    IndexTestUtil.setRandomStopWords(index);
    final long filteredTf = index.getTermFrequency();
    assertEquals("Term frequency differs. plain=" + unfilteredTf + " filter="
            + filteredTf + ".", index.getTermFrequency(),
            instance.getTermFrequency());

    assertNotEquals(
            "TF using stop-words should be lower than without. filter="
            + filteredTf + " plain=" + unfilteredTf + ".",
            filteredTf, unfilteredTf);
  }

  /**
   * Test method for getTermFrequency method.
   */
  protected static void testGetTermFrequency_0args__randFields(
          final TestIndexDataProvider index, final IndexDataProvider instance) {
    LOG.info("Test getTermFrequency 0arg (random fields)");
    IndexTestUtil.setRandomFields(index);
    assertEquals("Term frequency differs.", index.getTermFrequency(),
            instance.getTermFrequency());
  }

  /**
   * Test method for getTermFrequency method.
   */
  protected static void testGetTermFrequency_0args(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) {
    LOG.info("Test getTermFrequency 0arg");
    // plain check against test index
    assertEquals("Term frequency differs.", index.getTermFrequency(),
            instance.getTermFrequency());
  }

  /**
   * Test method for getTermFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetTermFrequency_ByteArray(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    final Iterator<ByteArray> idxTermsIt = index.getTermsIterator();
    while (idxTermsIt.hasNext()) {
      final ByteArray idxTerm = idxTermsIt.next();
      assertEquals("Term frequency differs (stopped: " + excludeStopwords
              + "). term=" + idxTerm, index.
              getTermFrequency(idxTerm), instance.getTermFrequency(idxTerm));
      if (excludeStopwords) {
        assertFalse("Stopword found in term list.", stopwords.contains(
                idxTerm));
      }
    }
  }

  /**
   * Test of getTermFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetTermFrequency_ByteArray__stopped(
          final TestIndexDataProvider index, final IndexDataProvider instance)
          throws
          Exception {
    LOG.info("Test getTermFrequency [term] (stopped)");
    // check with stopwords
    IndexTestUtil.setRandomStopWords(index);
    _testGetTermFrequency_ByteArray(index, instance);
  }

  /**
   * Test of getTermFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetTermFrequency_ByteArray__randFields(
          final TestIndexDataProvider index, final IndexDataProvider instance)
          throws Exception {
    LOG.info("Test getTermFrequency [term] (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testGetTermFrequency_ByteArray(index, instance);
  }

  /**
   * Test of getTermFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetTermFrequency_ByteArray(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getTermFrequency [term]");
    _testGetTermFrequency_ByteArray(index, instance);
  }

  /**
   * Test method for getRelativeTermFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetRelativeTermFrequency(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    Iterator<ByteArray> idxTermsIt = index.getTermsIterator();

    while (idxTermsIt.hasNext()) {
      final ByteArray idxTerm = idxTermsIt.next();
      assertEquals("Relative term frequency differs (stopped: "
              + excludeStopwords + "). term=" + idxTerm, index.
              getRelativeTermFrequency(idxTerm), instance.
              getRelativeTermFrequency(idxTerm), 0);
      if (excludeStopwords) {
        assertFalse("Stopword found in term list.", stopwords.contains(
                idxTerm));
      }
    }
  }

  /**
   * Test of getRelativeTermFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetRelativeTermFrequency(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getRelativeTermFrequency");
    _testGetRelativeTermFrequency(index, instance);
  }

  /**
   * Test of getRelativeTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetRelativeTermFrequency__stopped(
          final TestIndexDataProvider index, final IndexDataProvider instance)
          throws
          Exception {
    LOG.info("Test getRelativeTermFrequency (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    _testGetRelativeTermFrequency(index, instance);
  }

  /**
   * Test of getRelativeTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetRelativeTermFrequency__randFields(
          final TestIndexDataProvider index, final IndexDataProvider instance)
          throws
          Exception {
    LOG.info("Test getRelativeTermFrequency (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testGetRelativeTermFrequency(index, instance);
  }

  /**
   * Test method for getTermsIterator method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetTermsIterator(final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    Iterator<ByteArray> result = instance.getTermsIterator();

    int iterations = 0;
    while (result.hasNext()) {
      iterations++;
      if (excludeStopwords) {
        assertFalse("Found stopword.", stopwords.contains(result.next()));
      } else {
        result.next();
      }
    }

    assertEquals("Not all terms found while iterating.", instance.
            getUniqueTermsCount(), iterations);
    assertEquals("Different values for unique terms reported.", index.
            getUniqueTermsCount(), instance.getUniqueTermsCount());
  }

  /**
   * Test of getTermsIterator method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_DEFAULT_ENCODING")
  protected static void testGetTermsIterator(final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getTermsIterator");
    _testGetTermsIterator(index, instance);
  }

  /**
   * Test of getTermsIterator method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_DEFAULT_ENCODING")
  protected static void testGetTermsIterator__stopped(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getTermsIterator");
    IndexTestUtil.setRandomStopWords(index);
    _testGetTermsIterator(index, instance);
  }

  /**
   * Test of getTermsIterator method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_DEFAULT_ENCODING")
  protected static void testGetTermsIterator__randField(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getTermsIterator (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testGetTermsIterator(index, instance);
  }

  /**
   * Test of getDocumentCount method.
   */
  protected static void testGetDocumentCount(final TestIndexDataProvider index,
          final IndexDataProvider instance) {
    LOG.info("Test getDocumentCount");
    final long expResult = index.getDocumentCount();
    final long result = instance.getDocumentCount();
    assertEquals("Different number of documents reported.", expResult, result);
  }

  /**
   * Test method for getDocumentModel method, of class
   * CachedIndexDataProvider.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetDocumentModel(final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final Integer docId = docIdIt.next();
      final DocumentModel iDocModel = instance.getDocumentModel(docId);
      final DocumentModel eDocModel = index.getDocumentModel(docId);

      assertTrue("Equals failed (stopped: " + excludeStopwords
              + ") for docId=" + docId, eDocModel.equals(iDocModel));

      if (excludeStopwords) {
        for (ByteArray term : iDocModel.termFreqMap.keySet()) {
          assertFalse("Found stopword in model.", stopwords.contains(term));
        }
      }
    }
  }

  /**
   * Test of getDocumentModel method, of class CachedIndexDataProvider.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetDocumentModel(final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getDocumentModel");
    _testGetDocumentModel(index, instance);
  }

  /**
   * Test of getDocumentModel method, of class CachedIndexDataProvider. Using
   * stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetDocumentModel__stopped(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getDocumentModel (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    _testGetDocumentModel(index, instance);
  }

  /**
   * Test of getDocumentModel method, of class CachedIndexDataProvider. Using
   * stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetDocumentModel__randField(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getDocumentModel (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testGetDocumentModel(index, instance);
  }

  /**
   * Test of getDocumentIdIterator method.
   */
  protected static void testGetDocumentIdIterator(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) {
    LOG.info("Test getDocumentIdIterator");
    final long docCount = index.getDocumentCount();
    long docCountIt = 0;
    final Iterator<Integer> result = instance.getDocumentIdIterator();
    while (result.hasNext()) {
      docCountIt++;
      result.next();
    }
    assertEquals(docCount, docCountIt);
  }

  /**
   * Test of getDocumentIdSource method.
   */
  @SuppressWarnings("UnnecessaryUnboxing")
  protected static void testGetDocumentIdSource(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) {
    LOG.info("Test getDocumentIdSource");
    final long docCount = index.getDocumentCount();
    Processing p = new Processing();
    p.setSource(instance.getDocumentIdSource());
    assertEquals("Not all items provided by source or processed by target.",
            docCount, p.debugTestSource().longValue());
  }

  /**
   * Test of getUniqueTermsCount method.
   */
  protected static void testGetUniqueTermsCount(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) {
    LOG.info("Test getUniqueTermsCount");

    assertEquals("Unique term count values are different.",
            index.getTermSet().size(), instance.getUniqueTermsCount());
  }

  /**
   * Test of getUniqueTermsCount method. Using stopwords.
   */
  protected static void testGetUniqueTermsCount__stopped(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) {
    LOG.info("Test getUniqueTermsCount (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    assertEquals("Unique term count values are different.",
            index.getTermSet().size(), instance.getUniqueTermsCount());
  }

  /**
   * Test of getUniqueTermsCount method. Using stopwords.
   */
  protected static void testGetUniqueTermsCount__randFields(
          final TestIndexDataProvider index, final IndexDataProvider instance) {
    LOG.info("Test getUniqueTermsCount (random fields)");
    IndexTestUtil.setRandomFields(index);
    assertEquals("Unique term count values are different.",
            index.getTermSet().size(), instance.getUniqueTermsCount());
  }

  /**
   * Test of hasDocument method.
   */
  protected static void testHasDocument(final TestIndexDataProvider index,
          final IndexDataProvider instance) {
    LOG.info("Test hasDocument");

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      assertTrue("Document not found.", instance.hasDocument(docIdIt.next()));
    }

    assertFalse("Document should not be found.", instance.hasDocument(-1));
    assertFalse("Document should not be found.", instance.hasDocument(
            (int) index.getDocumentCount()));
  }

  /**
   * Test method for documentContains method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testDocumentContains(final IndexDataProvider instance)
          throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    for (int i = 0; i < instance.getDocumentCount(); i++) {
      final DocumentModel docModel = instance.getDocumentModel(i);
      for (ByteArray byteArray : docModel.termFreqMap.keySet()) {
        assertTrue("Document contains term mismatch (stopped: "
                + excludeStopwords + ").", instance.documentContains(
                        i, byteArray));
        if (excludeStopwords) {
          assertFalse("Found stopword.", stopwords.contains(byteArray));
        }
      }
    }
  }

  /**
   * Test of documentContains method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testDocumentContains(final IndexDataProvider instance)
          throws Exception {
    LOG.info("Test documentContains");
    _testDocumentContains(instance);
  }

  /**
   * Test of documentContains method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testDocumentContains__stopped(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test documentContains (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    _testDocumentContains(instance);
  }

  /**
   * Test of documentContains method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testDocumentContains__randField(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test documentContains (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testDocumentContains(instance);
  }

  /**
   * Test of getTermsSource method.
   */
  @SuppressWarnings("UnnecessaryUnboxing")
  protected static void testGetTermsSource(final TestIndexDataProvider index,
          final IndexDataProvider instance) {
    LOG.info("Test getTermsSource");
    final int termsCount = new HashSet<>(index.getTermList()).size();
    Processing p = new Processing();
    p.setSource(instance.getTermsSource());
    assertEquals("Not all items provided by source or processed by target.",
            termsCount, p.debugTestSource().longValue());
  }

  /**
   * Test of setTermData method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testSetTermData(final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    throw new UnsupportedOperationException("BROKEN!");
//    LOG.info("Test setTermData");
//    final String prefix = "test";
//    final String key = "testKey";
//    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
//    Processing p;
//
//    // part one - all random data
//    testData = IndexTestUtil.generateTermData(index, TEST_TERMDATA_AMOUNT);
//    p = new Processing(new IndexTestUtil.IndexTermDataTarget(
//            new CollectionSource<>(testData), instance, prefix));
//    p.process();
//
//    instance.clearTermData();
//
//    // part two - all data on one key
//    testData = IndexTestUtil.generateTermData(instance, key,
//            TEST_TERMDATA_AMOUNT);
//    p = new Processing(new IndexTestUtil.IndexTermDataTarget(
//            new CollectionSource<>(testData), instance, prefix));
//    p.process();
  }

  /**
   * Test of getTermData method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetTermData_4args(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    throw new UnsupportedOperationException("BROKEN!");
//    LOG.info("Test getTermData_4args");
//    final String prefix = "test4a";
//    final String key = "testKey4a";
//    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
//    Processing p;
//    int retrievalCount;
//
//    // part one - all random data
//    // create test data
//    testData = IndexTestUtil.generateTermData(index, TEST_TERMDATA_AMOUNT);
//    p = new Processing(new IndexTestUtil.IndexTermDataTarget(
//            new CollectionSource<>(testData), instance, prefix));
//    p.process();
//
//    // retrieve test data
//    retrievalCount = 0;
//    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> t4 : testData) {
//      assertEquals("Pass 1: Retrieved termData mismatch. prefix=" + prefix
//              + " docId=" + t4.a + " term=" + t4.b + " key=" + t4.c, instance.
//              getTermData(prefix, t4.a, t4.b, t4.c), t4.d);
//      retrievalCount++;
//    }
//
//    assertEquals("Pass 1: Not all set termData was retrieved.",
//            TEST_TERMDATA_AMOUNT, retrievalCount);
//
//    instance.clearTermData();
//
//    // part two - all data on one key
//    // create test data
//    testData = IndexTestUtil.generateTermData(index, key,
//            TEST_TERMDATA_AMOUNT);
//    p = new Processing(new IndexTestUtil.IndexTermDataTarget(
//            new CollectionSource<>(testData), instance, prefix));
//    p.process();
//
//    // retrieve test data
//    retrievalCount = 0;
//    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> t4 : testData) {
//      assertEquals("Pass 2: Retrieved termData mismatch.", instance.
//              getTermData(
//                      prefix, t4.a, t4.b, key), t4.d);
//      retrievalCount++;
//    }
//
//    assertEquals("Pass 2: Not all set termData was retrieved.",
//            TEST_TERMDATA_AMOUNT, retrievalCount);
  }

  /**
   * Test of getTermData method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetTermData_3args(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    throw new UnsupportedOperationException("BROKEN!");
//    LOG.info("Test getTermData_3args");
//
//    final String prefix = "test3a";
//    final String key = "testKey3a";
//    final int docId = RandomValue.getInteger(0, (int) index.getDocumentCount()
//            - 1);
//    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
//    Processing p;
//    int retrievalCount;
//
//    // create test data
//    testData = IndexTestUtil.generateTermData(index, docId, key,
//            TEST_TERMDATA_AMOUNT);
//    p = new Processing(new IndexTestUtil.IndexTermDataTarget(
//            new CollectionSource<>(testData), instance, prefix));
//    p.process();
//
//    Map<ByteArray, Integer> expResult = new HashMap<>(testData.size());
//    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> t4 : testData) {
//      expResult.put(t4.b, t4.d);
//    }
//
//    // retrieve test data
//    retrievalCount = 0;
//    for (Map.Entry<ByteArray, Object> dataEntry : instance.getTermData(prefix,
//            docId, key).entrySet()) {
//      final Integer expValue = expResult.get(dataEntry.getKey());
//      final Integer value = (Integer) dataEntry.getValue();
//      assertEquals(expValue, value);
//      retrievalCount++;
//    }
//
//    assertEquals("Not all set termData was retrieved.", TEST_TERMDATA_AMOUNT,
//            retrievalCount);
  }

  /**
   * Test of clearTermData method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testClearTermData(final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    throw new UnsupportedOperationException("BROKEN!");
//    LOG.info("Test clearTermData");
//    final String prefix = "testClear";
//    instance.registerPrefix(prefix);
//    Processing p;
//    Collection<Tuple.Tuple4<Integer, ByteArray, String, Integer>> testData;
//    testData = IndexTestUtil.generateTermData(index, TEST_TERMDATA_AMOUNT);
//    p = new Processing(new IndexTestUtil.IndexTermDataTarget(
//            new CollectionSource<>(testData), instance, prefix));
//    p.process();
//
//    instance.clearTermData();
//
////    try {
//    for (Tuple.Tuple4<Integer, ByteArray, String, Integer> t4 : testData) {
//      assertEquals("Expected empty result (null).", null, instance.
//              getTermData(prefix, t4.a, t4.b, t4.c));
//    }
////      fail("Expected an exception to be thrown.");
////    } catch (IllegalArgumentException ex) {
////    }
  }

  /**
   * Test method for getDocumentsTermSet method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetDocumentsTermSet(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    final int docAmount = RandomValue.getInteger(2, (int) index.
            getDocumentCount() - 1);
    Collection<Integer> docIds = new HashSet<>(docAmount);
    for (int i = 0; i < docAmount;) {
      if (docIds.add(RandomValue.getInteger(0, RandomValue.getInteger(2,
              (int) index.getDocumentCount() - 1)))) {
        i++;
      }
    }
    Collection<ByteArray> expResult = index.getDocumentsTermSet(docIds);
    Collection<ByteArray> result = instance.getDocumentsTermSet(docIds);

    assertEquals("Not the same amount of terms retrieved (stopped: "
            + excludeStopwords + ").", expResult.size(),
            result.size());
    assertTrue("Not all terms retrieved (stopped: "
            + excludeStopwords + ").", expResult.containsAll(result));

    if (excludeStopwords) {
      for (ByteArray term : result) {
        assertFalse("Stopword found in term list.", stopwords.contains(term));
      }
    }
  }

  /**
   * Test of getDocumentsTermSet method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetDocumentsTermSet(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getDocumentsTermSet");
    _testGetDocumentsTermSet(index, instance);
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetDocumentsTermSet__stopped(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getDocumentsTermSet (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    _testGetDocumentsTermSet(index, instance);
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetDocumentsTermSet__randField(
          final TestIndexDataProvider index, final IndexDataProvider instance)
          throws
          Exception {
    LOG.info("Test getDocumentsTermSet (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testGetDocumentsTermSet(index, instance);
  }

  /**
   * Test of registerPrefix method.
   */
  protected static void testRegisterPrefix(final IndexDataProvider instance) {
    throw new UnsupportedOperationException("BROKEN!");
//    LOG.info("Test registerPrefix");
//    instance.registerPrefix("foo");
  }

  /**
   * Test of fieldsChanged method.
   */
  protected static void testFieldsChanged(final TestIndexDataProvider index,
          final IndexDataProvider instance) {
    LOG.info("Test fieldsChanged");

    final Collection<String> exp = IndexTestUtil.setRandomFields(index);
    final Collection<String> res = instance.testGetFieldNames();

    assertEquals("Not all fields received.", exp.size(), res.size());
    assertTrue("Not all fields in result set.", res.containsAll(exp));
  }

  /**
   * Test method for getDocumentFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetDocumentFrequency(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    for (ByteArray term : index.getTermSet()) {
      assertEquals("Document frequency mismatch (stopped: "
              + excludeStopwords + ") (" + ByteArrayUtil.
              utf8ToString(term) + ").", index.getDocumentFrequency(term),
              instance.getDocumentFrequency(term));
      if (excludeStopwords) {
        assertFalse("Found stopword in term list.", stopwords.contains(term));
      }
    }
  }

  /**
   * Test of getDocumentFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetDocumentFrequency(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getDocumentFrequency");
    _testGetDocumentFrequency(index, instance);
  }

  /**
   * Test of getDocumentFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetDocumentFrequency__stopped(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test getDocumentFrequency (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    _testGetDocumentFrequency(index, instance);
  }

  /**
   * Test of getDocumentFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void testGetDocumentFrequency__randField(
          final TestIndexDataProvider index, final IndexDataProvider instance)
          throws
          Exception {
    LOG.info("Test getDocumentFrequency (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testGetDocumentFrequency(index, instance);
  }

  /**
   * Test of warmUp method.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  protected static void testWarmUp(final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test warmUp");
    instance.warmUp();
  }

  /**
   * Test of wordsChanged method.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  protected static void testWordsChanged(final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    LOG.info("Test wordsChanged");
    final Collection<String> exp = IndexTestUtil.setRandomStopWords(index);
    final Collection<String> res = instance.testGetStopwords();

    assertEquals("Not all stopwords received.", exp.size(), res.size());
    assertTrue("Not all stopwords in result set.", res.containsAll(exp));
  }
}
