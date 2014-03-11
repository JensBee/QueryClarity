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
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.Tuple;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared test functions for {@link IndexDataProvider} implementing classes.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
final class IndexDataProviderTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          IndexDataProviderTest.class);

  /**
   * Amount of test term-data to generate.
   */
  private static final int TEST_TERMDATA_AMOUNT = 10000;

  /**
   * Test of getTermFrequency method.
   */
  protected static void testGetTermFrequency_0args(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getTermFrequency 0arg");
    assertEquals("Term frequency differs.", index.getTermFrequency(),
            instance.getTermFrequency());
  }

  /**
   * Test of getTermFrequency method.
   */
  protected static void testGetTermFrequency_BytesWrap(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getTermFrequency 1arg");
    final Iterator<BytesWrap> idxTermsIt = index.getTermsIterator();
    while (idxTermsIt.hasNext()) {
      final BytesWrap idxTerm = idxTermsIt.next();
      assertEquals("Term frequency differs. term=" + idxTerm, index.
              getTermFrequency(idxTerm), instance.getTermFrequency(idxTerm));
    }
  }

  /**
   * Test of getRelativeTermFrequency method.
   */
  protected static void testGetRelativeTermFrequency(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getRelativeTermFrequency");
    final Iterator<BytesWrap> idxTermsIt = index.getTermsIterator();
    while (idxTermsIt.hasNext()) {
      final BytesWrap idxTerm = idxTermsIt.next();
      assertEquals("Relative term frequency differs. term=" + idxTerm, index.
              getRelativeTermFrequency(idxTerm), instance.
              getRelativeTermFrequency(idxTerm), 0);
    }
  }

  /**
   * Test of getTermsIterator method.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_DEFAULT_ENCODING")
  protected static void testGetTermsIterator(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getTermsIterator");
    Iterator<BytesWrap> result = instance.getTermsIterator();
    int iterations = 0;
    while (result.hasNext()) {
      iterations++;
      result.next();
    }
    assertEquals("Not all terms found while iterating.", instance.
            getUniqueTermsCount(), iterations);
    assertEquals("Different values for unique terms reported.", index.
            getUniqueTermsCount(), instance.getUniqueTermsCount());
  }

  /**
   * Test of getDocumentCount method.
   */
  protected static void testGetDocumentCount(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getDocumentCount");
    final long expResult = index.getDocumentCount();
    final long result = instance.getDocumentCount();
    assertEquals("Different number of documents reported.", expResult, result);
  }

  /**
   * Test of getDocumentModel method, of class CachedIndexDataProvider.
   */
  protected static void testGetDocumentModel(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getDocumentModel");
    Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final Integer docId = docIdIt.next();
      final DocumentModel iDocModel = instance.getDocumentModel(docId);
      final DocumentModel eDocModel = index.getDocumentModel(docId);

      assertTrue("Equals failed for docId=" + docId, eDocModel.equals(
              iDocModel));
    }
  }

  /**
   * Test of getDocumentIdIterator method.
   */
  protected static void testGetDocumentIdIterator(final TestIndex index,
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
  protected static void testGetDocumentIdSource(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getDocumentIdSource");
    final long docCount = index.getDocumentCount();
    Processing p = new Processing();
    p.setSource(index.getDocumentIdSource());
    assertEquals("Not all items provided by source or processed by target.",
            (Long) (long) docCount, p.debugTestSource());
  }

  /**
   * Test of getUniqueTermsCount method.
   */
  protected static void testGetUniqueTermsCount(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getUniqueTermsCount");

    long result = instance.getUniqueTermsCount();
    assertEquals("Unique term count values are different.",
            index.getTermSet().size(), result);
  }

  /**
   * Test of hasDocument method.
   */
  protected static void testHasDocument(final TestIndex index,
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
   * Test of documentContains method.
   */
  protected static void testDocumentContains(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test documentContains");
    for (int i = 0; i < index.getDocumentCount(); i++) {
      final DocumentModel docModel = index.getDocumentModel(i);
      for (BytesWrap bw : docModel.termFreqMap.keySet()) {
        assertTrue("Document contains term mismatch.", index.documentContains(
                i, bw));
      }
    }
  }

  /**
   * Test of getTermsSource method.
   */
  protected static void testGetTermsSource(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getTermsSource");
    final int termsCount = new HashSet<>(index.getTermList()).size();
    Processing p = new Processing();
    p.setSource(index.getTermsSource());
    assertEquals("Not all items provided by source or processed by target.",
            (Long) (long) termsCount, p.debugTestSource());
  }

  /**
   * Test of setTermData method.
   */
  protected static void testSetTermData(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test setTermData");
    final String prefix = "test";
    final String key = "testKey";
    Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>> testData;
    Processing p;

    // part one - all random data
    testData = IndexTestUtils.generateTermData(index, TEST_TERMDATA_AMOUNT);
    p = new Processing(new IndexTestUtils.IndexTermDataTarget(
            new CollectionSource<>(testData), index, prefix));
    p.process();

    index.clearTermData();

    // part two - all data on one key
    testData = IndexTestUtils.generateTermData(index, key,
            TEST_TERMDATA_AMOUNT);
    p = new Processing(new IndexTestUtils.IndexTermDataTarget(
            new CollectionSource<>(testData), index, prefix));
    p.process();
  }

  /**
   * Test of getTermData method.
   */
  protected static void testGetTermData_4args(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getTermData_4args");
    final String prefix = "test4a";
    final String key = "testKey4a";
    Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>> testData;
    Processing p;
    int retrievalCount;

    // part one - all random data
    // create test data
    testData = IndexTestUtils.generateTermData(index, TEST_TERMDATA_AMOUNT);
    p = new Processing(new IndexTestUtils.IndexTermDataTarget(
            new CollectionSource<>(testData), index, prefix));
    p.process();

    // retrieve test data
    retrievalCount = 0;
    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> t4 : testData) {
      assertEquals("Pass 1: Retrieved termData mismatch. prefix=" + prefix
              + " docId=" + t4.a + " term=" + t4.b + " key=" + t4.c, index.
              getTermData(prefix, t4.a, t4.b, t4.c), t4.d);
      retrievalCount++;
    }

    assertEquals("Pass 1: Not all set termData was retrieved.",
            TEST_TERMDATA_AMOUNT, retrievalCount);

    index.clearTermData();

    // part two - all data on one key
    // create test data
    testData = IndexTestUtils.generateTermData(index, key,
            TEST_TERMDATA_AMOUNT);
    p = new Processing(new IndexTestUtils.IndexTermDataTarget(
            new CollectionSource<>(testData), index, prefix));
    p.process();

    // retrieve test data
    retrievalCount = 0;
    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> t4 : testData) {
      assertEquals("Pass 2: Retrieved termData mismatch.", index.getTermData(
              prefix, t4.a, t4.b, key), t4.d);
      retrievalCount++;
    }

    assertEquals("Pass 2: Not all set termData was retrieved.",
            TEST_TERMDATA_AMOUNT, retrievalCount);
  }

  /**
   * Test of getTermData method.
   */
  protected static void testGetTermData_3args(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getTermData_3args");

    final String prefix = "test3a";
    final String key = "testKey3a";
    final int docId = RandomValue.getInteger(0, (int) index.getDocumentCount()
            - 1);
    Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>> testData;
    Processing p;
    int retrievalCount;

    // create test data
    testData = IndexTestUtils.generateTermData(index, docId, key,
            TEST_TERMDATA_AMOUNT);
    p = new Processing(new IndexTestUtils.IndexTermDataTarget(
            new CollectionSource<>(testData), index, prefix));
    p.process();

    Map<BytesWrap, Integer> expResult = new HashMap(testData.size());
    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> t4 : testData) {
      expResult.put(t4.b, t4.d);
    }

    // retrieve test data
    retrievalCount = 0;
    for (Map.Entry<BytesWrap, Object> dataEntry : index.getTermData(prefix,
            docId,
            key).entrySet()) {
      final Integer expValue = expResult.get(dataEntry.getKey());
      final Integer value = (Integer) dataEntry.getValue();
      assertEquals(expValue, value);
      retrievalCount++;
    }

    assertEquals("Not all set termData was retrieved.", TEST_TERMDATA_AMOUNT,
            retrievalCount);
  }

  /**
   * Test of getProperty method.
   */
  protected static void testGetProperty_3args(final TestIndex index,
          final IndexDataProvider instance) {
    LOG.info("Test getProperty 3arg");
    final String value = "testDefaultValue";
    assertEquals("Default property value not returned.", value, instance.
            getProperty("foo", "bar", value));
  }
}
