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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the TestIndex class.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TestIndexTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          TestIndexTest.class);

  /**
   * Temporary Lucene memory index.
   */
  private static TestIndex index;

  /**
   * Amount of test term-data to generate.
   */
  private static final int TEST_TERMDATA_AMOUNT = 10000;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    index = new TestIndex();
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());
  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static void tearDownClass() {
    index.dispose();
  }

  @Test
  public void testGetDocumentCount() {
    LOG.info("Test getDocumentCount");
    assertNotEquals("No documents found.", 0, index.getDocumentCount());
  }

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

  @Test
  public void testGetTermFrequency() {
    LOG.info("Test getTermFrequency");
    assertEquals("Term frequency and term list size differs.", index.
            getTermFrequency(), index.getTermList().size());
  }

  @Test
  public void testGetTermFrequency_term() {
    LOG.info("Test getTermFrequency_term");
    final Collection<BytesWrap> terms = index.getTermList();
    for (BytesWrap term : terms) {
      assertNotNull("Expected frequency value for term.", index.
              getTermFrequency(term));
      assertNotEquals("Expected frequency value >0 for term.", (Long) index.
              getTermFrequency(term), (Long) 0L);
    }
  }

  @Test
  public void testGetRelativeTermFrequency() {
    LOG.info("Test getRelativeTermFrequency");
    final Collection<BytesWrap> terms = index.getTermList();
    for (BytesWrap term : terms) {
      assertNotEquals("Expected frequency value >0 term.", index.
              getRelativeTermFrequency(term), 0);
    }
  }

  @Test
  public void testGetTargetFields() {
    LOG.info("Test getTargetFields");
    final int[] fieldState = index.getFieldState();
    int[] newFieldState = new int[fieldState.length];

    assertNotEquals("Expected fields >0.", index.getFields().length, 0);

    if (fieldState.length > 1) {
      // toggle some fields
      newFieldState = fieldState.clone();
      // ensure both states are not the same
      while (Arrays.equals(newFieldState, fieldState)) {
        for (int i = 0; i < fieldState.length; i++) {
          newFieldState[i] = RandomValue.getInteger(0, 1);
        }
      }

      index.setFieldState(newFieldState);
      assertNotEquals("Expected count lower than all fields.", index.
              getFields().length, fieldState.length);
    } else {
      LOG.warn("Skip test section. Field count == 1.");
    }
  }

  @Test
  public void testGetTermsIterator() {
    LOG.info("Test getTermsIterator");
    final int termsCount = index.getTermSet().size();
    final Iterator<BytesWrap> termsIt = index.getTermsIterator();
    int itCount = 0;
    while (termsIt.hasNext()) {
      itCount++;
      termsIt.next();
    }
    assertEquals("Term count and iteration count differs.", itCount,
            termsCount);
  }

  @Test
  public void testGetTermsSource() {
    LOG.info("Test getTermsSource");
    final int termsCount = index.getTermSet().size();
    Processing p = new Processing();
    p.setSource(index.getTermsSource());
    assertEquals("Not all items provided by source or processed by target.",
            (Long) (long) termsCount, p.debugTestSource());
  }

  @Test
  public void testGetDocumentIdSource() {
    LOG.info("Test getDocumentIdSource");
    final long docCount = index.getDocumentCount();
    Processing p = new Processing();
    p.setSource(index.getDocumentIdSource());
    assertEquals("Not all items provided by source or processed by target.",
            (Long) (long) docCount, p.debugTestSource());
  }

  @Test
  public void testGetUniqueTermsCount() {
    LOG.info("Test getUniqueTermsCount");
    final int termsCount = index.getTermSet().size();
    assertEquals(index.getUniqueTermsCount(), termsCount);
  }

  @Test
  public void testSetTermData() {
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

  @Test
  public void testGetTermData_4args() {
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

  @Test
  public void testGetTermData_3args() {
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
    for (Entry<BytesWrap, Object> dataEntry : index.getTermData(prefix, docId,
            key).entrySet()) {
      final Integer expValue = expResult.get(dataEntry.getKey());
      final Integer value = (Integer) dataEntry.getValue();
      assertEquals(expValue, value);
      retrievalCount++;
    }

    assertEquals("Not all set termData was retrieved.", TEST_TERMDATA_AMOUNT,
            retrievalCount);
  }

  @Test
  public void testHasDocumentModel() {
    LOG.info("Test hasDocumentModel");
    for (int i = 0; i < index.getDocumentCount(); i++) {
      assertTrue("Document (" + i + ") not found.", index.hasDocument(i));
    }
  }

  @Test
  public void testGetDocumentModel() throws Exception {
    LOG.info("Test getDocumentModel");
    final int indexTermFreq = (int) index.getTermFrequency();
    int countedIndexTermFreq = 0;

    for (int i = 0; i < index.getDocumentCount(); i++) {
      final DocumentModel docModel = index.getDocumentModel(i);
      assertNotNull("Document model was null.", docModel);
      countedIndexTermFreq += docModel.termFrequency;
      if (docModel.id > index.getDocumentCount() || docModel.id < 0) {
        fail("Illegal document id " + docModel.id);
      }
      assertNotNull("Term frequency map was null.", docModel.termFreqMap);
      assertNotEquals("Term frequency map was empty.", docModel.termFreqMap.
              size(), 0);
      for (Entry<BytesWrap, Long> tfEntry : docModel.termFreqMap.entrySet()) {
        assertNotEquals("Term frequency entry was <=0", (Long) tfEntry.
                getValue(), (Long) 0L);
        assertTrue("Term contains mismatch.", docModel.contains(tfEntry.
                getKey()));
      }
    }

    assertEquals("Index term frequency differs from overall frequency "
            + "calculated based on document models.", indexTermFreq,
            countedIndexTermFreq);
  }

  @Test
  public void testDocumentContains() {
    LOG.info("Test documentContains");
    for (int i = 0; i < index.getDocumentCount(); i++) {
      final DocumentModel docModel = index.getDocumentModel(i);
      for (BytesWrap bw : docModel.termFreqMap.keySet()) {
        assertTrue("Document contains term mismatch.", index.documentContains(
                i, bw));
      }
    }
  }

  @Test
  public void testGetFieldState() {
    LOG.info("Test getFieldState");
    final int fieldCount = index.getFields().length;
    assertEquals("Not all fields found in field state array.", fieldCount,
            index.getFieldState().length);
  }

  @Test
  public void testSetFieldState() throws Exception {
    LOG.info("Test setFieldState");
    final int[] fieldState = index.getFieldState();
    int[] newFieldState = new int[fieldState.length];

    final TestIndex otherIndex = new TestIndex();
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());

    // turn off all fields
    Arrays.fill(newFieldState, 0);
    index.setFieldState(newFieldState);
    assertTrue("There should be no terms.", index.getTermList().isEmpty());
    assertTrue("There should be no terms.", index.getTermSet().isEmpty());
    for (int i = 0; i < index.getDocumentCount(); i++) {
      assertTrue("TermFreqMap should be empty.", index.
              getDocumentTermFrequencyMap(i).isEmpty());
      assertTrue("Document termSet should be empty.", index.
              getDocumentTermSet(i).isEmpty());
    }
    assertEquals("Term frequency should be zero.", 0L, index.
            getTermFrequency());
    assertTrue("There should be no terms.", index.getTermList().isEmpty());
    assertTrue("There should be no terms.", index.getTermSet().isEmpty());
    assertFalse("Iterator should have no terms.", index.getTermsIterator().
            hasNext());
    assertEquals("Term count should be zero.", 0L, index.getUniqueTermsCount());

    // test other instance state
    assertFalse("There should be terms.", otherIndex.getTermList().isEmpty());
    assertFalse("There should be terms.", otherIndex.getTermSet().isEmpty());
    for (int i = 0; i < otherIndex.getDocumentCount(); i++) {
      assertFalse("TermFreqMap should not be empty.", otherIndex.
              getDocumentTermFrequencyMap(i).isEmpty());
      assertFalse("Document termSet should not be empty.", otherIndex.
              getDocumentTermSet(i).isEmpty());
    }
    assertNotEquals("Term frequency should not be zero.", 0L, otherIndex.
            getTermFrequency());
    assertFalse("There should be terms.", otherIndex.getTermList().isEmpty());
    assertFalse("There should be terms.", otherIndex.getTermSet().isEmpty());
    assertTrue("Iterator should have terms.", otherIndex.getTermsIterator().
            hasNext());
    assertNotEquals("Term count should not be zero.", 0L, otherIndex.
            getUniqueTermsCount());

    if (fieldState.length > 1) {
      // toggle some fields
      newFieldState = fieldState.clone();
      // ensure both states are not the same
      while (Arrays.equals(newFieldState, fieldState)) {
        for (int i = 0; i < fieldState.length; i++) {
          newFieldState[i] = RandomValue.getInteger(0, 1);
        }
      }

      index.setFieldState(fieldState);
      otherIndex.setFieldState(newFieldState);

      assertFalse("Term lists should not be equal.", index.getTermList().
              size() == otherIndex.getTermList().size());
      assertNotEquals("Term frequency should not be equal.", index.
              getTermFrequency(), otherIndex.getTermFrequency());
    } else {
      LOG.warn("Skip test section. Field count == 1.");
    }
    index.setFieldState(fieldState);
  }
}
