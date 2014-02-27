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
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.util.Tuple;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.BytesRef;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
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

  private static TestIndex index;

  @BeforeClass
  public static void setUpClass() throws IOException, ParseException {
    index = new TestIndex();
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());
  }

  @Test
  public void testGetDocumentCount() {
    LOG.info("Test getDocumentCount");
    assertEquals("Wrong document count reported.", index.getDocumentCount(),
            TestIndex.test_getDocuments().size());
  }

  @Test
  public void testGetDocumentIdIterator() {
    LOG.info("Test getDocumentIdIterator");
    final List<Integer> docIdList = new ArrayList((int) index.
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
    assertNotEquals("Expected fields >0.", index.getTargetFields().length, 0);
  }

  @Test
  public void testGetTermsIterator() {
    LOG.info("Test getTermsIterator");
    final int termsCount = new HashSet<>(index.getTermList()).size();
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
    final int termsCount = new HashSet<>(index.getTermList()).size();
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
    final int termsCount = new HashSet<>(index.getTermList()).size();
    assertEquals(index.getUniqueTermsCount(), termsCount);
  }

  @Test
  public void testSetTermData() {
    LOG.info("Test setTermData");
    final String prefix = "test";
    final String key = "testKey";
    final int amount = 5000;
    Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>> testData;
    Processing p;

    // part one - all random data
    testData = IndexTestUtils.generateTermData(index, amount);
    p = new Processing(new IndexTestUtils.IndexTermDataTarget(
            new CollectionSource<>(testData), index, prefix));
    p.process();

    index.reset();

    // part two - all data on one key
    testData = IndexTestUtils.generateTermData(index, key, amount);
    p = new Processing(new IndexTestUtils.IndexTermDataTarget(
            new CollectionSource<>(testData), index, prefix));
    p.process();
  }

  @Test
  public void testGetTermData_4arg() {
    LOG.info("Test getTermData_4arg");
    final String prefix = "test";
    final String key = "testKey";
    final int amount = 5000;
    Collection<Tuple.Tuple4<Integer, BytesWrap, String, Integer>> testData;
    Processing p;
    int retrievalCount;

    // part one - all random data
    // create test data
    testData = IndexTestUtils.generateTermData(index, amount);
    p = new Processing(new IndexTestUtils.IndexTermDataTarget(
            new CollectionSource<>(testData), index, prefix));
    p.process();

    // retrieve test data
    retrievalCount = 0;
    for (Tuple.Tuple4<Integer, BytesWrap, String, Integer> t4 : testData) {
      assertEquals("Pass 1: Retrieved termData mismatch. prefix=" + prefix
              + " docId=" + t4.a + " term=" + t4.b + " key=" + t4.c, index.getTermData(
                      prefix, t4.a, t4.b, t4.c), t4.d);
      retrievalCount++;
    }

    assertEquals("Pass 1: Not all set termData was retrieved.", amount,
            retrievalCount);

    index.reset();

    // part two - all data on one key
    // create test data
    testData = IndexTestUtils.generateTermData(index, key, amount);
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

    assertEquals("Pass 2: Not all set termData was retrieved.", amount,
            retrievalCount);
  }

  @Test
  public void testGetTermData_3arg() {
    LOG.info("Test getTermData_3arg");
    final int testEntryCount = 3;
    final String prefix = "testPrefix_3a";
    final Integer docId = 0;
    final Tuple.Tuple2<BytesWrap, String> d1 = Tuple.tuple2(new BytesWrap(
            new BytesRef("testRetrieve_3a1")), "testRetrievalData_3a1");
    final Tuple.Tuple2<BytesWrap, String> d2 = Tuple.tuple2(new BytesWrap(
            new BytesRef("testRetrieve_3a2")), "testRetrievalData_3a2");
    final Tuple.Tuple2<BytesWrap, String> d3 = Tuple.tuple2(new BytesWrap(
            new BytesRef("testRetrieve_3a3")), "testRetrievalData_3a3");
    final String key = "testRetrievalKey_3a";

    index.setTermData(prefix, docId, d1.a, key, d1.b);
    index.setTermData(prefix, docId, d2.a, key, d2.b);
    index.setTermData(prefix, docId, d3.a, key, d3.b);

    assertEquals("Value retrieval mismatch.", d1.b, index.getTermData(prefix,
            docId, d1.a, key));
    assertEquals("Value retrieval mismatch.", d2.b, index.getTermData(prefix,
            docId, d2.a, key));
    assertEquals("Value retrieval mismatch.", d3.b, index.getTermData(prefix,
            docId, d3.a, key));

    int matches = 0;
    for (Entry<BytesWrap, Object> dataEntry : index.getTermData(prefix, docId,
            key).entrySet()) {
      if (dataEntry.getKey().equals(d1.a)) {
        assertEquals("Value retrieval mismatch.", dataEntry.getValue(), d1.b);
        matches++;
      } else if (dataEntry.getKey().equals(d2.a)) {
        assertEquals("Value retrieval mismatch.", dataEntry.getValue(), d2.b);
        matches++;
      } else if (dataEntry.getKey().equals(d3.a)) {
        assertEquals("Value retrieval mismatch.", dataEntry.getValue(), d3.b);
        matches++;
      }
    }
    assertEquals("Not all data entries matched.", matches, testEntryCount);
  }

  @Test
  public void testHasDocumentModel() {
    LOG.info("Test hasDocumentModel");
    for (int i = 0; i < index.getDocumentCount(); i++) {
      assertTrue("Document (" + i + ") not found.", index.hasDocument(i));
    }
  }

  @Test
  public void testGetDocumentModel() {
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
  public void testRegisterPrefix() {
    LOG.info("Test registerPrefix");
    index.registerPrefix("foo");
  }
}
