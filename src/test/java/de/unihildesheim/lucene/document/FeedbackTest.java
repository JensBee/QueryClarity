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
package de.unihildesheim.lucene.document;

import de.unihildesheim.ByteArray;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.RandomValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.Query;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Test for {@link Feedback}.
 *
 * @author Jens Bertram
 */
public final class FeedbackTest extends MultiIndexDataProviderTestCase {

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public FeedbackTest(
          final Class<? extends IndexDataProvider> dataProv,
          final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Test of get method, of class Feedback.
   *
   * @see #testGet_Query_int__random()
   * @see #testGet_Query_int__all()
   * @see #testGet_Query_int__matching()
   */
  public void testGet_Query_int() {
    // implemented in seperate functions
  }

  /**
   * Test of get method, of class Feedback. Get random results.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGet_Query_int__random() throws Exception {
    // try to get some random results
    final long maxDocCount = index.getDocumentCount();
    Collection<Integer> result;
    for (int i = 1; i < maxDocCount; i += 10) {
      result = Feedback.get(index.getQueryObj(), i);
      assertNotEquals(msg("There must be results."), 0, result.size());
    }
  }

  /**
   * Test of get method, of class Feedback. Get all results.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGet_Query_int__all() throws Exception {
    // try to get some random results
    Collection<Integer> result;
    result = Feedback.get(index.getQueryObj(), -1);
    assertNotEquals(msg("No documents retrieved from feedback."), 0,
            result.size());
  }

  /**
   * Test of get method, of class Feedback. Get matching results.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGet_Query_int__matching() throws Exception {
    // check if a matching document is in the result set
    Collection<Integer> result;
    final DocumentModel docModel = index.getDocumentModel(RandomValue.
            getInteger(0, (int) index.getDocumentCount() - 1));
    final String[] singleTermQuery = new String[]{""};
    final String[] multiTermQuery = new String[RandomValue.getInteger(2,
            docModel.termFreqMap.size() - 1)];
    int termIdx = 0;
    for (ByteArray term : docModel.termFreqMap.keySet()) {
      multiTermQuery[termIdx] = ByteArrayUtil.utf8ToString(term);
      if (++termIdx >= multiTermQuery.length) {
        break;
      }
    }
    final List<ByteArray> terms = new ArrayList<>(docModel.termFreqMap.
            keySet());
    final int idx = RandomValue.getInteger(0, terms.size() - 1);
    singleTermQuery[0] = ByteArrayUtil.utf8ToString(terms.get(idx));

    boolean foundDoc = false;
    Query query = index.getQueryObj(singleTermQuery);
    result = Feedback.get(query, -1);
    for (Integer docId : result) {
      if (docId.equals(docModel.id)) {
        foundDoc = true;
      }
    }
    assertTrue(msg("Document not in single-term query result set. result="
            + result.size() + " query=" + query + " docs=" + result
            + " searchId=" + docModel.id), foundDoc);

    foundDoc = false;
    result = Feedback.get(index.getQueryObj(multiTermQuery), -1);
    for (Integer docId : result) {
      if (docId == docModel.id) {
        foundDoc = true;
      }
    }
    assertTrue(msg("Document not in multi-term query result set."), foundDoc);
  }

  /**
   * Test of getFixed method, of class Feedback.
   *
   * @see #testGetFixed_Query_int__random()
   * @see #testGetFixed_Query_int__maxResult()
   * @see #testGetFixed_Query_int__matching()
   */
  public void testGetFixed_Query_int() {
    // implemented in seperate functions
  }

  /**
   * Test of getFixed method, of class Feedback. Random results.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFixed_Query_int__random() throws Exception {
    final long maxDocCount = index.getDocumentCount();
    Collection<Integer> result;
    // try to get some random results
    for (int i = 1; i < maxDocCount; i += 10) {
      result = Feedback.getFixed(index.getQueryObj(), i);
      assertEquals(msg("Less than expected documents returned."), i, result.
              size());
    }
  }

  /**
   * Test of getFixed method, of class Feedback. Try get more than available.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFixed_Query_int__maxResult() throws Exception {
    // try to get more documents than available
    final long maxDocCount = index.getDocumentCount();
    Collection<Integer> result;
    result = Feedback.getFixed(index.getQueryObj(), (int) maxDocCount
            + 100);
    assertEquals(msg("Less than expected documents returned."), maxDocCount,
            result.size());
  }

  /**
   * Test of getFixed method, of class Feedback. Matching documents.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFixed_Query_int__matching() throws Exception {
    final long maxDocCount = index.getDocumentCount();
    Collection<Integer> result;

    // check if a matching document is in the result set
    final DocumentModel docModel = index.getDocumentModel(RandomValue.
            getInteger(0, (int) index.getDocumentCount() - 1));
    final String[] singleTermQuery = new String[]{""};
    final String[] multiTermQuery = new String[RandomValue.getInteger(2,
            docModel.termFreqMap.size())];
    int termIdx = 0;
    for (ByteArray term : docModel.termFreqMap.keySet()) {
      if (singleTermQuery[0].isEmpty() && RandomValue.getBoolean()) {
        singleTermQuery[0] = ByteArrayUtil.utf8ToString(term);
      }
      multiTermQuery[termIdx] = ByteArrayUtil.utf8ToString(term);
      if (++termIdx >= multiTermQuery.length) {
        break;
      }
    }

    result = Feedback.getFixed(index.getQueryObj(singleTermQuery),
            (int) maxDocCount);
    assertTrue(msg("Document not in single-term query result set."), result.
            contains(docModel.id));

    result = Feedback.getFixed(index.getQueryObj(multiTermQuery),
            (int) maxDocCount);
    assertTrue(msg("Document not in multi-term query result set."), result.
            contains(docModel.id));
  }

  /**
   * Test of get method, of class Feedback.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGet_3args() throws Exception {
    // wrapper function - just test if it succeeds
    assertFalse(msg("No results."), Feedback.get(Environment.getIndexReader(),
            index.getQueryObj(), RandomValue.getInteger(1, (int) index.
                    getDocumentCount())).isEmpty());
  }

  /**
   * Test of getFixed method, of class Feedback.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFixed_3args() throws Exception {
    System.out.println("getFixed [reader, query, docCount]");
    // wrapper function - just test if it succeeds
    assertFalse(msg("No results."), Feedback.getFixed(Environment.
            getIndexReader(),
            index.getQueryObj(), RandomValue.getInteger(1, (int) index.
                    getDocumentCount())).isEmpty());
  }
}
