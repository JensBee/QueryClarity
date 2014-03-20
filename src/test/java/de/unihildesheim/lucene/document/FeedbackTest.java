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

import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.query.TermsQueryBuilder;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.RandomValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.Query;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link Feedback}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
@RunWith(Parameterized.class)
public class FeedbackTest extends MultiIndexDataProviderTestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          FeedbackTest.class);

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    index = new TestIndex(TestIndex.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());
  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static void tearDownClass() {
    // close the test index
    index.dispose();
  }

  /**
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public void setUp() throws Exception {
    caseSetUp();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return getCaseParameters();
  }

  public FeedbackTest(
          final Class<? extends IndexDataProvider> dataProv) {
    super(dataProv);
  }

  /**
   * Test of get method, of class Feedback.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGet() throws Exception {
    LOG.info("Test get");
    final long maxDocCount = index.getDocumentCount();

    Collection<Integer> result;

    // get all query
    result = Feedback.get(index.getQueryObj(), -1);
    assertNotEquals("No documents retrieved from feedback.", 0,
            result.size());

    // try to get some random results
    for (int i = 1; i < maxDocCount; i += 10) {
      result = Feedback.get(index.getQueryObj(), i);
      assertNotEquals("There must be results.", 0, result.size());
    }

    // check if a matching document is in the result set
    final DocumentModel docModel = index.getDocumentModel(RandomValue.
            getInteger(0, (int) index.getDocumentCount()));
    final String[] singleTermQuery = new String[]{""};
    final String[] multiTermQuery = new String[RandomValue.getInteger(2,
            docModel.termFreqMap.size() - 1)];
    int termIdx = 0;
    for (BytesWrap term : docModel.termFreqMap.keySet()) {
      multiTermQuery[termIdx] = term.toString();
      if (++termIdx >= multiTermQuery.length) {
        break;
      }
    }
    final List<BytesWrap> terms = new ArrayList<>(docModel.termFreqMap.
            keySet());
    final int idx = RandomValue.getInteger(0, terms.size() - 1);
    singleTermQuery[0] = terms.get(idx).toString();

    boolean foundDoc = false;
    Query query = index.getQueryObj(singleTermQuery);
    result = Feedback.get(query, -1);
    for (Integer docId : result) {
      if (docId.equals(docModel.id)) {
        foundDoc = true;
      }
    }
    assertTrue("Document not in single-term query result set. result="
            + result.size() + " query=" + query + " docs=" + result
            + " searchId=" + docModel.id, foundDoc);

    foundDoc = false;
    result = Feedback.get(index.getQueryObj(multiTermQuery), -1);
    for (Integer docId : result) {
      if (docId == docModel.id) {
        foundDoc = true;
      }
    }
    assertTrue("Document not in multi-term query result set.", foundDoc);
  }

  /**
   * Test of getFixed method, of class Feedback.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFixed() throws Exception {
    LOG.info("Test getFixed");
    final long maxDocCount = index.getDocumentCount();
    Collection<Integer> result;

    // try to get some random results
    for (int i = 1; i < maxDocCount; i += 10) {
      result = Feedback.getFixed(index.getQueryObj(), i);
      assertEquals("Less than expected documents returned.", i, result.size());
    }

    // try to get more documents than available
    result = Feedback.getFixed(index.getQueryObj(), (int) maxDocCount
            + 100);
    assertEquals("Less than expected documents returned.", maxDocCount,
            result.size());

    // check if a matching document is in the result set
    final DocumentModel docModel = index.getDocumentModel(RandomValue.
            getInteger(0, (int) index.getDocumentCount()));
    final String[] singleTermQuery = new String[]{""};
    final String[] multiTermQuery = new String[RandomValue.getInteger(2,
            docModel.termFreqMap.size())];
    int termIdx = 0;
    for (BytesWrap term : docModel.termFreqMap.keySet()) {
      if (singleTermQuery[0].isEmpty() && RandomValue.getBoolean()) {
        singleTermQuery[0] = term.toString();
      }
      multiTermQuery[termIdx] = term.toString();
      if (++termIdx >= multiTermQuery.length) {
        break;
      }
    }

    result = Feedback.getFixed(index.getQueryObj(singleTermQuery),
            (int) maxDocCount);
    assertTrue("Document not in single-term query result set.", result.
            contains(docModel.id));

    result = Feedback.getFixed(index.getQueryObj(multiTermQuery),
            (int) maxDocCount);
    assertTrue("Document not in multi-term query result set.", result.
            contains(docModel.id));
  }

}
