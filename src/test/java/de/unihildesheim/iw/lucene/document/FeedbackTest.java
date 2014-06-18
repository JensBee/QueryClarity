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
package de.unihildesheim.iw.lucene.document;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Test for {@link Feedback}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("ParameterizedParametersStaticCollection")
@RunWith(Parameterized.class)
public final class FeedbackTest
    extends MultiIndexDataProviderTestCase {

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public FeedbackTest(
      final DataProviders dataProv,
      final RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Test of get method, of class Feedback. Get random results.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGet_Query_int__random()
      throws Exception {
    try (final IndexDataProvider instance = getInstance()) {
      // try to get some random results
      final long maxDocCount = instance.getDocumentCount();
      Collection<Integer> result;
      for (int i = 1; (long) i < maxDocCount; i += 10) {
        result = Feedback.get(TestIndexDataProvider.getIndexReader(),
            this.referenceIndex.getQueryObj().getQueryObj(), i);
        Assert.assertNotEquals(msg("There must be results."),
            0L, (long) result.size());
      }
    }
  }

  /**
   * Test of get method, of class Feedback. Get all results.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGet_Query_int__all()
      throws Exception {
    // try to get some random results
    final Collection<Integer> result;
    result = Feedback.get(TestIndexDataProvider.getIndexReader(),
        this.referenceIndex.getQueryObj().getQueryObj(), -1);
    Assert.assertNotEquals(
        msg("No documents retrieved from feedback.", this.referenceIndex),
        0L, (long) result.size());
  }

  /**
   * Test of get method, of class Feedback. Get matching results.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("ReuseOfLocalVariable")
  @Test
  public void testGet_Query_int__matching()
      throws Exception {
    // check if a matching document is in the result set
    Collection<Integer> result;

    try (final IndexDataProvider index = getInstance()) {
      assert index.getDocumentCount() > 0L;
      final DocumentModel docModel = index.getDocumentModel(RandomValue
          .getInteger(0, (int) index.getDocumentCount() - 1));
      final String[] singleTermQuery = {""};
      final String[] multiTermQuery = new String[RandomValue.getInteger(2,
          docModel.getTermFreqMap().size() - 1)];
      int termIdx = 0;
      for (final ByteArray term : docModel.getTermFreqMap().keySet()) {
        multiTermQuery[termIdx] = ByteArrayUtils.utf8ToString(term);
        if (++termIdx >= multiTermQuery.length) {
          break;
        }
      }
      final List<ByteArray> terms = new ArrayList<>(docModel.getTermFreqMap().
          keySet());
      final int idx = RandomValue.getInteger(0, terms.size() - 1);
      singleTermQuery[0] = ByteArrayUtils.utf8ToString(terms.get(idx));

      boolean foundDoc = false;
      final Query query = this.referenceIndex.getSTQueryObj
          (singleTermQuery).getQueryObj();
      result = Feedback.get(TestIndexDataProvider.getIndexReader(), query, -1);
      for (final Integer docId : result) {
        if (docId.equals(docModel.id)) {
          foundDoc = true;
        }
      }
      Assert.assertTrue(msg("Document not in single-term query result set. " +
          "result=" + result.size() + " query=" + query + " docs=" + result
          + " searchId=" + docModel.id), foundDoc);

      foundDoc = false;
      result = Feedback.get(TestIndexDataProvider.getIndexReader(),
          this.referenceIndex.getSTQueryObj(multiTermQuery).getQueryObj(),
          -1
      );
      for (final Integer docId : result) {
        if (docId == docModel.id) {
          foundDoc = true;
        }
      }
      Assert.assertTrue(
          msg("Document not in multi-term query result set."), foundDoc);
    }
  }

  /**
   * Test of getFixed method, of class Feedback. Random results.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFixed_Query_int__random()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final long maxDocCount = index.getDocumentCount();
      Collection<Integer> result;
      // try to get some random results
      for (int i = 1; (long) i < maxDocCount; i += 10) {
        result = Feedback.getFixed(TestIndexDataProvider.getIndexReader(),
            this.referenceIndex.getQueryObj().getQueryObj(), i);
        Assert.assertEquals(msg("Less than expected documents returned."),
            (long) i, (long) result.size());
      }
    }
  }

  /**
   * Test of getFixed method, of class Feedback. Try get more than available.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFixed_Query_int__maxResult()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      // try to get more documents than available
      final long maxDocCount = index.getDocumentCount();
      final Collection<Integer> result;
      result = Feedback.getFixed(TestIndexDataProvider.getIndexReader(),
          this.referenceIndex.getQueryObj().getQueryObj(),
          (int) maxDocCount + 100);
      Assert.assertEquals(msg("Less than expected documents returned."),
          maxDocCount, (long) result.size());
    }
  }

  /**
   * Test of getFixed method, of class Feedback. Matching documents.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFixed_Query_int__matching()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final long maxDocCount = index.getDocumentCount();
      Collection<Integer> result;

      // check if a matching document is in the result set
      final DocumentModel docModel = index.getDocumentModel(RandomValue.
          getInteger(0, (int) index.getDocumentCount() - 1));
      final String[] singleTermQuery = {""};
      final String[] multiTermQuery = new String[RandomValue.getInteger(2,
          docModel.getTermFreqMap().size())];
      int termIdx = 0;
      for (final ByteArray term : docModel.getTermFreqMap().keySet()) {
        if (singleTermQuery[0].isEmpty() && RandomValue.getBoolean()) {
          singleTermQuery[0] = ByteArrayUtils.utf8ToString(term);
        }
        multiTermQuery[termIdx] = ByteArrayUtils.utf8ToString(term);
        if (++termIdx >= multiTermQuery.length) {
          break;
        }
      }

      // backup, if query is still empty
      if (singleTermQuery[0].isEmpty()) {
        singleTermQuery[0] = multiTermQuery[0];
      }

      result = Feedback.getFixed(TestIndexDataProvider.getIndexReader(),
          this.referenceIndex.getSTQueryObj(singleTermQuery).getQueryObj(),
          (int) maxDocCount);
      Assert.assertTrue(msg("Document not in single-term query result set."),
          result.contains(docModel.id));

      result = Feedback.getFixed(TestIndexDataProvider.getIndexReader(),
          this.referenceIndex.getSTQueryObj(multiTermQuery).getQueryObj(),
          (int) maxDocCount
      );
      Assert.assertTrue(msg("Document not in multi-term query result set."),
          result.contains(docModel.id));
    }
  }

  /**
   * Test of get method, of class Feedback.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGet_3args()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      // wrapper function - just test if it succeeds
      Assert.assertFalse(msg("No results."),
          Feedback.get(TestIndexDataProvider.getIndexReader(),
              this.referenceIndex.getQueryObj().getQueryObj(),
              RandomValue.getInteger(1, (int) index.getDocumentCount())
          ).isEmpty()
      );
    }
  }

  /**
   * Test of getFixed method, of class Feedback.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFixed_3args()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      // wrapper function - just test if it succeeds
      Assert.assertFalse(msg("No results."),
          Feedback.getFixed(TestIndexDataProvider.getIndexReader(),
              this.referenceIndex.getQueryObj().getQueryObj(),
              RandomValue.getInteger(1, (int) index.getDocumentCount())
          ).isEmpty());
    }
  }
}
