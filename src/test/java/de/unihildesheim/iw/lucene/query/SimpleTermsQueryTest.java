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
package de.unihildesheim.iw.lucene.query;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link SimpleTermsQuery}.
 *
 * @author Jens Bertram
 */
@RunWith(Parameterized.class)
public final class SimpleTermsQueryTest
    extends MultiIndexDataProviderTestCase {

  /**
   * Setup test using a defined {@link IndexDataProvider}.
   *
   * @param dataProv Data provider to use
   * @param rType Data provider configuration
   */
  public SimpleTermsQueryTest(
      final DataProviders dataProv,
      final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Get an instance with random query string
   *
   * @return Instance with a random query string set
   * @throws Exception Any exception thrown indicates an error
   */
  private SimpleTermsQuery getInstance()
      throws Exception {
    return getInstance(TestIndexDataProvider.util.getQueryString());
  }

  /**
   * Get an instance with the given query string set
   *
   * @param query Query string
   * @return Instance with the given query string set
   * @throws Exception Any exception thrown indicates an error
   */
  private SimpleTermsQuery getInstance(final String query)
      throws Exception {
    return new SimpleTermsQuery(query, SimpleTermsQuery.DEFAULT_OPERATOR,
        referenceIndex.getDocumentFields(),
        referenceIndex.getStopwords());
  }

  /**
   * Test constructor of class SimpleTermQuery.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void testConstructor()
      throws Exception {
    try {
      new SimpleTermsQuery(" ", SimpleTermsQuery.DEFAULT_OPERATOR,
          referenceIndex.getDocumentFields(),
          referenceIndex.getStopwords());
      fail(msg("Expected exception: Empty query string."));
    } catch (IllegalArgumentException ex) {
    }
    try {
      new SimpleTermsQuery(null, SimpleTermsQuery.DEFAULT_OPERATOR,
          referenceIndex.getDocumentFields(),
          referenceIndex.getStopwords());
      fail(msg("Expected exception: Empty query string (null)."));
    } catch (IllegalArgumentException ex) {
    }
    try {
      new SimpleTermsQuery("", SimpleTermsQuery.DEFAULT_OPERATOR,
          referenceIndex.getDocumentFields(),
          referenceIndex.getStopwords());
      fail(msg("Expected exception: Empty query string."));
    } catch (IllegalArgumentException ex) {
    }
  }

  /**
   * Test of toString method, of class SimpleTermQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testToString_String()
      throws Exception {
    final SimpleTermsQuery instance = getInstance();
    for (final String field : referenceIndex.getDocumentFields()) {
      final String result = instance.toString(field);
      assertFalse("Results were empty.", result.isEmpty());
    }
  }

  /**
   * Test of toString method, of class SimpleTermQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testToString_0args()
      throws Exception {
    getInstance().toString();
  }

  /**
   * Test of createWeight method, of class SimpleTermQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCreateWeight()
      throws Exception {
    final IndexSearcher searcher =
        new IndexSearcher(referenceIndex.getIndexReader().
            getContext());
    getInstance().createWeight(searcher);
  }

  /**
   * Test of getQueryObj method, of class SimpleTermsQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryObj()
      throws Exception {
    final String queryStr = TestIndexDataProvider.util.getQueryString();
    final SimpleTermsQuery instance = getInstance(queryStr);
    final Collection<String> result = instance.getQueryTerms();
    final Collection<ByteArray> exp = new QueryUtils(referenceIndex
        .getIndexReader(), referenceIndex.getDocumentFields())
        .getAllQueryTerms(queryStr);
    final Collection<String> expResult = new ArrayList<>(exp.size());
    final Collection<String> stopwords = referenceIndex.getStopwords();

    for (final ByteArray ba : exp) {
      final String term = ByteArrayUtils.utf8ToString(ba);
      if (!stopwords.contains(term)) {
        expResult.add(term);
      }
    }

    assertEquals(msg("Term count differs."), expResult.size(), result.size());
    assertTrue(msg("Not all terms present."), expResult.containsAll(result));
  }

  /**
   * Test of getQueryTerms method, of class SimpleTermsQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryTerms()
      throws Exception {
    final int termsCount = RandomValue.getInteger(3, 100);
    final List<String> terms = new ArrayList<>(termsCount);
    final Collection<String> stopwords = referenceIndex.getStopwords();

    for (int i = 0; i < termsCount; i++) {
      final String term = RandomValue.getString(1, 15);
      if (!stopwords.contains(term)) {
        terms.add(term);
      }
    }
    final SimpleTermsQuery instance = getInstance(StringUtils.join(terms, " "));

    assertEquals(msg("Not all terms returned."), terms.size(), instance.
        getQueryTerms().size());
    assertTrue(msg("Not all terms in result set."), instance.getQueryTerms().
        containsAll(terms));
  }

}
