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
package de.unihildesheim.lucene.query;

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.util.StringUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link SimpleTermsQuery}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
@RunWith(Parameterized.class)
public final class SimpleTermsQueryTest extends MultiIndexDataProviderTestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          SimpleTermsQueryTest.class);

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

  /**
   * Use all {@link IndexDataProvider}s for testing.
   *
   * @return
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return getCaseParameters();
  }

  public SimpleTermsQueryTest(
          final Class<? extends IndexDataProvider> dataProv) {
    super(dataProv);
  }

  private SimpleTermsQuery getInstance() throws Exception {
    return getInstance(index.getQueryString());
  }

  private SimpleTermsQuery getInstance(final String query) throws Exception {
    return new SimpleTermsQuery(query, SimpleTermsQuery.DEFAULT_OPERATOR,
            Environment.getFields(), Environment.getStopwords());
  }

  /**
   * Test constructor of class SimpleTermQuery.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testConstructor() throws Exception {
    LOG.info("Test constructor");
    SimpleTermsQuery instance;
    try {
      instance = new SimpleTermsQuery(" ", SimpleTermsQuery.DEFAULT_OPERATOR,
              Environment.getFields(), Environment.getStopwords());
      fail("Expected exception: Empty query string.");
    } catch (IllegalArgumentException ex) {
    }
    try {
      instance = new SimpleTermsQuery(null, SimpleTermsQuery.DEFAULT_OPERATOR,
              Environment.getFields(), Environment.getStopwords());
      fail("Expected exception: Empty query string (null).");
    } catch (IllegalArgumentException ex) {
    }
    try {
      instance = new SimpleTermsQuery("", SimpleTermsQuery.DEFAULT_OPERATOR,
              Environment.getFields(), Environment.getStopwords());
      fail("Expected exception: Empty query string.");
    } catch (IllegalArgumentException ex) {
    }
  }

  /**
   * Test of toString method, of class SimpleTermQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testToString_String() throws Exception {
    LOG.info("Test toString 1arg");
    final SimpleTermsQuery instance = getInstance();
    for (String field : Environment.getFields()) {
      String result = instance.toString(field);
      assertFalse(result.isEmpty());
    }
  }

  /**
   * Test of toString method, of class SimpleTermQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testToString_0args() throws Exception {
    LOG.info("Test toString 0arg");
    final SimpleTermsQuery instance = getInstance();
    instance.toString();
  }

  /**
   * Test of wordsChanged method, of class SimpleTermQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testWordsChanged() throws Exception {
    LOG.info("Test wordsChanged");
    final Collection<String> oldWords = new ArrayList<>(Environment.
            getStopwords());

    SimpleTermsQuery instance;
    final String queryStr = index.getQueryString();
    instance = getInstance(queryStr);
    final String preFilter = instance.toString();

    final List<String> qTerms
            = new ArrayList<>(StringUtils.split(queryStr, " "));
    final List<String> stopWords = new ArrayList<>(1);
    stopWords.add(qTerms.get(0));
    Environment.setStopwords(stopWords);
    instance = getInstance(queryStr);
    final String postFilter = instance.toString();

    assertNotEquals("Pre- and post-filtered queries should not be the same.",
            preFilter, postFilter);
  }

  /**
   * Test of createWeight method, of class SimpleTermQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCreateWeight() throws Exception {
    LOG.info("Test createWeight");
    IndexSearcher searcher = new IndexSearcher(Environment.getIndexReader().
            getContext());
    final SimpleTermsQuery instance = getInstance();
    Weight result = instance.createWeight(searcher);
    LOG.debug("W: {}", result);
  }

}
