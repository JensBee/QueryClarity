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

import de.unihildesheim.ByteArray;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndexDataProvider;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.StringUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link SimpleTermsQuery}.
 *
 *
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
    index = new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
            isInitialized());
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

  @After
  public void tearDown() throws Exception {
  }

  /**
   * Use all {@link IndexDataProvider}s for testing.
   *
   * @return Parameter collection
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return getCaseParameters();
  }

  /**
   * Setup test using a defined {@link IndexDataProvider}.
   *
   * @param dataProv Data provider to use
   * @param rType Data provider configuration
   */
  public SimpleTermsQueryTest(
          final Class<? extends IndexDataProvider> dataProv,
          final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Get an instance with random query string
   *
   * @return Instance with a random query string set
   * @throws Exception Any exception thrown indicates an error
   */
  private SimpleTermsQuery getInstance() throws Exception {
    return getInstance(index.getQueryString());
  }

  /**
   * Get an instance with the given query string set
   *
   * @param query Query string
   * @return Instance with the given query string set
   * @throws Exception Any exception thrown indicates an error
   */
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
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  public void testConstructor() throws Exception {
    LOG.info("Test constructor");
    try {
      new SimpleTermsQuery(" ", SimpleTermsQuery.DEFAULT_OPERATOR,
              Environment.getFields(), Environment.getStopwords());
      fail("Expected exception: Empty query string.");
    } catch (IllegalArgumentException ex) {
    }
    try {
      new SimpleTermsQuery(null, SimpleTermsQuery.DEFAULT_OPERATOR,
              Environment.getFields(), Environment.getStopwords());
      fail("Expected exception: Empty query string (null).");
    } catch (IllegalArgumentException ex) {
    }
    try {
      new SimpleTermsQuery("", SimpleTermsQuery.DEFAULT_OPERATOR,
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

  /**
   * Test of getQueryObj method, of class SimpleTermsQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryObj() throws Exception {
    LOG.info("Test getQueryObj");
    final String queryStr = index.getQueryString();
    final SimpleTermsQuery instance = getInstance(queryStr);
    final Collection<String> result = instance.getQueryTerms();
    final Collection<ByteArray> exp = QueryUtils.getAllQueryTerms(queryStr);
    final Collection<String> expResult = new ArrayList<>(exp.size());
    for (ByteArray ba : exp) {
      expResult.add(ByteArrayUtil.utf8ToString(ba));
    }

    assertEquals("Term count differs.", expResult.size(), result.size());
    assertTrue("Not all terms present.", expResult.containsAll(result));
  }

  /**
   * Test of getQueryTerms method, of class SimpleTermsQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryTerms() throws Exception {
    LOG.info("Test getQueryTerms");
    final int termsCount = RandomValue.getInteger(3, 100);
    final List<String> terms = new ArrayList<>(termsCount);

    for (int i = 0; i < termsCount; i++) {
      final String term = RandomValue.getString(1, 15);
      terms.add(term);
    }
    SimpleTermsQuery instance = getInstance(StringUtils.join(terms, " "));

    assertEquals("Not all terms returned.", terms.size(), instance.
            getQueryTerms().size());
    assertTrue("Not all terms in result set.", instance.getQueryTerms().
            containsAll(terms));
  }

}
