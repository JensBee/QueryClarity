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
import de.unihildesheim.lucene.index.TestIndexDataProvider;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.RandomValue;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.util.ArrayList;
import java.util.Collection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link QueryUtils}.
 *
 *
 */
public class QueryUtilsTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          QueryUtilsTest.class);

  /**
   * Temporary Lucene memory index.
   */
  private static TestIndexDataProvider index;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {

    // create the test index
    index = new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.",
            TestIndexDataProvider.isInitialized());
    index.setupEnvironment(null, null);
  }

  /**
   * Test of getUniqueQueryTerms method, of class QueryUtils.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("DM_DEFAULT_ENCODING")
  public void testGetUniqueQueryTerms() throws Exception {
    LOG.info("Test getUniqueQueryTerms");
    final int termsCount = RandomValue.getInteger(3, 100);
    final Collection<ByteArray> termsBw = new ArrayList<>(termsCount);
    final Collection<String> terms = new ArrayList<>(termsCount);

    for (int i = 0; i < termsCount; i++) {
      final String term = RandomValue.getString(1, 15);

      termsBw.add(new ByteArray(term.getBytes()));
      terms.add(term);
    }

    final String queryString = index.getQueryString(terms.toArray(
            new String[termsCount]));
    final Collection<ByteArray> result = QueryUtils.getUniqueQueryTerms(
            queryString);

    assertTrue("Not all terms returned.", result.containsAll(termsBw));

    if (termsBw.size() != result.size()) {
      @java.lang.SuppressWarnings("StringBufferWithoutInitialCapacity")
      StringBuilder sbInitial = new StringBuilder();

      for (ByteArray bw : termsBw) {
        sbInitial.append(new String(bw.bytes)).append(' ');
      }

      @java.lang.SuppressWarnings("StringBufferWithoutInitialCapacity")
      StringBuilder sbResult = new StringBuilder();

      for (ByteArray bw : result) {
        sbResult.append(ByteArrayUtil.utf8ToString(bw)).append(
                ' ');
      }
    }

    assertEquals("Initial term list and returned list differ in size.",
            termsBw.size(), result.size());
  }

  /**
   * Test of getAllQueryTerms method, of class QueryUtils.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("DM_DEFAULT_ENCODING")
  public void testGetAllQueryTerms() throws Exception {
    LOG.info("Test getAllQueryTerms");
    final int termsCount = RandomValue.getInteger(3, 100);
    final Collection<ByteArray> termsBw = new ArrayList<>(termsCount);
    final Collection<String> terms = new ArrayList<>(termsCount);

    for (int i = 0; i < termsCount; i++) {
      final String term = RandomValue.getString(1, 15);

      termsBw.add(new ByteArray(term.getBytes()));
      terms.add(term);
    }

    // double the lists
    terms.addAll(terms);
    termsBw.addAll(termsBw);

    final String queryString = index.getQueryString(terms.toArray(
            new String[termsCount]));
    final Collection<ByteArray> result = QueryUtils.getAllQueryTerms(
            queryString);

    assertTrue("Not all terms returned.", result.containsAll(termsBw));

    if (termsBw.size() != result.size()) {
      @java.lang.SuppressWarnings("StringBufferWithoutInitialCapacity")
      StringBuilder sbInitial = new StringBuilder();

      for (ByteArray bw : termsBw) {
        sbInitial.append(new String(bw.bytes)).append(' ');
      }

      @java.lang.SuppressWarnings("StringBufferWithoutInitialCapacity")
      StringBuilder sbResult = new StringBuilder();

      for (ByteArray bw : result) {
        sbResult.append(ByteArrayUtil.utf8ToString(bw)).append(
                ' ');
      }
    }

    assertEquals("Initial term list and returned list differ in size.",
            termsBw.size(), result.size());
  }
}
