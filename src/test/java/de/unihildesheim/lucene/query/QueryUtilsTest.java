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
import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.RandomValue;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link QueryUtils}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
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
  private static TestIndex index;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    // create the test index
    index = new TestIndex(TestIndex.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());
    index.setupEnvironment();
  }

  /**
   * Test of getQueryTerms method, of class QueryUtils.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("DM_DEFAULT_ENCODING")
  public void testGetQueryTerms() throws Exception {
    LOG.info("Test getQueryTerms");
    IndexReader reader = Environment.getIndexReader();

    final int termsCount = RandomValue.getInteger(3, 100);
    final Collection<BytesWrap> termsBw = new ArrayList(termsCount);
    final Collection<String> terms = new ArrayList(termsCount);

    for (int i = 0; i < termsCount; i++) {
      final String term = RandomValue.getString(1, 15);
      termsBw.add(new BytesWrap(term.getBytes()));
      terms.add(term);
    }

    final Query query = QueryUtils.buildQuery(Environment.getFields(), index.
            getQueryString(terms.toArray(new String[termsCount])));

    final Collection<BytesWrap> result = QueryUtils.
            getUniqueQueryTerms(reader,
                    query);
    assertTrue("Not all terms returned.", result.containsAll(termsBw));
    if (termsBw.size() != result.size()) {
      StringBuilder sbInitial = new StringBuilder();
      for (BytesWrap bw : termsBw) {
        sbInitial.append(new String(bw.getBytes())).append(' ');
      }
      LOG.error("INITIAL: {}", sbInitial.toString());

      StringBuilder sbResult = new StringBuilder();
      for (BytesWrap bw : result) {
        sbResult.append(new String(bw.getBytes())).append(' ');
      }
      LOG.error("RESULT: {}", sbResult.toString());
    }
    assertEquals("Initial term list and returned list differ in size.",
            termsBw.size(), result.size());
  }

}
