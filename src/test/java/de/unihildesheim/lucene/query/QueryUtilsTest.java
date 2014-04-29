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
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.RandomValue;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test for {@link QueryUtils}.
 *
 * @author Jens Bertram
 */
@RunWith(Parameterized.class)
public final class QueryUtilsTest extends MultiIndexDataProviderTestCase {

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public QueryUtilsTest(
          final Class<? extends IndexDataProvider> dataProv,
          final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Test of getUniqueQueryTerms method, of class QueryUtils.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetUniqueQueryTerms() throws Exception {
    final int termsCount = RandomValue.getInteger(3, 100);
    final Collection<ByteArray> termsBw = new HashSet<>(termsCount);
    final Collection<String> terms = new HashSet<>(termsCount);
    final Collection<String> stopwords = Environment.getStopwords();

    for (int i = 0; i < termsCount; i++) {
      final String term = RandomValue.getString(1, 15);

      if (!stopwords.contains(term)) {
        termsBw.add(new ByteArray(term.getBytes("UTF-8")));
      }
      terms.add(term);
    }

    final String queryString = index.getQueryString(terms.toArray(
            new String[terms.size()]));
    final Collection<ByteArray> result = QueryUtils.getUniqueQueryTerms(
            queryString);

    assertEquals(msg("Terms amount mismatch."), termsBw.size(), result.size());
    assertTrue(msg("Term list content differs."), result.containsAll(termsBw));
  }

  /**
   * Test of getAllQueryTerms method, of class QueryUtils.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("DM_DEFAULT_ENCODING")
  public void testGetAllQueryTerms() throws Exception {
    final int termsCount = RandomValue.getInteger(3, 100);
    final Collection<ByteArray> termsBw = new ArrayList<>(termsCount);
    final Collection<String> terms = new ArrayList<>(termsCount);
    final Collection<String> stopwords = Environment.getStopwords();

    for (int i = 0; i < termsCount; i++) {
      final String term = RandomValue.getString(1, 15);

      if (!stopwords.contains(term)) {
        termsBw.add(new ByteArray(term.getBytes("UTF-8")));
      }
      terms.add(term);
    }

    // double the lists
    terms.addAll(terms);
    termsBw.addAll(termsBw);

    final String queryString = index.getQueryString(terms.toArray(
            new String[termsCount]));
    final Collection<ByteArray> result = QueryUtils.getAllQueryTerms(
            queryString);

    assertTrue(msg("Not all terms returned."), result.containsAll(termsBw));

    if (termsBw.size() != result.size()) {
      @java.lang.SuppressWarnings("StringBufferWithoutInitialCapacity")
      StringBuilder sbInitial = new StringBuilder();

      for (ByteArray bw : termsBw) {
        sbInitial.append(new String(bw.bytes)).append(' ');
      }

      @java.lang.SuppressWarnings("StringBufferWithoutInitialCapacity")
      StringBuilder sbResult = new StringBuilder();

      for (ByteArray bw : result) {
        sbResult.append(ByteArrayUtil.utf8ToString(bw)).append(' ');
      }
    }

    assertEquals(msg("Initial term list and returned list differ in size."),
            termsBw.size(), result.size());
  }
}
