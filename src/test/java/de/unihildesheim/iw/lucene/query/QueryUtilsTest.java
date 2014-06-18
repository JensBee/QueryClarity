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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Test for {@link QueryUtils}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("ParameterizedParametersStaticCollection")
@RunWith(Parameterized.class)
public final class QueryUtilsTest
    extends MultiIndexDataProviderTestCase {
  // TODO: use instances!

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      QueryUtilsTest.class);

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public QueryUtilsTest(final DataProviders dataProv, final RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Test of getUniqueQueryTerms method, of class QueryUtils.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testGetUniqueQueryTerms()
      throws Exception {
    final int termsCount = RandomValue.getInteger(3, 100);
    final Collection<ByteArray> termsBw = new HashSet<>(termsCount);
    final List<String> terms = new ArrayList<>(termsCount);
    final Set<String> stopwords = this.referenceIndex.getStopwords();

    for (int i = 0; i < termsCount; i++) {
      final String term = RandomValue.getString(1, 15);

      if (!stopwords.contains(term)) {
        termsBw.add(new ByteArray(term.getBytes("UTF-8")));
      }
      terms.add(term);
    }

    final String queryString = this.referenceIndex.getQueryString(
        terms.toArray(new String[terms.size()]));
    LOG.debug("QS->{}", queryString);
    final Set<ByteArray> result = new QueryUtils(
        this.referenceIndex.getAnalyzer(),
        TestIndexDataProvider.getIndexReader(),
        this.referenceIndex.getDocumentFields()
    ).getUniqueQueryTerms(queryString);

    // manual stopword removal
    final Iterator<ByteArray> rt = result.iterator();
    while (rt.hasNext()) {
      if (stopwords.contains(ByteArrayUtils.utf8ToString(rt.next()))) {
        rt.remove();
      }
    }

    LOG.debug("A={} B={}", termsBw, result);
    Assert.assertEquals(msg("Terms amount mismatch.", this.referenceIndex),
        (long) termsBw.size(),
        (long) result.size());
    Assert.assertTrue(msg("Term list content differs.", this.referenceIndex),
        result.containsAll(termsBw));
  }

  /**
   * Test of getAllQueryTerms method, of class QueryUtils.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testGetAllQueryTerms()
      throws Exception {
    final int termsCount = RandomValue.getInteger(3, 100);
    final Collection<ByteArray> termsBw = new ArrayList<>(termsCount);
    final List<String> terms = new ArrayList<>(termsCount);
    final Set<String> stopwords = this.referenceIndex.getStopwords();

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

    final String queryString = this.referenceIndex.getQueryString(
        terms.toArray(new String[termsCount]));
    final Collection<ByteArray> result = new QueryUtils(
        this.referenceIndex.getAnalyzer(),
        TestIndexDataProvider.getIndexReader(),
        this.referenceIndex.getDocumentFields()
    ).getAllQueryTerms(queryString);

    // manual stopword removal
    final Iterator<ByteArray> rt = result.iterator();
    while (rt.hasNext()) {
      if (stopwords.contains(ByteArrayUtils.utf8ToString(rt.next()))) {
        rt.remove();
      }
    }

    Assert.assertTrue(msg("Not all terms returned.", this.referenceIndex),
        result.containsAll(termsBw));
    Assert
        .assertEquals(msg("Initial term list and returned list differ in size.",
                this.referenceIndex),
            (long) termsBw.size(), (long) result.size());
  }
}
