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
import de.unihildesheim.iw.lucene.index.IndexTestUtils;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Test for {@link SimpleTermsQuery}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("ParameterizedParametersStaticCollection")
@RunWith(Parameterized.class)
public final class SimpleTermsQueryTest
    extends MultiIndexDataProviderTestCase {

  /**
   * Setup test using a defined {@link IndexDataProvider}.
   *
   * @param dataProv Data provider to use
   * @param rType Data provider configuration
   */
  public SimpleTermsQueryTest(final DataProviders dataProv,
      final RunType rType) {
    super(dataProv, rType);
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
      new SimpleTermsQuery(this.referenceIndex.getAnalyzer(), " ",
          SimpleTermsQuery.DEFAULT_OPERATOR,
          this.referenceIndex.getDocumentFields());
      Assert.fail(
          msg("Expected exception: Empty query string.", this.referenceIndex));
    } catch (final IllegalArgumentException ex) {
      // pass
    }
    try {
      new SimpleTermsQuery(this.referenceIndex.getAnalyzer(), null,
          SimpleTermsQuery.DEFAULT_OPERATOR,
          this.referenceIndex.getDocumentFields());
      Assert.fail(msg("Expected exception: Empty query string (null).",
          this.referenceIndex));
    } catch (final NullPointerException ex) {
      // pass
    }
    try {
      new SimpleTermsQuery(this.referenceIndex.getAnalyzer(), "",
          SimpleTermsQuery.DEFAULT_OPERATOR,
          this.referenceIndex.getDocumentFields());
      Assert.fail(
          msg("Expected exception: Empty query string.", this.referenceIndex));
    } catch (final IllegalArgumentException ex) {
      // pass
    }
  }

  /**
   * Test of getSTQueryObj method, of class SimpleTermsQuery.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryObj()
      throws Exception {
    final String queryStr = this.referenceIndex.getQueryString();
    final SimpleTermsQuery instance = getInstance(queryStr);
    final Collection<String> result = instance.getQueryTerms();
    final Collection<ByteArray> exp = new QueryUtils(
        this.referenceIndex.getAnalyzer(),
        TestIndexDataProvider.getIndexReader(),
        this.referenceIndex.getDocumentFields()).getAllQueryTerms(queryStr);
    final Collection<String> expResult = new ArrayList<>(exp.size());
    final Collection<String> stopwords = this.referenceIndex.getStopwords();

    for (final ByteArray ba : exp) {
      final String term = ByteArrayUtils.utf8ToString(ba);
      if (!stopwords.contains(term)) {
        expResult.add(term);
      }
    }

    Assert.assertEquals(msg("Term count differs.", this.referenceIndex),
        (long) expResult.size(), (long) result.size());
    Assert.assertTrue(msg("Not all terms present.", this.referenceIndex),
        expResult.containsAll(result));
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
    return new SimpleTermsQuery(this.referenceIndex.getAnalyzer(), query,
        SimpleTermsQuery.DEFAULT_OPERATOR,
        this.referenceIndex.getDocumentFields());
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
    final Collection<String> terms = new ArrayList<>(termsCount);
    final Collection<String> stopwords = this.referenceIndex.getStopwords();

    for (int i = 0; i < termsCount; i++) {
      final String term = RandomValue.getString(1, 15);
      if (!stopwords.contains(term)) {
        terms.add(term);
      }
    }
    final SimpleTermsQuery instance = getInstance(StringUtils.join(terms, " "));

    Assert.assertEquals(msg("Not all terms returned.", this.referenceIndex),
        (long) terms.size(), (long) instance.getQueryTerms().size());
    Assert.assertTrue(msg("Not all terms in result set.", this.referenceIndex),
        instance.getQueryTerms().containsAll(terms));
  }


  /**
   * Test of stopwords method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testBuilderSetStopWords()
      throws Exception {
    final Collection<String> newStopWords =
        new HashSet<>(this.referenceIndex.getRandomStopWords());
    final int amount = RandomValue.getInteger(10, 100);
    final Collection<String> terms = new HashSet<>(amount);

    // generate some query terms
    for (int i = 0; i < amount; ) {
      final String term = RandomValue.getString(1, 100);
      if (!newStopWords.contains(term) && terms.add(term)) {
        i++;
      }
    }

    final SimpleTermsQuery.Builder instance =
        new SimpleTermsQuery.Builder(TestIndexDataProvider.getIndexReader(),
            this.referenceIndex.getDocumentFields());
    instance.analyzer(IndexTestUtils.getAnalyzer(newStopWords));

    @SuppressWarnings("StringBufferWithoutInitialCapacity")
    final StringBuilder qb = new StringBuilder();
    for (final String t : newStopWords) {
      qb.append(t).append(' ');
    }
    for (final String t : terms) {
      qb.append(t).append(' ');
    }

    final SimpleTermsQuery stq = instance.query(qb.toString()).build();
    final Collection<String> finalTerms = new HashSet<>(stq.getQueryTerms());

    for (final String t : terms) {
      Assert.assertTrue("Term not found. t=" + t, finalTerms.contains(t));
    }
    for (final String t : newStopWords) {
      Assert.assertFalse("Stopword found. t=" + t, finalTerms.contains(t));
    }
  }

  /**
   * Test of setDocumentFields method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testBuilderSetFields()
      throws Exception {
    final Set<String> fields =
        new HashSet<>(this.referenceIndex.getRandomFields());
    final SimpleTermsQuery.Builder instance =
        new SimpleTermsQuery.Builder(TestIndexDataProvider
            .getIndexReader(), this.referenceIndex.getDocumentFields());
    instance.analyzer(this.referenceIndex.getAnalyzer());

    final String qStr =
        instance.fields(fields).query("foo").build().getQueryObj()
            .toString();
    for (final String f : fields) {
      // stupid simple & may break easily
      Assert.assertTrue("Field not found.", qStr.contains(f + ":foo"));
    }
  }

  /**
   * Test of build method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testBuilderBuild()
      throws Exception {
    final Set<String> fields =
        new HashSet<>(this.referenceIndex.getRandomFields());
    final Collection<String> stopwords = new HashSet<>(this.referenceIndex
        .getRandomStopWords());

    final SimpleTermsQuery.Builder instance =
        new SimpleTermsQuery.Builder(
            TestIndexDataProvider.getIndexReader(), fields);
    instance.analyzer(IndexTestUtils.getAnalyzer(stopwords));
    instance.boolOperator(QueryParser.Operator.OR);
    final String query = RandomValue.getString(1, 100) + " " + RandomValue.
        getString(1, 100);

    instance.query(query).build();
  }
}
