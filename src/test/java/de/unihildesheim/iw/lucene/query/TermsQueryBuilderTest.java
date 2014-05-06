/*
 * Copyright (C) 2014 Jens Bertram
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

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.Environment;
import de.unihildesheim.iw.lucene.index.IndexTestUtil;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.util.RandomValue;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.junit.*;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.*;

/**
 * Test for {@link TermsQueryBuilder}.
 *
 * @author Jens Bertram
 */
public final class TermsQueryBuilderTest
    extends TestCase {

  /**
   * Test documents index.
   */
  private static TestIndexDataProvider index;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static final void setUpClass()
      throws Exception {
    index = new TestIndexDataProvider(
        TestIndexDataProvider.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
        isInitialized());

  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static final void tearDownClass() {
    // close the test index
    index.dispose();
  }

  /**
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public final void setUp()
      throws Exception {
    Environment.clear();
    index.setupEnvironment(null, null);
  }

  /**
   * Test of setStopWords method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetStopWords()
      throws Exception {
    final Collection<String> newStopWords =
        IndexTestUtil.getRandomStopWords(
            index);
    final int amount = RandomValue.getInteger(10, 100);
    final Collection<String> terms = new HashSet<>(amount);

    // generate some query terms
    for (int i = 0; i < amount; ) {
      final String term = RandomValue.getString(1, 100);
      if (!newStopWords.contains(term) && terms.add(term)) {
        i++;
      }
    }

    final TermsQueryBuilder instance = new TermsQueryBuilder();
    instance.setStopWords(newStopWords);

    @SuppressWarnings("StringBufferWithoutInitialCapacity")
    final StringBuilder qb = new StringBuilder();
    for (String t : newStopWords) {
      qb.append(t).append(' ');
    }
    for (String t : terms) {
      qb.append(t).append(' ');
    }

    final SimpleTermsQuery stq =
        instance.buildUsingEnvironment(qb.toString());
    final Collection<String> finalTerms =
        new HashSet<>(stq.getQueryTerms());

    for (String t : terms) {
      assertTrue("Term not found. t=" + t, finalTerms.contains(t));
    }
    for (String t : newStopWords) {
      assertFalse("Stopword found. t=" + t, finalTerms.contains(t));
    }
  }

  /**
   * Test of setFields method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetFields()
      throws Exception {
    final Collection<String> fields = IndexTestUtil.getRandomFields(index);
    final String[] newFields = fields.toArray(new String[fields.size()]);
    final TermsQueryBuilder instance = new TermsQueryBuilder();

    final String qStr = instance.setFields(newFields).buildUsingEnvironment(
        "foo").getQueryObj().toString();
    for (String f : fields) {
      // stupid simple & may break easily
      assertTrue("Field not found.", qStr.contains(f + ":foo"));
    }
  }

  /**
   * Test of setBoolOperator method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  public void testSetBoolOperator()
      throws Exception {
    TermsQueryBuilder instance;

    // TODO: how to check results?
    for (QueryParser.Operator op : QueryParser.Operator.values()) {
      instance = new TermsQueryBuilder();
      instance.setBoolOperator(op).
          buildUsingEnvironment("foo bar").toString();
    }
  }

  /**
   * Test of buildFromEnvironment method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testBuildFromEnvironment()
      throws Exception {
    final Collection<String> fields = IndexTestUtil.getRandomFields(index);
    final Collection<String> stopwords = IndexTestUtil.getRandomStopWords(
        index);

    // generate some query terms
    final int amount = RandomValue.getInteger(10, 100);
    final Collection<String> terms = new HashSet<>(amount);

    for (int i = 0; i < amount; ) {
      final String term = RandomValue.getString(1, 100);
      if (!stopwords.contains(term) && terms.add(term)) {
        i++;
      }
    }

    Environment.clear();
    index.setupEnvironment(index, fields, stopwords);

    TermsQueryBuilder instance = new TermsQueryBuilder();
    instance.setStopWords(stopwords);

    @SuppressWarnings("StringBufferWithoutInitialCapacity")
    StringBuilder qb = new StringBuilder();
    for (String t : stopwords) {
      qb.append(t).append(' ');
    }
    for (String t : terms) {
      qb.append(t).append(' ');
    }

    SimpleTermsQuery stq = instance.buildUsingEnvironment(qb.toString());
    Collection<String> finalTerms = new HashSet<>(stq.getQueryTerms());

    for (String t : terms) {
      assertTrue("Term not found. t=" + t, finalTerms.contains(t));
    }
    for (String t : stopwords) {
      assertFalse("Stopword found. t=" + t, finalTerms.contains(t));
    }

    final String qStr = stq.getQueryObj().toString();
    for (String f : fields) {
      // stupid simple & may break easily
      assertTrue("Field not found.", qStr.contains(f + ":"));
    }
  }

  /**
   * Test of buildUsingEnvironment method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testBuildUsingEnvironment()
      throws Exception {
    TermsQueryBuilder instance;
    final Collection<String> fields = IndexTestUtil.getRandomFields(index);
    final Collection<String> stopwords = IndexTestUtil.getRandomStopWords(
        index);

    final String query = RandomValue.getString(1, 100) + " " + RandomValue.
        getString(
            1,
            100);

    instance = new TermsQueryBuilder();
    instance.buildUsingEnvironment(query);

    Environment.clear();
    index.setupEnvironment(index, fields, stopwords);

    instance = new TermsQueryBuilder();
    instance.buildUsingEnvironment(query);

    Environment.clear();
    instance = new TermsQueryBuilder();
    try {
      instance.buildUsingEnvironment(query);
      fail("Expected an exception.");
    } catch (IllegalStateException ex) {

    }
  }

  /**
   * Test of build method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void testBuild()
      throws Exception {
    final Collection<String> fields = IndexTestUtil.getRandomFields(index);
    final Collection<String> stopwords = IndexTestUtil.getRandomStopWords(
        index);

    TermsQueryBuilder instance = new TermsQueryBuilder();
    instance.setFields(fields.toArray(new String[fields.size()]));
    instance.setStopWords(stopwords);
    instance.setBoolOperator(QueryParser.Operator.OR);
    final String query = RandomValue.getString(1, 100) + " " + RandomValue.
        getString(
            1,
            100);

    instance.build(query);
  }

}
