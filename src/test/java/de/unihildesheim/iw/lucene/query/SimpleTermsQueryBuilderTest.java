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
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.util.RandomValue;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link SimpleTermsQueryBuilder}.
 *
 * @author Jens Bertram
 */
public final class SimpleTermsQueryBuilderTest
    extends TestCase {

  /**
   * Test documents referenceIndex.
   */
  private static TestIndexDataProvider referenceIndex;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static final void setUpClass()
      throws Exception {
    referenceIndex =
        new TestIndexDataProvider(TestIndexDataProvider.DEFAULT_INDEX_SIZE);
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
        isInitialized());

  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static final void tearDownClass() {
    // close the test referenceIndex
    referenceIndex.dispose();
  }

  /**
   * Test of stopwords method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetStopWords()
      throws Exception {
    final Set<String> newStopWords =
        new HashSet<>(this.referenceIndex.util.getRandomStopWords());
    final int amount = RandomValue.getInteger(10, 100);
    final Collection<String> terms = new HashSet<>(amount);

    // generate some query terms
    for (int i = 0; i < amount; ) {
      final String term = RandomValue.getString(1, 100);
      if (!newStopWords.contains(term) && terms.add(term)) {
        i++;
      }
    }

    final SimpleTermsQueryBuilder instance =
        new SimpleTermsQueryBuilder(this.referenceIndex
            .getIndexReader(), this.referenceIndex.getDocumentFields());
    instance.stopwords(newStopWords);

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
      assertTrue("Term not found. t=" + t, finalTerms.contains(t));
    }
    for (final String t : newStopWords) {
      assertFalse("Stopword found. t=" + t, finalTerms.contains(t));
    }
  }

  /**
   * Test of setDocumentFields method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetFields()
      throws Exception {
    final Set<String> fields =
        new HashSet<>(this.referenceIndex.util.getRandomFields());
    final SimpleTermsQueryBuilder instance =
        new SimpleTermsQueryBuilder(this.referenceIndex
            .getIndexReader(), this.referenceIndex.getDocumentFields());

    final String qStr =
        instance.fields(fields).query("foo").build().getQueryObj()
            .toString();
    for (final String f : fields) {
      // stupid simple & may break easily
      assertTrue("Field not found.", qStr.contains(f + ":foo"));
    }
  }

  /**
   * Test of boolOperator method, of class TermsQueryBuilder.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  public void testSetBoolOperator()
      throws Exception {
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    SimpleTermsQueryBuilder instance;

    // TODO: how to check results?
    for (final QueryParser.Operator op : QueryParser.Operator.values()) {
      instance =
          new SimpleTermsQueryBuilder(this.referenceIndex.getIndexReader(),
              this.referenceIndex.getDocumentFields());
      instance.boolOperator(op).query("foo bar").build().toString();
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
    final Set<String> fields =
        new HashSet<>(this.referenceIndex.util.getRandomFields());
    final Set<String> stopwords = new HashSet<>(this.referenceIndex.util
        .getRandomStopWords());

    final SimpleTermsQueryBuilder instance =
        new SimpleTermsQueryBuilder(this.referenceIndex
            .getIndexReader(), fields);
    instance.stopwords(stopwords);
    instance.boolOperator(QueryParser.Operator.OR);
    final String query = RandomValue.getString(1, 100) + " " + RandomValue.
        getString(1, 100);

    instance.query(query).build();
  }

}
