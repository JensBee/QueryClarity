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
package de.unihildesheim.iw.lucene.scoring.clarity;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.lucene.index.FixedTestIndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexTestUtils;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Test for {@link SimplifiedClarityScore}.
 *
 * @author Jens Bertram
 */
public final class SimplifiedClarityScoreTest
    extends TestCase {

  /**
   * Allowed delta in clarity score calculation.
   */
  private static final double DELTA_SCORE = Double.valueOf("9E-15");

  /**
   * Global singleton instance of the test-index.
   */
  private static final FixedTestIndexDataProvider FIXED_INDEX =
      FixedTestIndexDataProvider.getInstance();

  /**
   * Test of calculateClarity method, of class SimplifiedClarityScore. Run for a
   * single term.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity_singleTerm()
      throws Exception {
    final SimplifiedClarityScore instance = getInstanceBuilder().build();
    final String term = FixedTestIndexDataProvider.KnownData
        .TF_DOC_0.entrySet().iterator().next().getKey();

    final SimplifiedClarityScore.Result result =
        instance.calculateClarity(term);
    final double expected = calculateScore(result);

    Assert.assertEquals("Single term score value differs.",
        expected, result.getScore(), 0d);
  }

  /**
   * Get a builder creating a new instance.
   *
   * @return New Instance builder
   * @throws IOException Thrown on low-level I/O errors
   */
  private static SimplifiedClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return new SimplifiedClarityScore.Builder()
        .indexReader(FixedTestIndexDataProvider.TMP_IDX.getReader())
        .analyzer(IndexTestUtils.getAnalyzer())
        .indexDataProvider(FIXED_INDEX);
  }

  /**
   * Calculate the clarity score based on a result set provided by the real
   * calculation method.
   *
   * @param result Result set
   * @return Clarity score
   */
  private static double calculateScore(
      final SimplifiedClarityScore.Result result) {
    // calculate reference
    final int ql = result.getQueryTerms().size(); // number of terms in query
    final List<String> qTermsStr = new ArrayList<>(ql);
    for (final ByteArray qTerm : result.getQueryTerms()) {
      qTermsStr.add(ByteArrayUtils.utf8ToString(qTerm));
    }

    double score = 0d;
    final Iterable<String> uniqueQTerms = new HashSet<>(qTermsStr);
    for (final String qTerm : uniqueQTerms) {
      final int times = timesInCollection(qTermsStr, qTerm);
      assert times > 0;
      final double qMod = (double) times / (double) ql; // query model
      final double relTf = // relative collection term frequency
          FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ.get(qTerm)
              .doubleValue() /
              (double) FixedTestIndexDataProvider.KnownData.TERM_COUNT;
      score += qMod * MathUtils.log2(qMod / relTf);
    }
    return score;
  }

  /**
   * Get the amount of times a string is in a list of strings.
   *
   * @param coll String collection to search
   * @param term Term to search for
   * @return Times the term is found in the collection
   */
  private static int timesInCollection(final Iterable<String> coll,
      final String term) {
    int counter = 0;
    for (final String aTerm : coll) {
      if (term.equals(aTerm)) {
        counter++;
      }
    }
    return counter;
  }

  /**
   * Test of calculateClarity method, of class SimplifiedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity()
      throws Exception {
    final SimplifiedClarityScore instance = getInstanceBuilder().build();

    // some random terms from the index will make up a query
    final Tuple.Tuple2<List<String>, List<ByteArray>> randQTerms =
        FixedTestIndexDataProvider.getRandomIndexTerms();
    final List<String> qTermsStr = randQTerms.a;

    // create a query string from the list of terms
    final String queryStr = StringUtils.join(qTermsStr, " ");

    final SimplifiedClarityScore.Result result =
        instance.calculateClarity(queryStr);

    // calculate reference
    final double score = calculateScore(result);

    Assert.assertEquals(score, result.getScore(), DELTA_SCORE);
  }
}
