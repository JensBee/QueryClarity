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
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        FIXED_INDEX.getRandomIndexTerms();
    final List<String> qTermsStr = randQTerms.a;

    // create a query string from the list of terms
    final String queryStr = StringUtils.join(qTermsStr, " ");

    final ClarityScoreResult result = instance.calculateClarity(queryStr);

    // calculate reference
    final int ql = qTermsStr.size(); // number of terms in query
    double score = 0d;
    final Set<String> uniqueQTerms = new HashSet<>(qTermsStr);
    for (final String qTerm : uniqueQTerms) {
      final int times = timesInCollection(qTermsStr, qTerm);
      assert times > 0;
      final double qMod = (double) times / ql; // query model
      final double relTf = // relative collection term frequency
          FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ.get(qTerm)
              .doubleValue() / FixedTestIndexDataProvider.KnownData.TERM_COUNT;
      score += qMod * MathUtils.log2(qMod / relTf);
    }
    Assert.assertEquals(score, result.getScore(), DELTA_SCORE);
  }

  private SimplifiedClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return new SimplifiedClarityScore.Builder()
        .indexReader(FixedTestIndexDataProvider.TMP_IDX.getReader())
        .analyzer(IndexTestUtils.getAnalyzer())
        .indexDataProvider(FIXED_INDEX);
  }

  /**
   * Get the amount of times a string is in a list of strings.
   *
   * @param coll String collection to search
   * @param term Term to search for
   * @return Times the term is found in the collection
   */
  private int timesInCollection(final List<String> coll,
      final String term) {
    int counter = 0;
    for (final String aTerm : coll) {
      if (term.equals(aTerm)) {
        counter++;
      }
    }
    return counter;
  }
}
