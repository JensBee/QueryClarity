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
import de.unihildesheim.iw.lucene.AbstractMultiIndexDataProviderTestCase;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.util.MathUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link SimplifiedClarityScore}.
 *
 * @author Jens Bertram
 */
@RunWith(Parameterized.class)
public final class SimplifiedClarityScoreTest
    extends AbstractMultiIndexDataProviderTestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      SimplifiedClarityScoreTest.class);

  /**
   * Delta allowed in clarity score calculation.
   */
  private static final double ALLOWED_SCORE_DELTA = 0.0000000001;

  /**
   * Setup test using a defined {@link IndexDataProvider}.
   *
   * @param dataProv Data provider to use
   * @param rType Data provider configuration
   */
  public SimplifiedClarityScoreTest(
      final DataProviders dataProv,
      final AbstractMultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  private SimplifiedClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return new SimplifiedClarityScore.Builder()
        .indexDataProvider(this.index);
  }

  /**
   * Test of calculateClarity method, of class SimplifiedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity()
      throws Exception {
    final String query = TestIndexDataProvider.util.getQueryString();
    final SimplifiedClarityScore instance = getInstanceBuilder().build();

    final Collection<ByteArray> queryTerms = new ArrayList<>(15);
    for (String qTerm : query.split("\\s+")) {
      queryTerms.add(new ByteArray(qTerm.getBytes("UTF-8")));
    }

    final double ql = Integer.valueOf(queryTerms.size()).doubleValue();
    final double tokenColl = Long.valueOf(referenceIndex
        .getUniqueTermsCount()).doubleValue();

    double score = 0;
    for (ByteArray term : queryTerms) {
      double qtf = 0;
      for (ByteArray aTerm : queryTerms) {
        if (aTerm.equals(term)) {
          qtf++;
        }
      }
      final double pml = qtf / ql;
      final double pcoll = referenceIndex.getTermFrequency(term)
          .doubleValue()
          / tokenColl;
      score += pml * MathUtils.log2(pml / pcoll);
    }

    final ClarityScoreResult result = instance.calculateClarity(query);

    final double maxResult = Math.max(score, result.getScore());
    final double minResult = Math.min(score, result.getScore());
    LOG.debug(msg("SCORE test={} scs={} deltaAllow={} delta={}"), score,
        result.getScore(), ALLOWED_SCORE_DELTA, maxResult - minResult);

    assertEquals(msg("Score mismatch."), score, result.getScore(),
        ALLOWED_SCORE_DELTA);
  }

}
