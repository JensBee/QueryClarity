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
package de.unihildesheim.lucene.scoring.clarity.impl;

import de.unihildesheim.ByteArray;
import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndexDataProvider;
import de.unihildesheim.util.MathUtils;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link SimplifiedClarityScore}.
 *
 
 */
@RunWith(Parameterized.class)
public final class SimplifiedClarityScoreTest
        extends MultiIndexDataProviderTestCase {

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
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    index = new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
            isInitialized());
  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static void tearDownClass() {
    // close the test index
    index.dispose();
  }

  /**
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public void setUp() throws Exception {
    caseSetUp();
  }

  /**
   * Use all {@link IndexDataProvider}s for testing.
   *
   * @return Parameter collection
   */
  @Parameters
  public static Collection<Object[]> data() {
    return getCaseParameters();
  }

  /**
   * Setup test using a defined {@link IndexDataProvider}.
   *
   * @param dataProv Data provider to use
   * @param rType Data provider configuration
   */
  public SimplifiedClarityScoreTest(
          final Class<? extends IndexDataProvider> dataProv,
          final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Test of calculateClarity method, of class SimplifiedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity() throws Exception {
    LOG.info("Test calculateClarity");
    final String query = index.getQueryString();
    final SimplifiedClarityScore instance = new SimplifiedClarityScore();

    final Collection<ByteArray> queryTerms = new ArrayList<>(15);
    for (String qTerm : query.split("\\s+")) {
      queryTerms.add(new ByteArray(qTerm.getBytes("UTF-8")));
    }

    final double ql = Integer.valueOf(queryTerms.size()).doubleValue();
    final double tokenColl = Long.valueOf(index.getUniqueTermsCount()).
            doubleValue();

    double score = 0;
    for (ByteArray term : queryTerms) {
      double qtf = 0;
      for (ByteArray aTerm : queryTerms) {
        if (aTerm.equals(term)) {
          qtf++;
        }
      }
      final double pml = qtf / ql;
      final double pcoll = index.getTermFrequency(term).doubleValue()
              / tokenColl;
      score += pml * MathUtils.log2(pml / pcoll);
    }

    final ClarityScoreResult result = instance.calculateClarity(query);

    LOG.debug("Scores test={} scs={}", score, result.getScore());
    assertEquals(score, result.getScore(), ALLOWED_SCORE_DELTA);
  }

}
