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

import de.unihildesheim.TestConfig;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.MathUtils;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link SimplifiedClarityScore}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
@RunWith(Parameterized.class)
public class SimplifiedClarityScoreTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          SimplifiedClarityScoreTest.class);
  /**
   * DataProvider instance currently in use.
   */
  private final Class<? extends IndexDataProvider> dataProvType;

  /**
   * Test documents index.
   */
  private static TestIndex index;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    index = new TestIndex(TestIndex.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());
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
    Environment.clear();
    if (this.dataProvType == null) {
      index.setupEnvironment();
    } else {
      index.setupEnvironment(this.dataProvType);
    }
    index.clearTermData();
    Environment.clearAllProperties();
  }

  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = TestConfig.getDataProviderParameter();
    params.add(new Object[]{null});
    return params;
  }

  public SimplifiedClarityScoreTest(
          final Class<? extends IndexDataProvider> dataProv) {
    this.dataProvType = dataProv;
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

    final Collection<BytesWrap> queryTerms = new ArrayList(15);
    for (String qTerm : query.split("\\s+")) {
      queryTerms.add(new BytesWrap(qTerm.getBytes("UTF-8")));
    }

    final double ql = Integer.valueOf(queryTerms.size()).doubleValue();
    final double tokenColl = Long.valueOf(index.getUniqueTermsCount()).
            doubleValue();

    double score = 0;
    for (BytesWrap term : queryTerms) {
      double qtf = 0;
      for (BytesWrap aTerm : queryTerms) {
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
    assertEquals(score, result.getScore(), TestConfig.DOUBLE_ALLOWED_DELTA);
  }

}
