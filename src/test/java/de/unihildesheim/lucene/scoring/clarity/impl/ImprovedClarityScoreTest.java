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
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndexDataProvider;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.metrics.DocumentMetrics;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.query.TermsQueryBuilder;
import de.unihildesheim.util.MathUtils;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link ImprovedClarityScore}.
 */
@RunWith(Parameterized.class)
public final class ImprovedClarityScoreTest
        extends MultiIndexDataProviderTestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          ImprovedClarityScoreTest.class);

  /**
   * Delta allowed in clarity score calculation.
   */
  private static final double ALLOWED_SCORE_DELTA = 0.00009;

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
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return getCaseParameters();
  }

  /**
   * Setup test using a defined {@link IndexDataProvider}.
   *
   * @param dataProv Data provider to use
   * @param rType Data provider configuration
   */
  public ImprovedClarityScoreTest(
          final Class<? extends IndexDataProvider> dataProv,
          final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Calculate the document model.
   *
   * @param conf Configuration
   * @param docId Document-id
   * @param term Term
   * @return Document model value
   */
  private double calc_pdt(final ImprovedClarityScoreConfiguration conf,
          final int docId, final ByteArray term) {
    final double smoothing = conf.getDocumentModelSmoothingParameter();
    final double lambda = conf.getDocumentModelParamLambda();
    final double beta = conf.getDocumentModelParamBeta();

    final DocumentModel docModel = DocumentMetrics.getModel(docId);
    final double termFreq = docModel.metrics().tf(term);
    final double relCollFreq = CollectionMetrics.relTf(term);

    double termSum = 0;
    // get the term frequency of each term in the document
    for (Long tfTerm : docModel.termFreqMap.values()) {
      termSum += tfTerm + smoothing;
    }
    double model = (termFreq + (smoothing * relCollFreq)) / termSum;
    model = (lambda * ((beta * model) + ((1 - beta) * relCollFreq))) + ((1
            - lambda) * relCollFreq);

    return model;
  }

  /**
   * Calculate the query model.
   *
   * @param conf Configuration
   * @param term Current term
   * @param fbDocIds Feedback documents
   * @param queryTerms Query terms
   * @return Query model value
   */
  private double calc_pqt(final ImprovedClarityScoreConfiguration conf,
          final ByteArray term, final Collection<Integer> fbDocIds,
          final Collection<ByteArray> queryTerms) {
    double model = 0;
    for (Integer fbDocId : fbDocIds) {
      double aModel = calc_pdt(conf, fbDocId, term);
      for (ByteArray qTerm : queryTerms) {
        aModel *= calc_pdt(conf, fbDocId, qTerm);
      }
      model += aModel;
    }
    return model;
  }

  /**
   * Test of calculateClarity method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity() throws Exception {
    LOG.info("Test calculateClarity");
    final String query = index.getQueryString();
    final ImprovedClarityScoreConfiguration conf
            = new ImprovedClarityScoreConfiguration();
    final ImprovedClarityScore instance = new ImprovedClarityScore(conf);

    // calculate
    ImprovedClarityScore.Result result = instance.calculateClarity(query);

    LOG.info("Calculating reference score.");

    // check configuration
    assertEquals("Configuration object mismatch.", conf, result.
            getConfiguration());

    // check initial query
    assertEquals("Query mismatch.", query, result.getQueries().get(0));

    // build query
    TermsQueryBuilder qBuilder = new TermsQueryBuilder().setBoolOperator(
            QueryParser.Operator.AND);
    Query queryObj = qBuilder.buildUsingEnvironment(query);

    // retrieve initial feedback set
    Collection<Integer> feedbackDocIds = new HashSet<>(conf.
            getMaxFeedbackDocumentsCount());
    feedbackDocIds.addAll(Feedback.get(queryObj, conf.
            getMaxFeedbackDocumentsCount()));

    if (feedbackDocIds.size() < conf.getMinFeedbackDocumentsCount()) {
      assertTrue("Expecting query to be simplified.", result.
              wasQuerySimplified());
    }

    // extract terms from feedback documents
    final Collection<ByteArray> fbTerms = Environment.getDataProvider().
            getDocumentsTermSet(feedbackDocIds);

    // get document frequency threshold
    int minDf = (int) (CollectionMetrics.numberOfDocuments()
            * conf.getFeedbackTermSelectionThreshold());
    final Iterator<ByteArray> fbTermsIt = fbTerms.iterator();

    // remove terms with lower than threshold df
    while (fbTermsIt.hasNext()) {
      final ByteArray term = fbTermsIt.next();
      if (CollectionMetrics.df(term) < minDf) {
        fbTermsIt.remove();
      }
    }

    assertEquals("Feedback term count mismatch.", fbTerms.size(), result.
            getFeedbackTerms().size());
    assertTrue("Feedback terms mismatch.", fbTerms.containsAll(result.
            getFeedbackTerms()));

    double score = 0;
    final Collection<ByteArray> qTerms = QueryUtils.getAllQueryTerms(query);
    for (ByteArray fbTerm : fbTerms) {
      final double pqt = calc_pqt(conf, fbTerm, feedbackDocIds, qTerms);
      score += pqt * MathUtils.log2(pqt / CollectionMetrics.relTf(fbTerm));
    }

    LOG.debug("Scores test={} ics={}", score, result.getScore());
    assertEquals(score, result.getScore(), ALLOWED_SCORE_DELTA);
  }
}
