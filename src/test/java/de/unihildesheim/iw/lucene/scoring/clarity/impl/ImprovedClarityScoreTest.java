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
package de.unihildesheim.iw.lucene.scoring.clarity.impl;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.SupportsPersistenceTestMethods;
import de.unihildesheim.iw.lucene.Environment;
import de.unihildesheim.iw.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.document.Feedback;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.metrics.CollectionMetrics;
import de.unihildesheim.iw.lucene.metrics.DocumentMetrics;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.query.TermsQueryBuilder;
import de.unihildesheim.iw.util.ByteArrayUtil;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.RandomValue;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Test for {@link ImprovedClarityScore}.
 *
 * @author Jens Bertram
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
  private static final double ALLOWED_SCORE_DELTA = 0.005;

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
  @SuppressWarnings("checkstyle:methodname")
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
                                                                         -
                                                                         lambda) *
                                                                        relCollFreq);

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
  @SuppressWarnings("checkstyle:methodname")
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
  public void testCalculateClarity()
      throws Exception {
    final String query = index.getQueryString();
    final ImprovedClarityScoreConfiguration conf
        = new ImprovedClarityScoreConfiguration();
    final ImprovedClarityScore instance = new ImprovedClarityScore(conf);

    // calculate
    ImprovedClarityScore.Result result = instance.calculateClarity(query);

    // check configuration
    assertEquals(msg("Configuration object mismatch."), conf, result.
        getConfiguration());

    // check initial query
    assertEquals(msg("Query mismatch."), query, result.getQueries().get(0));

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
      assertTrue(msg("Expecting query to be simplified."), result.
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

    assertEquals(msg("Feedback term count mismatch."), fbTerms.size(), result.
        getFeedbackTerms().size());
    assertTrue(msg("Feedback terms mismatch."), fbTerms.containsAll(result.
        getFeedbackTerms()));

    double score = 0;
    final Collection<ByteArray> qTerms = QueryUtils.getAllQueryTerms(query);
    for (ByteArray fbTerm : fbTerms) {
      final double pqt = calc_pqt(conf, fbTerm, feedbackDocIds, qTerms);
      score += pqt * MathUtils.log2(pqt / CollectionMetrics.relTf(fbTerm));
    }

    final double maxResult = Math.max(score, result.getScore());
    final double minResult = Math.min(score, result.getScore());
    LOG.debug(msg("SCORE test={} ics={} deltaAllow={} delta={}"), score,
        result.getScore(), ALLOWED_SCORE_DELTA, maxResult - minResult);

    assertEquals(msg("Score mismatch."), score, result.getScore(),
        ALLOWED_SCORE_DELTA);
  }

  /**
   * Test of loadOrCreateCache method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testLoadOrCreateCache()
      throws Exception {
    SupportsPersistenceTestMethods.testLoadOrCreateCache(
        new ImprovedClarityScore());
  }

  /**
   * Test of createCache method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCreateCache()
      throws Exception {
    SupportsPersistenceTestMethods.testCreateCache(new ImprovedClarityScore());
  }

  /**
   * Test of loadCache method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testLoadCache()
      throws Exception {
    boolean thrown = false;
    try {
      SupportsPersistenceTestMethods.testLoadCache(new ImprovedClarityScore());
    } catch (Exception ex) {
      thrown = true;
    }
    if (!thrown) {
      fail(msg("Expected to catch an exception."));
    }
  }

  /**
   * Test of setConfiguration method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetConfiguration()
      throws Exception {
    final int maxFbParam = RandomValue.getInteger(1, 1000);
    final int minFbParam = 1;
    final double betaParam = RandomValue.getDouble(0.1, 0.9);
    final double lambdaParam = RandomValue.getDouble(0.1, 0.9);
    final double smoothingParam = RandomValue.getDouble(0.1, 0.9);
    final double termTsParam = RandomValue.getDouble(0.1, 0.9);
    final ImprovedClarityScore.QuerySimplifyPolicy qspParam
        = ImprovedClarityScore.QuerySimplifyPolicy.FIRST;

    final String queryString = index.getQueryString();
    ImprovedClarityScoreConfiguration newConf
        = new ImprovedClarityScoreConfiguration();

    newConf.setDocumentModelParamBeta(betaParam);
    newConf.setDocumentModelParamLambda(lambdaParam);
    newConf.setDocumentModelSmoothingParameter(smoothingParam);
    newConf.setFeedbackTermSelectionThreshold(termTsParam);
    newConf.setMaxFeedbackDocumentsCount(maxFbParam);
    newConf.setMinFeedbackDocumentsCount(minFbParam);
    newConf.setQuerySimplifyingPolicy(qspParam);

    ImprovedClarityScore instance = new ImprovedClarityScore();
    instance.setConfiguration(newConf);
    final ImprovedClarityScore.Result result
        = instance.calculateClarity(queryString);

    ImprovedClarityScoreConfiguration resConf = result.getConfiguration();
    assertEquals("Beta param value mismatch.", newConf.
        getDocumentModelParamBeta(), resConf.getDocumentModelParamBeta());
    assertEquals("Lambda param value mismatch.", newConf.
        getDocumentModelParamLambda(), resConf.
        getDocumentModelParamLambda());
    assertEquals("Smoothing param value mismatch.", newConf.
        getDocumentModelSmoothingParameter(), resConf.
        getDocumentModelSmoothingParameter());
    assertEquals("Term selection threshold value mismatch.", newConf.
        getFeedbackTermSelectionThreshold(), resConf.
        getFeedbackTermSelectionThreshold());
    assertEquals("Max feedback doc count value mismatch.", newConf.
        getMaxFeedbackDocumentsCount(), resConf.
        getMaxFeedbackDocumentsCount());
    assertEquals("Min feedback doc count value mismatch.", newConf.
        getMinFeedbackDocumentsCount(), resConf.
        getMinFeedbackDocumentsCount());
    assertEquals("Query simplifying policy mismatch.", newConf.
        getQuerySimplifyingPolicy(), resConf.getQuerySimplifyingPolicy());
  }

  /**
   * Test of calcQueryModel method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void testCalcQueryModel()
      throws Exception {
    Collection<ByteArray> qTerms = QueryUtils.getAllQueryTerms(index.
        getQueryString());
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    Collection<Integer> fbDocIds = new ArrayList<>();
    Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      if (RandomValue.getBoolean()) {
        fbDocIds.add(docId);
      }
    }
    ImprovedClarityScoreConfiguration icc
        = new ImprovedClarityScoreConfiguration();
    ImprovedClarityScore instance = new ImprovedClarityScore(icc);
    // a cache is needed for calculation
    instance.createCache(RandomValue.getString(50));

    Collection<ByteArray> fbTerms = index.getDocumentsTermSet(fbDocIds);

    for (ByteArray fbTerm : fbTerms) {
      final double result = instance.calcQueryModel(fbTerm, qTerms, fbDocIds);
      final double expected = calc_pqt(icc, fbTerm, fbDocIds, qTerms);
      assertEquals("Query model value differs.", expected, result,
          ALLOWED_SCORE_DELTA);
    }
  }

  /**
   * Test of preCalcDocumentModels method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void testPreCalcDocumentModels()
      throws Exception {
    ImprovedClarityScoreConfiguration icc
        = new ImprovedClarityScoreConfiguration();
    ImprovedClarityScore instance = new ImprovedClarityScore(icc);
    // a cache is needed for pre-calculation
    instance.createCache(RandomValue.getString(50));
    instance.preCalcDocumentModels();

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = DocumentMetrics.getModel(docId);

      Map<ByteArray, Object> valueMap = instance.testGetExtDocMan().getData(
          docId, DefaultClarityScore.DataKeys.DM.name());

      for (ByteArray term : docModel.termFreqMap.keySet()) {
        final double expResult = calc_pdt(icc, docModel.id, term);
        assertEquals(msg("Calculated document model value differs. docId="
                         + docId + " term=" + ByteArrayUtil.utf8ToString(term) +
                         " b="
                         + term + " v=" + valueMap.get(term)), expResult,
            (Double) valueMap.get(term), 0d
        );
      }
    }
  }

  /**
   * Test of testGetExtDocMan method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void testTestGetExtDocMan()
      throws Exception {
    ImprovedClarityScore instance = new ImprovedClarityScore();
    instance.createCache(RandomValue.getString(50));
    Assert.assertNotNull(instance.testGetExtDocMan());
  }
}
