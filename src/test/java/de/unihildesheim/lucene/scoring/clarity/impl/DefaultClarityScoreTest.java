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
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndexDataProvider;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.metrics.DocumentMetrics;
import de.unihildesheim.util.MathUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DefaultClarityScore}.
 */
@RunWith(Parameterized.class)
public final class DefaultClarityScoreTest
        extends MultiIndexDataProviderTestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScoreTest.class);

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
   * Get parameters for parameterized test.
   *
   * @return Test parameters
   */
  @Parameters
  public static Collection<Object[]> data() {
    return getCaseParameters();
  }

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public DefaultClarityScoreTest(
          final Class<? extends IndexDataProvider> dataProv,
          final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
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
   * Get a {@link DefaultClarityScore} instance setup to use the test index.
   *
   * @return Instance ready for tests
   */
  private static DefaultClarityScore getInstance() {
    return new DefaultClarityScore();
  }

  /**
   * Calculates a default document model (pdt), if the document does not
   * contain a given term.
   *
   * @param langmodelWeight Weight modifier to use
   * @param term Term to calculate
   * @return Calculated document model (pdt)
   */
  private static double calcDefaultDocumentModel(final double langmodelWeight,
          final ByteArray term) {
    return (1 - langmodelWeight) * CollectionMetrics.relTf(term);
  }

  /**
   * Calculate the document model (pdt) for a given document & term.
   *
   * @param langmodelWeight Weight modifier to use
   * @param docModel Document model to calculate for
   * @param term Term to calculate
   * @return Calculated document model (pdt)
   */
  private double calcDocumentModel(final double langmodelWeight,
          final DocumentModel docModel, final ByteArray term) {
    double model;

    if (docModel.contains(term)) {
      model = (docModel.metrics().relTf(term) * langmodelWeight)
              + calcDefaultDocumentModel(langmodelWeight, term);
    } else {
      LOG.error("({}) MISSING TERM!", getDataProviderName());
      model = calcDefaultDocumentModel(langmodelWeight, term);
    }
    return model;
  }

  /**
   * Calculate pc(t) - collection distribution for a term, collection model.
   *
   * @param term Term to calculate for
   * @return collection distribution for the given term
   */
  private double calc_pc(final ByteArray term) {
    return CollectionMetrics.relTf(term);
  }

  /**
   * Calculate pd(t) - document model for a term.
   *
   * @param langmodelWeight Weight modifier to use
   * @param term Term to calculate for
   * @return document model for the given term
   */
  private double calc_pd(final double langModelWeight,
          final DocumentModel docModel, final ByteArray term) {
    final double result = (langModelWeight * docModel.metrics().relTf(term))
            + ((1 - langModelWeight) * calc_pc(term));
    return result;
  }

  /**
   * Calculate pq(t) - query distribution for a term, query model.
   *
   * @param langmodelWeight Weight modifier to use
   * @param term Term to calculate for
   * @param feedbackDocs Sampled feedback documents to use for calculation
   * @param query Query issued
   * @return Query distribution model for the given term
   */
  private double calc_pq(final double langModelWeight, final ByteArray term,
          final Collection<DocumentModel> feedbackDocs,
          final Collection<ByteArray> query) {
    double pq = 0;
    // go through all feedback documents
    for (DocumentModel docModel : feedbackDocs) {
      double docValue = calc_pd(langModelWeight, docModel, term);
      for (ByteArray qTerm : query) {
        docValue *= calc_pd(langModelWeight, docModel, qTerm);
      }
      pq += docValue;
    }
    return pq;
  }

  /**
   * Calculate the clarity score.
   *
   * @param langmodelWeight Weight modifier to use
   * @param feedbackDocs Sampled feedback documents to use for calculation
   * @param query Query issued
   * @return Clarity score
   */
  private double calc_score(final double langModelWeight,
          final Collection<DocumentModel> feedbackDocs,
          final Collection<ByteArray> query) {
    final Iterator<ByteArray> termsIt;
    double score = 0;

    // calculation with all collection terms
//    final Iterator<BytesWrap> termsIt = index.getTermsIterator();
    // calculation only with terms from the query
//    termsIt = query.iterator();
    // calculation with terms from feedback documents
    final Collection<Integer> fbDocIds = new HashSet<>(feedbackDocs.size());
    for (DocumentModel docModel : feedbackDocs) {
      fbDocIds.add(docModel.id);
    }
    termsIt = Environment.getDataProvider().
            getDocumentsTermSet(fbDocIds).iterator();

    // go through all terms
    while (termsIt.hasNext()) {
      final ByteArray idxTerm = termsIt.next();
      final double pq = calc_pq(langModelWeight, idxTerm, feedbackDocs, query);
      score += pq * MathUtils.log2(pq / calc_pc(idxTerm));
    }
    return score;
  }

  /**
   * Test of getQueryTerms method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  public void testGetQueryTerms() throws Exception {
//    LOG.info("Test getQueryTerms");
//    final DefaultClarityScore instance = getInstance();
//    final String query = index.getQueryString();
//    instance.calculateClarity(query);
//
//    final Collection<ByteArray> expResult = QueryUtils.getUniqueQueryTerms(
//            query);
//    final Collection<ByteArray> result = instance.getQueryTerms();
//
//    assertEquals(getDataProviderName() + ": Query term count mismatch.",
//            expResult.size(), result.size());
//    assertTrue(getDataProviderName() + ": Query term collections mismatch.",
//            result.containsAll(expResult));
  }

  /**
   * Test of calcDocumentModels method, of class DefaultClarityScore.
   */
  @Test
  @Ignore
  public void testCalcDocumentModel() {
//    LOG.info("Test calcDocumentModel");
//
//    final DefaultClarityScoreConfiguration dcc
//            = new DefaultClarityScoreConfiguration();
//    final DefaultClarityScore instance = new DefaultClarityScore(dcc);
//    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
//      final DocumentModel docModel = DocumentMetrics.getModel(i);
//      instance.calcDocumentModel(docModel);
//
//      Map<ByteArray, Object> valueMap = Environment.getDataProvider().
//              getTermData(DefaultClarityScore.IDENTIFIER, i, instance.
//                      getDocModelDataKey());
//      for (ByteArray term : docModel.termFreqMap.keySet()) {
//        final double expResult = calcDocumentModel(dcc.getLangModelWeight(),
//                docModel, term);
//        assertEquals(getDataProviderName()
//                + ": Calculated document model value differs.", expResult,
//                valueMap.get(term));
//      }
//    }
  }

  /**
   * Test of preCalcDocumentModels method, of class DefaultClarityScore.
   */
  @Test
  @Ignore
  public void testPreCalcDocumentModels() {
//    LOG.info("Test preCalcDocumentModels");
//
//    final DefaultClarityScoreConfiguration dcc
//            = new DefaultClarityScoreConfiguration();
//    final DefaultClarityScore instance = new DefaultClarityScore(dcc);
//    instance.preCalcDocumentModels();
//
//    LOG.info("({}) Calculation done, testing results.", getDataProviderName());
//    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
//      final DocumentModel docModel = DocumentMetrics.getModel(i);
//
//      Map<ByteArray, Object> valueMap = Environment.getDataProvider().
//              getTermData(DefaultClarityScore.IDENTIFIER, i, instance.
//                      getDocModelDataKey());
//      for (ByteArray term : docModel.termFreqMap.keySet()) {
//        final double expResult = calcDocumentModel(dcc.getLangModelWeight(),
//                docModel, term);
//        assertEquals(getDataProviderName()
//                + ": Calculated document model value differs. docId=" + i
//                + " term=" + ByteArrayUtil.utf8ToString(term) + " b=" + term
//                + " v=" + valueMap.get(term), expResult, (Double) valueMap.
//                get(term), 0d);
//      }
//    }
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  public void testCalculateClarity_String_Collection() throws Exception {
//    LOG.info("Test calculateClarity [queryString, feedbackDocs]");
//    final String queryString = index.getQueryString();
//    final Query query = TermsQueryBuilder.buildFromEnvironment(queryString);
//    final DefaultClarityScoreConfiguration dcc
//            = new DefaultClarityScoreConfiguration();
//    final DefaultClarityScore instance = new DefaultClarityScore(dcc);
//
//    final Collection<Integer> feedbackDocs = Feedback.getFixed(query,
//            dcc.getFeedbackDocCount());
//    final Collection<DocumentModel> fbDocModels = new ArrayList<>(
//            feedbackDocs.size());
//    for (Integer docId : feedbackDocs) {
//      fbDocModels.add(DocumentMetrics.getModel(docId));
//    }
//
//    final DefaultClarityScore.Result result = instance.calculateClarity(
//            queryString, feedbackDocs);
//
//    LOG.debug("({}) Calculating reference score.", getDataProviderName());
//    final double score = calc_score(dcc.getLangModelWeight(),
//            fbDocModels, instance.getQueryTerms());
//
//    LOG.debug("({}) Scores test={} dcs={}", getDataProviderName(), score,
//            result.getScore());
//
//    assertEquals(getDataProviderName() + ": Clarity score mismatch.", score,
//            result.getScore(), ALLOWED_SCORE_DELTA);
  }

  /**
   * Test of getDocModelDataKey method, of class DefaultClarityScore.
   */
  @Test
  @Ignore
  public void testGetDocModelDataKey() {
//    LOG.info("Test getDocModelDataKey");
//    final DefaultClarityScore instance = getInstance();
//    final String result = instance.getDocModelDataKey();
//
//    if (result == null || result.isEmpty()) {
//      fail(getDataProviderName() + ": DocModelDataKey was empty or null.");
//    }
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity_String() throws Exception {
    LOG.info("calculateClarity [query string]");
    final String queryString = index.getQueryString();
    final DefaultClarityScoreConfiguration dcc
            = new DefaultClarityScoreConfiguration();
    final DefaultClarityScore instance = new DefaultClarityScore(dcc);
    final DefaultClarityScore.Result result
            = instance.calculateClarity(queryString);

    final Collection<Integer> feedbackDocs = result.getFeedbackDocuments();
    final Collection<DocumentModel> fbDocModels = new ArrayList<>(
            feedbackDocs.size());
    for (Integer docId : feedbackDocs) {
      fbDocModels.add(DocumentMetrics.getModel(docId));
    }

    LOG.debug("({}) Calculating reference score.", getDataProviderName());
    final double score = calc_score(dcc.getLangModelWeight(),
            fbDocModels, result.getQueryTerms());

    LOG.debug("({}) Scores test={} dcs={}", getDataProviderName(), score,
            result.getScore());

    assertEquals(getDataProviderName() + ": Clarity score mismatch.", score,
            result.getScore(), ALLOWED_SCORE_DELTA);
  }
}
