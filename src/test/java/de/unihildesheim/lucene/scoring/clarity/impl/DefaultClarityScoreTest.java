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

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.query.TermsQueryBuilder;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.MathUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import org.apache.lucene.search.Query;
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
 * Test for {@link DefaultClarityScore}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
@RunWith(Parameterized.class)
public final class DefaultClarityScoreTest extends MultiIndexDataProviderTestCase {

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

  @Parameters
  public static Collection<Object[]> data() {
    return getCaseParameters();
  }

  public DefaultClarityScoreTest(
          final Class<? extends IndexDataProvider> dataProv) {
    super(dataProv);
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
          final BytesWrap term) {
    final double ft = Environment.getDataProvider().getTermFrequency(term).
            doubleValue();
    final double f = Long.valueOf(Environment.getDataProvider().
            getTermFrequency()).doubleValue();
    return (1 - langmodelWeight) * (ft / f);
  }

  /**
   * Calculate the document model (pdt) for a given document & term.
   *
   * @param langmodelWeight Weight modifier to use
   * @param docModel Document model to calculate for
   * @param term Term to calculate
   * @return Calculated document model (pdt)
   */
  private static double calcDocumentModel(final double langmodelWeight,
          final DocumentModel docModel, final BytesWrap term) {
    double model;

    if (docModel.contains(term)) {
      final double ftD = docModel.termFrequency(term).doubleValue();
      final double fd = Long.valueOf(docModel.termFrequency).doubleValue();
      model = (langmodelWeight * (ftD / fd)) + calcDefaultDocumentModel(
              langmodelWeight, term);
    } else {
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
  private double calc_pc(final BytesWrap term) {
    final double result
            = CollectionMetrics.tf(term).doubleValue() / Long.
            valueOf(CollectionMetrics.tf()).doubleValue();
    return result;
  }

  /**
   * Calculate pd(t) - document model for a term.
   *
   * @param langmodelWeight Weight modifier to use
   * @param term Term to calculate for
   * @return document model for the given term
   */
  private double calc_pd(final double langModelWeight,
          final DocumentModel docModel, final BytesWrap term) {
    // number of occurences of term in document
    Long tf = docModel.termFrequency(term);
    double ftd;
    if (tf == null) {
      // term is not in document
      ftd = 0d;
    } else {
      ftd = tf.doubleValue();
    }
    // number of terms in docment
    final double fd = Long.valueOf(docModel.termFrequency).doubleValue();

    final double result = (langModelWeight * (ftd / fd)) + ((1
            - langModelWeight) * calc_pc(term));
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
  private double calc_pq(final double langModelWeight, final BytesWrap term,
          final Collection<DocumentModel> feedbackDocs,
          final Collection<BytesWrap> query) {
    double pq = 0;
    // go through all feedback documents
    for (DocumentModel docModel : feedbackDocs) {
      double docValue = 0;
      docValue = calc_pd(langModelWeight, docModel, term);
      for (BytesWrap qTerm : query) {
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
          final Collection<BytesWrap> query) {
    final Iterator<BytesWrap> termsIt;
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
    termsIt = Environment.getDataProvider().getDocumentsTermSet(fbDocIds).
            iterator();

    // go through all terms
    while (termsIt.hasNext()) {
      final BytesWrap idxTerm = termsIt.next();
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
  public void testGetQueryTerms() throws Exception {
    LOG.info("Test getQueryTerms");
    final DefaultClarityScore instance = getInstance();
    final String query = index.getQueryString();
    instance.calculateClarity(query);

    final Collection<BytesWrap> expResult = new ArrayList<>(15);
    for (String qTerm : query.split("\\s+")) {
      expResult.add(new BytesWrap(qTerm.getBytes("UTF-8")));
    }
    final Collection<BytesWrap> result = instance.getQueryTerms();

    assertEquals("Query term count mismatch.", expResult.size(), result.size());
    assertTrue("Query term collections mismatch.", result.containsAll(
            expResult));
  }

  /**
   * Test of calcDocumentModels method, of class DefaultClarityScore.
   */
  @Test
  public void testCalcDocumentModel() {
    LOG.info("Test calcDocumentModel");

    final DefaultClarityScoreConfiguration dcc
            = new DefaultClarityScoreConfiguration();
    final DefaultClarityScore instance = new DefaultClarityScore(dcc);
    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      final DocumentModel docModel = Environment.getDataProvider().
              getDocumentModel(i);
      instance.calcDocumentModel(docModel);

      Map<BytesWrap, Object> valueMap = Environment.getDataProvider().
              getTermData(DefaultClarityScore.PREFIX, i, instance.
                      getDocModelDataKey());
      for (BytesWrap term : docModel.termFreqMap.keySet()) {
        final double expResult = calcDocumentModel(dcc.getLangModelWeight(),
                docModel, term);
        if (term == null) {
          fail("***term was null");
        }
        if (valueMap == null) {
          fail("***valueMap was null");
        }
        if (valueMap.get(term) == null) {
          fail("***valueMapData was null");
        }
        assertEquals("Calculated document model value differs.", expResult,
                valueMap.get(term));
      }
    }
  }

  /**
   * Test of preCalcDocumentModels method, of class DefaultClarityScore.
   */
  @Test
  public void testPreCalcDocumentModels() {
    LOG.info("Test preCalcDocumentModels");

    final DefaultClarityScoreConfiguration dcc
            = new DefaultClarityScoreConfiguration();
    final DefaultClarityScore instance = new DefaultClarityScore(dcc);
    instance.preCalcDocumentModels();

    LOG.info("Calculation done, testing results.");
    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      final DocumentModel docModel = Environment.getDataProvider().
              getDocumentModel(i);

      Map<BytesWrap, Object> valueMap = Environment.getDataProvider().
              getTermData(DefaultClarityScore.PREFIX, i, instance.
                      getDocModelDataKey());
      for (BytesWrap term : docModel.termFreqMap.keySet()) {
        final double expResult = calcDocumentModel(dcc.getLangModelWeight(),
                docModel, term);
        if (term == null) {
          fail("***term was null");
        }
        if (valueMap == null) {
          fail("***valueMap was null");
        }
        if (valueMap.get(term) == null) {
          fail("***valueMapData was null");
        }
        assertEquals("Calculated document model value differs. docId=" + i
                + " term=" + term + "", expResult, valueMap.get(term));
      }
    }
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity() throws Exception {
    LOG.info("Test calculateClarity");
    final String queryString = index.getQueryString();
    final Query query = TermsQueryBuilder.buildFromEnvironment(queryString);
    final DefaultClarityScoreConfiguration dcc
            = new DefaultClarityScoreConfiguration();
    final DefaultClarityScore instance = new DefaultClarityScore(dcc);

    final Collection<Integer> feedbackDocs = Feedback.getFixed(query,
            dcc.getFeedbackDocCount());
    final Collection<DocumentModel> fbDocModels = new ArrayList<>(
            feedbackDocs.size());
    for (Integer docId : feedbackDocs) {
      fbDocModels.add(Environment.getDataProvider().getDocumentModel(docId));
    }

    final ClarityScoreResult result = instance.calculateClarity(queryString,
            feedbackDocs);

    LOG.debug("Calculating reference score.");
    final double score = calc_score(dcc.getLangModelWeight(),
            fbDocModels, QueryUtils.getUniqueQueryTerms(queryString));

    LOG.debug("Scores test={} dcs={}", score, result.getScore());

    assertEquals("Clarity score mismatch.", score, result.getScore(),
            ALLOWED_SCORE_DELTA);
  }

  /**
   * Test of getDocModelDataKey method, of class DefaultClarityScore.
   */
  @Test
  public void testGetDocModelDataKey() {
    LOG.info("Test getDocModelDataKey");
    final DefaultClarityScore instance = getInstance();
    final String result = instance.getDocModelDataKey();

    if (result == null || result.isEmpty()) {
      fail("DocModelDataKey was empty or null.");
    }
  }
}
