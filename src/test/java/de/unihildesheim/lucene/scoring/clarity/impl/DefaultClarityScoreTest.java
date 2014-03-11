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
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.MathUtils;
import java.io.IOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DefaultClarityScore}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultClarityScoreTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScoreTest.class);

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
    index = new TestIndex();
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
   * Setup, run before each test.
   */
  @Before
  public void setUp() {
    // clear any external data stored to index
    index.clearTermData();
  }

  /**
   * Get a {@link DefaultClarityScore} instance setup to use the test index.
   *
   * @return Instance ready for tests
   */
  private static DefaultClarityScore getInstance() {
    try {
      return new DefaultClarityScore(index.getReader(), index);
    } catch (IOException ex) {
      fail("Failed to get DefaultClarityScore instance.");
    }
    return null;
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
    final double ft = index.getTermFrequency(term).doubleValue();
    final double f = Long.valueOf(index.getTermFrequency()).doubleValue();
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
  private static Double calcDocumentModel(final double langmodelWeight,
          final DocumentModel docModel, final BytesWrap term) {
    Double model = null;

    if (docModel.contains(term)) {
      final double ftD = docModel.termFrequency(term).doubleValue();
      final double fd = Long.valueOf(docModel.termFrequency).doubleValue();
      model = (langmodelWeight * (ftD / fd)) + calcDefaultDocumentModel(
              langmodelWeight, term);
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
    final double result = index.getTermFrequency(term).doubleValue()
            / Long.valueOf(index.getTermFrequency()).doubleValue();
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
    termsIt = index.getDocumentsTermSet(fbDocIds).iterator();


    // go through all terms in query
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

    final Collection<BytesWrap> expResult = new ArrayList(15);
    for (String qTerm : query.split("\\s+")) {
      expResult.add(new BytesWrap(qTerm.getBytes("UTF-8")));
    }
    final Collection<BytesWrap> result = instance.getQueryTerms();

    assertEquals("Query term count mismatch.", expResult.size(), result.size());
    assertTrue("Query term collections mismatch.", result.containsAll(
            expResult));
  }

  /**
   * Test of getIndexDataProvider method, of class DefaultClarityScore.
   */
  @Test
  public void testGetIndexDataProvider() {
    LOG.info("Test getIndexDataProvider");
    final DefaultClarityScore instance = getInstance();
    assertEquals("DataProvider mismatch.", instance.getIndexDataProvider(),
            index);
  }

  /**
   * Test of getReader method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetReader() throws Exception {
    LOG.info("Test getReader");
    final DefaultClarityScore instance = getInstance();
    assertNotNull("IndexReader was null.", instance.getReader());
  }

  /**
   * Test of getLanguagemodelWeight method, of class DefaultClarityScore.
   */
  @Test
  public void testGetLangmodelWeight() {
    LOG.info("Test getLangmodelWeight");
    final DefaultClarityScore instance = getInstance();
    final double weight = instance.getLangmodelWeight();
    if (weight < 0 || weight > 1) {
      fail("Weight must be in range >0, <1");
    }
  }

  /**
   * Test of calcDocumentModels method, of class DefaultClarityScore.
   */
  @Test
  public void testCalcDocumentModel() {
    LOG.info("Test calcDocumentModel");

    final DefaultClarityScore instance = getInstance();
    for (int i = 0; i < index.getDocumentCount(); i++) {
      final DocumentModel docModel = index.getDocumentModel(i);
      final Collection<BytesWrap> docTerms = index.getDocumentTermSet(i);
      instance.calcDocumentModel(docModel);

      Map<BytesWrap, Object> valueMap = index.getTermData(
              DefaultClarityScore.PREFIX, i, instance.getDocModelDataKey());
      for (BytesWrap term : docTerms) {
        final Double expResult = calcDocumentModel(instance.
                getLangmodelWeight(), docModel, term);
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

    final DefaultClarityScore instance = getInstance();
    instance.preCalcDocumentModels();

    LOG.info("Calculation done, testing results.");
    for (int i = 0; i < index.getDocumentCount(); i++) {
      final DocumentModel docModel = index.getDocumentModel(i);
      final Collection<BytesWrap> docTerms = index.getDocumentTermSet(i);

      Map<BytesWrap, Object> valueMap = index.getTermData(
              DefaultClarityScore.PREFIX, i, instance.getDocModelDataKey());
      for (BytesWrap term : docTerms) {
        final Double expResult = calcDocumentModel(instance.
                getLangmodelWeight(), docModel, term);
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
    final Query query = QueryUtils.buildQuery(index.getFields(),
            index.getQueryString());
    final DefaultClarityScore instance = getInstance();

    final Collection<Integer> feedbackDocs = Feedback.getFixed(index.
            getReader(), query, instance.getFeedbackDocumentCount());
    final Collection<DocumentModel> fbDocModels = new ArrayList<>(
            feedbackDocs.size());
    for (Integer docId : feedbackDocs) {
      fbDocModels.add(index.getDocumentModel(docId));
    }

    final ClarityScoreResult result = instance.calculateClarity(query,
            feedbackDocs);

    final double score = calc_score(instance.getLangmodelWeight(),
            fbDocModels, QueryUtils.getUniqueQueryTerms(index.getReader(),
                    query));

    LOG.debug("Scores test={} dcs={}", score, result.getScore());

    assertEquals("Clarity score mismatch.", score, result.getScore(),
            TestConfig.DOUBLE_ALLOWED_DELTA);
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

  /**
   * Test of getFeedbackDocumentCount method, of class DefaultClarityScore.
   */
  @Test
  public void testGetFeedbackDocumentCount() {
    LOG.info("Test getFeedbackDocumentCount");
    DefaultClarityScore instance = getInstance();
    final int result = instance.getFeedbackDocumentCount();

    if (result <= 0) {
      fail("Count of feedback documents was <= 0");
    }
  }

  /**
   * Test of setFeedbackDocumentCount method, of class DefaultClarityScore.
   */
  @Test
  public void testSetFeedbackDocumentCount() {
    LOG.info("Test setFeedbackDocumentCount");
    int fbDocCount = 100;
    DefaultClarityScore instance = getInstance();
    instance.setFeedbackDocumentCount(fbDocCount);

    assertEquals("Nmber of feedback documents differs.", instance.
            getFeedbackDocumentCount(), fbDocCount);
  }

}
