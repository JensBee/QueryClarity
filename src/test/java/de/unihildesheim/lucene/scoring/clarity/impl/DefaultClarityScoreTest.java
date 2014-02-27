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

import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
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
   * @throws IOException Thrown on low-level I/O errors
   * @throws ParseException
   */
  @BeforeClass
  public static void setUpClass() throws IOException, ParseException {
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
    index.reset();
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
    final double ft = index.getTermFrequency(term);
    final double f = index.getTermFrequency();
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
      final double ftD = docModel.termFrequency(term);
      final double fd = docModel.termFrequency;
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
    final double result = (double) index.getTermFrequency(term)
            / (double) index.
            getTermFrequency();
//    LOG.debug("pc({}) = {}", BytesWrapUtil.bytesWrapToString(term), result);
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
      ftd = 0;
    } else {
      ftd = (double) (long) tf;
    }
    // number of terms in docment
    final double fd = docModel.termFreqMap.size();

    final double result = (langModelWeight * (ftd / fd)) + ((1
            - langModelWeight) * calc_pc(term));
//    LOG.debug("pd({}, {}) = {}", docModel.id, BytesWrapUtil.bytesWrapToString(
//            term), result);
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
    for (DocumentModel docModel : feedbackDocs) {
      double docValue = 0;
      docValue = calc_pd(langModelWeight, docModel, term);
      for (BytesWrap qTerm : query) {
        docValue *= calc_pd(langModelWeight, docModel, qTerm);
      }
      pq += docValue;
    }
    LOG.debug("pq({}) = {}", BytesWrapUtil.bytesWrapToString(term), pq);
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
    final Iterator<BytesWrap> idxTermsIt = index.getTermsIterator();
    double score = 0;
    while (idxTermsIt.hasNext()) {
      final BytesWrap idxTerm = idxTermsIt.next();
      double termScore = 0;
      final double pq = calc_pq(langModelWeight, idxTerm, feedbackDocs, query);
      termScore = pq * (log2(pq) - log2(calc_pc(idxTerm)));
      LOG.
              debug("TermScore pq={} pc={} term={} score={}", pq, calc_pc(
                              idxTerm), BytesWrapUtil.bytesWrapToString(
                              idxTerm), termScore);
      score += termScore;
    }
    return score;
  }

  /**
   * Calculate log2 for a given value.
   *
   * @param value Value to do the calculation for
   * @return Log2 of the given value
   */
  private static double log2(final double value) {
    return Math.log(value) / Math.log(2);
  }

  /**
   * Test of getQueryTerms method, of class DefaultClarityScore.
   *
   * @throws java.io.IOException Thrown on low-level I/O errors
   * @throws org.apache.lucene.queryparser.classic.ParseException Thrown on
   * query parsing errors
   */
  @Test
  public void testGetQueryTerms() throws IOException, ParseException {
    LOG.info("Test getQueryTerms");
    final DefaultClarityScore instance = getInstance();
    final Query query = index.getQuery();
    instance.calculateClarity(query);
    final Collection<BytesWrap> expResult = QueryUtils.getQueryTerms(index.
            getReader(), query);
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
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  @Test
  public void testGetReader() throws IOException {
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

    for (int i = 0; i < index.getDocumentCount(); i++) {
      final DocumentModel docModel = index.getDocumentModel(i);
      final Collection<BytesWrap> docTerms = index.getDocumentTermSet(i);

      Map<BytesWrap, Object> valueMap = index.getTermData(
              DefaultClarityScore.PREFIX, i, instance.getDocModelDataKey());
      for (BytesWrap term : docTerms) {
        final Double expResult = calcDocumentModel(instance.
                getLangmodelWeight(), docModel, term);
        assertEquals("Calculated document model value differs. docId=" + i
                + " term=" + BytesWrapUtil.bytesWrapToString(term)
                + "", expResult, valueMap.get(term));
      }
    }
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws IOException Thrown on low-level I/O errors
   * @throws org.apache.lucene.queryparser.classic.ParseException Thrown on
   * query parsing errors
   */
  @Test
  public void testCalculateClarity() throws IOException, ParseException {
    LOG.info("ADD Test calculateClarity");
    final Query query = index.getQuery();
    final DefaultClarityScore instance = getInstance();

    final Collection<Integer> feedbackDocs = Feedback.getFixed(index.
            getReader(), query, instance.getFeedbackDocumentCount());
    final Collection<DocumentModel> fbDocModels = new ArrayList<>(
            feedbackDocs.
            size());
    for (Integer docId : feedbackDocs) {
      fbDocModels.add(index.getDocumentModel(docId));
    }

    final ClarityScoreResult result = instance.calculateClarity(query);

    final double score = calc_score(instance.getLangmodelWeight(),
            fbDocModels, QueryUtils.getQueryTerms(index.getReader(), query));

    LOG.debug("Scores {} {}", score, result.getScore());

    assertEquals("Clarity score mismatch.", score, result.getScore(), 0.001);
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
