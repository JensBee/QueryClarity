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
import de.unihildesheim.SupportsPersistenceTestMethods;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.metrics.DocumentMetrics;
import de.unihildesheim.lucene.query.TermsQueryBuilder;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.MathUtils;
import de.unihildesheim.util.RandomValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DefaultClarityScore}.
 *
 * @author Jens Bertram
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
      fail(msg("Missing term. term=" + ByteArrayUtil.utf8ToString(term)));
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
   * @param docModel Document model
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
   * @param langModelWeight Weight modifier to use
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
   * @param langModelWeight Weight modifier to use
   * @param feedbackDocs Sampled feedback documents to use for calculation
   * @param query Query issued
   * @return Clarity score
   */
  private double calc_score(final double langModelWeight,
          final Collection<DocumentModel> feedbackDocs,
          final Collection<ByteArray> query) {
    if (langModelWeight <= 0) {
      throw new IllegalArgumentException("LangModelWeight <= 0.");
    }
    if (feedbackDocs.isEmpty()) {
      throw new IllegalArgumentException("No feedback documents.");
    }
    if (query.isEmpty()) {
      throw new IllegalArgumentException("No query terms.");
    }

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
   * Test of calcDocumentModels method, of class DefaultClarityScore.
   *
   * @throws Exception
   */
  @Test
  public void testCalcDocumentModel() throws Exception {
    final DefaultClarityScoreConfiguration dcc
            = new DefaultClarityScoreConfiguration();
    final DefaultClarityScore instance = new DefaultClarityScore(dcc);
    instance.createCache(RandomValue.getString(50));

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = DocumentMetrics.getModel(docId);
      instance.calcDocumentModel(docModel);

      Map<ByteArray, Object> valueMap = instance.testGetExtDocMan().getData(
              docId, DefaultClarityScore.DataKeys.DM.name());

      for (ByteArray term : docModel.termFreqMap.keySet()) {
        final double expResult = calcDocumentModel(dcc.getLangModelWeight(),
                docModel, term);
        assertEquals(msg("Calculated document model value differs."),
                expResult, valueMap.get(term));
      }
    }
  }

  /**
   * Test of preCalcDocumentModels method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testPreCalcDocumentModels() throws Exception {
    final DefaultClarityScoreConfiguration dcc
            = new DefaultClarityScoreConfiguration();
    final DefaultClarityScore instance = new DefaultClarityScore(dcc);
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
        final double expResult = calcDocumentModel(dcc.getLangModelWeight(),
                docModel, term);
        assertEquals(msg("Calculated document model value differs. docId="
                + docId + " term=" + ByteArrayUtil.utf8ToString(term) + " b="
                + term + " v=" + valueMap.get(term)), expResult,
                (Double) valueMap.get(term), 0d);
      }
    }
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity_String_Collection() throws Exception {
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
      fbDocModels.add(DocumentMetrics.getModel(docId));
    }

    final DefaultClarityScore.Result result = instance.calculateClarity(
            queryString, feedbackDocs);

    LOG.debug("RES: lmw={} fbSize={} qtSize={}", result.getConfiguration().
            getLangModelWeight(), fbDocModels.size(), result.getQueryTerms().
            size());
    final double score = calc_score(result.getConfiguration().
            getLangModelWeight(),
            fbDocModels, result.getQueryTerms());

    LOG.debug(msg("Scores test={} dcs={} delta={}"), score, result.getScore(),
            ALLOWED_SCORE_DELTA);

    assertEquals(msg("Clarity score mismatch."), score,
            result.getScore(), ALLOWED_SCORE_DELTA);
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity_String() throws Exception {
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

    final double score = calc_score(dcc.getLangModelWeight(),
            fbDocModels, result.getQueryTerms());

    LOG.debug(msg("Scores test={} dcs={} delta={}"), getDataProviderName(),
            score, result.getScore(), ALLOWED_SCORE_DELTA);

    assertEquals(msg("Clarity score mismatch."), score,
            result.getScore(), ALLOWED_SCORE_DELTA);
  }

  /**
   * Test of loadOrCreateCache method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testLoadOrCreateCache() throws Exception {
    SupportsPersistenceTestMethods.testLoadOrCreateCache(
            new DefaultClarityScore());
  }

  /**
   * Test of createCache method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testCreateCache() throws Exception {
    SupportsPersistenceTestMethods.testCreateCache(new DefaultClarityScore());
  }

  /**
   * Test of loadCache method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testLoadCache() throws Exception {
    boolean thrown = false;
    try {
      SupportsPersistenceTestMethods.testLoadCache(new DefaultClarityScore());
    } catch (Exception ex) {
      thrown = true;
    }
    if (!thrown) {
      fail(msg("Expected to catch an exception."));
    }
  }

  /**
   * Test of setConfiguration method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("UnnecessaryUnboxing")
  public void testSetConfiguration() throws Exception {
    final int fbDocCount = RandomValue.getInteger(0, 1000);
    final double langmodelWeight = RandomValue.getDouble(0.1, 0.9);

    final String queryString = index.getQueryString();
    DefaultClarityScoreConfiguration newConf
            = new DefaultClarityScoreConfiguration();

    newConf.setFeedbackDocCount(fbDocCount);
    newConf.setLangModelWeight(langmodelWeight);

    DefaultClarityScore instance = new DefaultClarityScore();
    instance.setConfiguration(newConf);
    final DefaultClarityScore.Result result
            = instance.calculateClarity(queryString);

    assertEquals("Configured feedback document count differs.", fbDocCount,
            result.getConfiguration().getFeedbackDocCount().intValue());
    assertEquals("Configured language model weight value differs.",
            langmodelWeight, result.getConfiguration().getLangModelWeight(), 0);
  }

  /**
   * Test of testGetExtDocMan method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testTestGetExtDocMan() throws Exception {
    DefaultClarityScore instance = new DefaultClarityScore();
    instance.createCache(RandomValue.getString(50));
    Assert.assertNotNull(instance.testGetExtDocMan());
  }

}
