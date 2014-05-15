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
import de.unihildesheim.iw.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.document.Feedback;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.query.TermsQueryBuilder;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.concurrent.AtomicDouble;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
  private static final double ALLOWED_SCORE_DELTA = Double.valueOf("5E-3");

  /**
   * Delta allowed in query model calculation.
   */
  private static final double ALLOWED_QMODEL_DELTA = Double.valueOf("1E-14");

  /**
   * Delta allowed in document model calculation.
   */
  private static final double ALLOWED_DMODEL_DELTA = Double.valueOf("1E-10");
  ;

  /**
   * Setup test using a defined {@link IndexDataProvider}.
   *
   * @param dataProv Data provider to use
   * @param rType Data provider configuration
   */
  public ImprovedClarityScoreTest(
      final DataProviders dataProv,
      final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  private ImprovedClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return new ImprovedClarityScore.Builder()
        .indexDataProvider(this.index)
        .dataPath(TestIndexDataProvider.reference.getDataDir())
        .indexReader(referenceIndex.getIndexReader())
        .createCache("test-" + RandomValue.getString(16))
        .temporary();
  }

  /**
   * Calculate the document model.
   *
   * @param conf Configuration
   * @param docId Document-id
   * @param term Term
   * @return Document-model value
   */
  @SuppressWarnings("checkstyle:methodname")
  private double calc_pdt(final ImprovedClarityScoreConfiguration conf,
      final int docId, final ByteArray term) {
    final Metrics metrics = new Metrics(this.index);
    final double smoothing = conf.getDocumentModelSmoothingParameter();
    final double lambda = conf.getDocumentModelParamLambda();
    final double beta = conf.getDocumentModelParamBeta();

    final DocumentModel docModel = metrics.getDocumentModel(docId);
    final double termFreq = docModel.metrics().tf(term);
    final double relCollFreq = metrics.collection.relTf(term);

    double termSum = 0;
    // get the term frequency of each term in the document
    for (final Long tfTerm : docModel.termFreqMap.values()) {
      termSum += tfTerm.doubleValue() + smoothing;
    }
    double model = (termFreq + (smoothing * relCollFreq)) / termSum;
    model = (lambda * ((beta * model) + ((1d - beta) * relCollFreq))) + ((1d
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
  @SuppressWarnings("checkstyle:methodname")
  private double calc_pqt(final ImprovedClarityScoreConfiguration conf,
      final ByteArray term, final Collection<Integer> fbDocIds,
      final Collection<ByteArray> queryTerms) {
    double model = 0;
    for (final Integer fbDocId : fbDocIds) {
      double aModel = calc_pdt(conf, fbDocId, term);
      for (final ByteArray qTerm : queryTerms) {
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
  @Ignore
  public void testCalculateClarity()
      throws Exception {
    final Metrics metrics = new Metrics(this.index);
    final String query = TestIndexDataProvider.util.getQueryString();
    final ImprovedClarityScoreConfiguration icc
        = new ImprovedClarityScoreConfiguration();
    icc.setFeedbackTermSelectionThreshold(0.1); // include most terms
    final ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(icc)
        .build();

    // calculate
    final ImprovedClarityScore.Result result = instance.calculateClarity(query);

    // check configuration
    assertEquals(msg("Configuration object mismatch."), icc, result.
        getConfiguration());

    // check initial query
    assertEquals(msg("Query mismatch."), query, result.getQueries().get(0));

    // build reference query
    final TermsQueryBuilder qBuilder = new TermsQueryBuilder(referenceIndex
        .getIndexReader(), this.index.getDocumentFields())
        .setBoolOperator(QueryParser.Operator.AND);
    final Query queryObj = qBuilder.query(query).build();
    // retrieve initial feedback set to check document availability
    final Collection<Integer> feedbackDocIds = new HashSet<>(icc.
        getMaxFeedbackDocumentsCount());
    feedbackDocIds.addAll(
        Feedback.get(referenceIndex.getIndexReader(), queryObj,
            icc.getMaxFeedbackDocumentsCount())
    );
    // check, if query must have been simplified
    if (feedbackDocIds.size() < icc.getMinFeedbackDocumentsCount()) {
      assertTrue(msg("Expecting query to be simplified."),
          result.wasQuerySimplified());
    }

    // extract terms from actually used feedback documents
    final Collection<ByteArray> fbTerms = this.index.getDocumentsTermSet(
        result.getFeedbackDocuments());

    // get document frequency threshold
    final int minDf = (int) (metrics.collection.numberOfDocuments()
        * icc.getFeedbackTermSelectionThreshold());
    final Iterator<ByteArray> fbTermsIt = fbTerms.iterator();

    // remove terms with lower than threshold df
    while (fbTermsIt.hasNext()) {
      final ByteArray term = fbTermsIt.next();
      if (metrics.collection.df(term) < minDf) {
        fbTermsIt.remove();
      }
    }

    // compare resulting terms
    assertEquals(msg("Feedback term count mismatch."), fbTerms.size(), result.
        getFeedbackTerms().size());
    assertTrue(msg("Feedback terms mismatch."), fbTerms.containsAll(result.
        getFeedbackTerms()));

    final Collection<ByteArray> qTerms = new QueryUtils(referenceIndex
        .getIndexReader(), this.index.getDocumentFields())
        .getAllQueryTerms(query);

    final AtomicDouble score = new AtomicDouble(0d);
    new Processing().setSourceAndTarget(
        new TargetFuncCall<>(
            new CollectionSource(result.getFeedbackTerms()),
            new PartialScoreCalculatorTarget(score, icc, metrics, qTerms,
                feedbackDocIds)
        )
    ).process(fbTerms.size());


//    for (final ByteArray fbTerm : fbTerms) {
//      final double pqt = calc_pqt(icc, fbTerm, feedbackDocIds, qTerms);
//      score += pqt * MathUtils.log2(pqt / metrics.collection.relTf(fbTerm));
//    }

    final double maxResult = Math.max(score.get(), result.getScore());
    final double minResult = Math.min(score.get(), result.getScore());
    LOG.debug(msg("IC-SCORE test={} ics={} deltaAllow={} delta={}"), score,
        result.getScore(), ALLOWED_SCORE_DELTA, maxResult - minResult);

    assertEquals(msg("Score mismatch."), score.get(), result.getScore(),
        ALLOWED_SCORE_DELTA);
  }

  /**
   * Test of setConfiguration method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  public void testSetConfiguration()
      throws Exception {
    final int maxFbParam = RandomValue.getInteger(1, 10);
    final int minFbParam = 1;
    final double betaParam = RandomValue.getDouble(0.1, 0.9);
    final double lambdaParam = RandomValue.getDouble(0.1, 0.9);
    final double smoothingParam = RandomValue.getDouble(0.1, 0.9);
    final double termTsParam = 0.1; // low to get matches
    final ImprovedClarityScore.QuerySimplifyPolicy qspParam
        = ImprovedClarityScore.QuerySimplifyPolicy.FIRST;

    final String queryString = TestIndexDataProvider.util.getQueryString();
    final ImprovedClarityScoreConfiguration icc
        = new ImprovedClarityScoreConfiguration();

    icc.setDocumentModelParamBeta(betaParam);
    icc.setDocumentModelParamLambda(lambdaParam);
    icc.setDocumentModelSmoothingParameter(smoothingParam);
    icc.setFeedbackTermSelectionThreshold(termTsParam);
    icc.setMaxFeedbackDocumentsCount(maxFbParam);
    icc.setMinFeedbackDocumentsCount(minFbParam);
    icc.setQuerySimplifyingPolicy(qspParam);

    final ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(icc)
        .build();

    final ImprovedClarityScore.Result result = instance.calculateClarity
        (queryString);

    final ImprovedClarityScoreConfiguration resConf = result.getConfiguration();
    assertEquals("Beta param value mismatch.", icc.
        getDocumentModelParamBeta(), resConf.getDocumentModelParamBeta());
    assertEquals("Lambda param value mismatch.", icc.
        getDocumentModelParamLambda(), resConf.
        getDocumentModelParamLambda());
    assertEquals("Smoothing param value mismatch.", icc.
        getDocumentModelSmoothingParameter(), resConf.
        getDocumentModelSmoothingParameter());
    assertEquals("Term selection threshold value mismatch.", icc.
        getFeedbackTermSelectionThreshold(), resConf.
        getFeedbackTermSelectionThreshold());
    assertEquals("Max feedback doc count value mismatch.", icc.
        getMaxFeedbackDocumentsCount(), resConf.
        getMaxFeedbackDocumentsCount());
    assertEquals("Min feedback doc count value mismatch.", icc.
        getMinFeedbackDocumentsCount(), resConf.
        getMinFeedbackDocumentsCount());
    assertEquals("Query simplifying policy mismatch.", icc.
        getQuerySimplifyingPolicy(), resConf.getQuerySimplifyingPolicy());
  }

  /**
   * Test of calcQueryModel method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  @SuppressWarnings("checkstyle:magicnumber")
  public void testCalcQueryModel()
      throws Exception {
    final Collection<ByteArray> qTerms = new QueryUtils(referenceIndex
        .getIndexReader(), this.index.getDocumentFields())
        .getAllQueryTerms(TestIndexDataProvider.util.getQueryString());
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<Integer> fbDocIds = new ArrayList<>();
    final Iterator<Integer> docIdIt = this.index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      if (RandomValue.getBoolean()) {
        fbDocIds.add(docId);
      }
    }
    final ImprovedClarityScoreConfiguration icc
        = new ImprovedClarityScoreConfiguration();
    icc.setMaxFeedbackDocumentsCount(RandomValue.getInteger(10, 100));
    final ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(icc)
        .build();

    final Collection<ByteArray> fbTerms = this.index.getDocumentsTermSet
        (fbDocIds);

    LOG.info("Validating query models.");
    new Processing().setSourceAndTarget(
        new TargetFuncCall<>(
            new CollectionSource(fbTerms),
            new QueryCalculatorTarget(instance, icc, qTerms, fbDocIds)
        )
    ).process(fbTerms.size());
  }

  /**
   * Test of preCalcDocumentModels method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  @SuppressWarnings("checkstyle:magicnumber")
  public void testPreCalcDocumentModels()
      throws Exception {
    final Metrics metrics = new Metrics(this.index);
    final ImprovedClarityScoreConfiguration icc = new
        ImprovedClarityScoreConfiguration();
    final ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(icc)
        .build();
    instance.preCalcDocumentModels();

    LOG.info("Validating document models.");
    new Processing().setSourceAndTarget(
        new TargetFuncCall<>(
            this.index.getDocumentIdSource(),
            new DocModelCalculatorTarget(metrics, instance, icc)
        )
    ).process((int) this.index.getDocumentCount());
  }

  /**
   * Test of testGetExtDocMan method, of class ImprovedClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  public void testTestGetExtDocMan()
      throws Exception {
    final ImprovedClarityScore instance = getInstanceBuilder().build();
    Assert.assertNotNull(instance.testGetExtDocMan());
  }

  /**
   * {@link Processing} {@link Target} for document model calculation.
   */
  private final class QueryCalculatorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    private final ImprovedClarityScore instance;
    private final Collection<ByteArray> qTerms;
    private final Collection<Integer> fbDocIds;
    private final ImprovedClarityScoreConfiguration icc;

    QueryCalculatorTarget(ImprovedClarityScore ics,
        ImprovedClarityScoreConfiguration icsConf, Collection<ByteArray>
        queryTerms, Collection<Integer> feedbackDocIds) {
      super();
      this.instance = ics;
      this.qTerms = queryTerms;
      this.fbDocIds = feedbackDocIds;
      this.icc = icsConf;
    }

    @Override
    public void call(final ByteArray fbTerm) {
      if (fbTerm != null) {
        final double result = instance.calcQueryModel(fbTerm, this.qTerms,
            this.fbDocIds);
        final double expected = calc_pqt(this.icc, fbTerm, this.fbDocIds,
            this.qTerms);
        assertEquals(msg("(" + getName() + ") Query model value differs."),
            expected, result, ALLOWED_QMODEL_DELTA);
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for document model calculation.
   */
  private final class DocModelCalculatorTarget
      extends TargetFuncCall.TargetFunc<Integer> {

    private final Metrics metrics;
    private final ImprovedClarityScore instance;
    private final ImprovedClarityScoreConfiguration icc;

    DocModelCalculatorTarget(Metrics m, ImprovedClarityScore ics,
        ImprovedClarityScoreConfiguration conf) {
      super();
      this.metrics = m;
      this.instance = ics;
      this.icc = conf;
    }

    @Override
    public void call(final Integer docId) {
      if (docId != null) {
        final DocumentModel docModel = metrics.getDocumentModel(docId);

        final Map<ByteArray, Object> valueMap = instance.testGetExtDocMan()
            .getData(docId, DefaultClarityScore.DataKeys.DM.name());

        for (final ByteArray term : docModel.termFreqMap.keySet()) {
          final double expResult = calc_pdt(icc, docModel.id, term);
          assertEquals(
              msg("(" + getName() + ") Calculated document model value " +
                  "differs. docId=" + docId + " term=" + ByteArrayUtils
                  .utf8ToString(term) + " b=" + term + " v=" + valueMap.get
                  (term)),
              expResult, (Double) valueMap.get(term), ALLOWED_DMODEL_DELTA
          );
        }
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for document model calculation.
   */
  private final class PartialScoreCalculatorTarget
      extends TargetFuncCall.TargetFunc<ByteArray> {

    private final ImprovedClarityScoreConfiguration icc;
    private final Metrics m;
    private final Collection<Integer> fbDocIds;
    private final Collection<ByteArray> qTerms;
    private final AtomicDouble score;

    public PartialScoreCalculatorTarget(
        final AtomicDouble target,
        final ImprovedClarityScoreConfiguration conf, final Metrics metrics,
        final Collection<ByteArray> queryTerms,
        final Collection<Integer> feedbackDocIds) {
      this.icc = conf;
      this.m = metrics;
      this.fbDocIds = feedbackDocIds;
      this.qTerms = queryTerms;
      this.score = target;
    }

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        final double pqt = calc_pqt(this.icc, term, this.fbDocIds, this.qTerms);
        score.addAndGet(pqt * MathUtils.log2(pqt / this.m.collection.relTf
            (term)));
      }
    }
  }

}
