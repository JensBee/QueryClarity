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
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.concurrent.AtomicDouble;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import org.junit.Assert;
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
import static org.junit.Assert.fail;

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
  private static final double ALLOWED_SCORE_DELTA = 0.009;

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public DefaultClarityScoreTest(
      final DataProviders dataProv,
      final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  private DefaultClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return new DefaultClarityScore.Builder()
        .indexDataProvider(this.index)
        .dataPath(TestIndexDataProvider.reference.getDataDir())
        .indexReader(referenceIndex.getIndexReader())
        .createCache("test-" + RandomValue.getString(16))
        .temporary();
  }

  /**
   * Calculates a default document model (pdt), if the document does not contain
   * a given term.
   *
   * @param langmodelWeight Weight modifier to use
   * @param term Term to calculate
   * @return Calculated document model (pdt)
   */
  private double calcDefaultDocumentModel(final double langmodelWeight,
      final ByteArray term) {
    final Metrics metrics = new Metrics(this.index);
    return (1 - langmodelWeight) * metrics.collection.relTf(term);
  }

  /**
   * Calculate the document model (pdt) for a given document & term.
   *
   * @param langmodelWeight Weight modifier to use
   * @param docModel Document-model to calculate for
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
      fail(msg("Missing term. term=" + ByteArrayUtils.utf8ToString(term)));
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
    final Metrics metrics = new Metrics(this.index);
    return metrics.collection.relTf(term);
  }

  /**
   * Calculate pd(t) - document model for a term.
   *
   * @param langModelWeight Weight modifier to use
   * @param docModel Document-model
   * @param term Term to calculate for
   * @return document model for the given term
   */
  private double calc_pd(final double langModelWeight,
      final DocumentModel docModel, final ByteArray term) {
    return (langModelWeight * docModel.metrics().relTf(term))
        + ((1 - langModelWeight) * calc_pc(term));
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
    for (final DocumentModel docModel : feedbackDocs) {
      double docValue = calc_pd(langModelWeight, docModel, term);
      for (final ByteArray qTerm : query) {
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
      final Collection<ByteArray> query)
      throws IOException, ProcessingException {
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

    // calculation with terms from feedback documents
    final Collection<Integer> fbDocIds = new HashSet<>(feedbackDocs.size());
    for (final DocumentModel docModel : feedbackDocs) {
      fbDocIds.add(docModel.id);
    }
    termsIt = this.index.getDocumentsTermSet(fbDocIds).iterator();


    final AtomicDouble pScore = new AtomicDouble(0);
    final Collection<ByteArray> fbTerms = this.index.getDocumentsTermSet
        (fbDocIds);
    new Processing().setSourceAndTarget(
        new Target.TargetFuncCall<>(
            new CollectionSource<>(fbTerms),
            new PartialScoreCalculatorTarget(pScore, langModelWeight,
                feedbackDocs, query)
        )
    ).process(fbTerms.size());

//    // go through all terms
//    while (termsIt.hasNext()) {
//      final ByteArray idxTerm = termsIt.next();
//      final double pq = calc_pq(langModelWeight, idxTerm, feedbackDocs,
// query);
//      score += pq * MathUtils.log2(pq / calc_pc(idxTerm));
//    }
    return pScore.doubleValue();
  }

  /**
   * Test of calcDocumentModels method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void testCalcDocumentModel()
      throws Exception {
    final Metrics metrics = new Metrics(this.index);
    final DefaultClarityScoreConfiguration dcc
        = new DefaultClarityScoreConfiguration();
    final DefaultClarityScore instance = getInstanceBuilder()
        .configuration(dcc)
        .build();

    new Processing().setSourceAndTarget(
        new Target.TargetFuncCall<>(
            this.index.getDocumentIdSource(),
            new DocModelCalculatorTarget(metrics, instance, dcc)
        )
    ).process((int) this.index.getDocumentCount());

//    final Iterator<Integer> docIdIt = this.index.getDocumentIdIterator();
//    while (docIdIt.hasNext()) {
//      final int docId = docIdIt.next();
//      final DocumentModel docModel = metrics.getDocumentModel(docId);
//      instance.calcDocumentModel(docModel);
//
//      final Map<ByteArray, Object> valueMap = instance.testGetExtDocMan()
//          .getData(docId, DefaultClarityScore.DataKeys.DM.name());
//
//      for (final ByteArray term : docModel.termFreqMap.keySet()) {
//        final double expResult = calcDocumentModel(dcc.getLangModelWeight(),
//            docModel, term);
//        assertEquals(msg("Calculated document model value differs."),
//            expResult, valueMap.get(term));
//      }
//    }
  }

  /**
   * Test of preCalcDocumentModels method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("checkstyle:magicnumber")
  public void testPreCalcDocumentModels()
      throws Exception {
    final Metrics metrics = new Metrics(this.index);
    final DefaultClarityScoreConfiguration dcc
        = new DefaultClarityScoreConfiguration();
    final DefaultClarityScore instance = getInstanceBuilder()
        .configuration(dcc)
        .build();
    instance.preCalcDocumentModels();

    new Processing().setSourceAndTarget(
        new Target.TargetFuncCall<>(
            this.index.getDocumentIdSource(),
            new DocModelCalculatorTarget(metrics, instance, dcc)
        )
    ).process((int) this.index.getDocumentCount());

//    final Iterator<Integer> docIdIt = this.index.getDocumentIdIterator();
//    while (docIdIt.hasNext()) {
//      final int docId = docIdIt.next();
//      final DocumentModel docModel = metrics.getDocumentModel(docId);
//
//      final Map<ByteArray, Object> valueMap = instance.testGetExtDocMan()
//          .getData(docId, DefaultClarityScore.DataKeys.DM.name());
//
//      for (final ByteArray term : docModel.termFreqMap.keySet()) {
//        final double expResult = calcDocumentModel(dcc.getLangModelWeight(),
//            docModel, term);
//        assertEquals(msg("Calculated document model value differs. docId="
//                + docId + " term=" + ByteArrayUtils.utf8ToString(term) +
//                " b="
//                + term + " v=" + valueMap.get(term)), expResult,
//            (Double) valueMap.get(term), 0d
//        );
//      }
//    }
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity()
      throws Exception {
    final Metrics metrics = new Metrics(this.index);
    final String queryString = TestIndexDataProvider.util.getQueryString();
    final DefaultClarityScoreConfiguration dcc
        = new DefaultClarityScoreConfiguration();
    final DefaultClarityScore instance = getInstanceBuilder()
        .configuration(dcc)
        .build();

    final DefaultClarityScore.Result result
        = instance.calculateClarity(queryString);

    final Collection<Integer> feedbackDocs = result.getFeedbackDocuments();
    final Collection<DocumentModel> fbDocModels = new ArrayList<>(
        feedbackDocs.size());
    for (final Integer docId : feedbackDocs) {
      fbDocModels.add(metrics.getDocumentModel(docId));
    }

    final double score = calc_score(dcc.getLangModelWeight(),
        fbDocModels, result.getQueryTerms());

    final double maxResult = Math.max(score, result.getScore());
    final double minResult = Math.min(score, result.getScore());
    LOG.debug(msg("DC-SCORE test={} dcs={} deltaAllow={} delta={}"), score,
        result.getScore(), ALLOWED_SCORE_DELTA, maxResult - minResult);

    assertEquals(msg("Clarity score mismatch."), score,
        result.getScore(), ALLOWED_SCORE_DELTA);
  }

  /**
   * Test of testGetExtDocMan method, of class DefaultClarityScore.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testTestGetExtDocMan()
      throws Exception {
    final DefaultClarityScore instance = getInstanceBuilder().build();
    Assert.assertNotNull(instance.testGetExtDocMan());
  }

  /**
   * {@link Processing} {@link Target} for document model calculation.
   */
  private final class DocModelCalculatorTarget
      extends Target.TargetFunc<Integer> {

    private final Metrics metrics;
    private final DefaultClarityScore instance;
    private final DefaultClarityScoreConfiguration dcc;

    DocModelCalculatorTarget(Metrics m, DefaultClarityScore dcs,
        DefaultClarityScoreConfiguration conf) {
      super();
      this.metrics = m;
      this.instance = dcs;
      this.dcc = conf;
    }

    @Override
    public void call(final Integer docId) {
      if (docId != null) {
        final DocumentModel docModel = this.metrics.getDocumentModel(docId);
        this.instance.calcDocumentModel(docModel);

        final Map<ByteArray, Object> valueMap = this.instance.testGetExtDocMan()
            .getData(docId, DefaultClarityScore.DataKeys.DM.name());

        for (final ByteArray term : docModel.termFreqMap.keySet()) {
          final double expResult = calcDocumentModel(this.dcc
              .getLangModelWeight(), docModel, term);
          assertEquals(msg("Calculated document model value differs."),
              expResult, valueMap.get(term));
        }
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for calculating parts of the reference
   * clarity score.
   */
  private final class PartialScoreCalculatorTarget
      extends Target.TargetFunc<ByteArray> {

    private final AtomicDouble target;
    private final double lmw;
    private final Collection<DocumentModel> fbDocs;
    private final Collection<ByteArray> q;

    PartialScoreCalculatorTarget(final AtomicDouble pScore,
        final double langModelWeight,
        final Collection<DocumentModel> feedbackDocs,
        final Collection<ByteArray> query) {
      super();
      this.target = pScore;
      this.lmw = langModelWeight;
      this.fbDocs = feedbackDocs;
      this.q = query;
    }

    @Override
    public void call(final ByteArray idxTerm) {
      if (idxTerm != null) {
        final double pq = calc_pq(this.lmw, idxTerm, this.fbDocs, this.q);
        this.target.addAndGet(pq * MathUtils.log2(pq / calc_pc(idxTerm)));
      }
    }
  }
}
