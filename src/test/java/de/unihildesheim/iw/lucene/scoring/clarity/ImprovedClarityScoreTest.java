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
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.lucene.index.FixedTestIndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexTestUtils;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.util.BigDecimalCache;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mapdb.Fun;
import org.nevec.rjm.BigDecimalMath;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Test for {@link ImprovedClarityScore}.
 *
 * @author Jens Bertram
 */
public final class ImprovedClarityScoreTest
    extends TestCase {
  /**
   * Allowed delta in document model calculation.
   */
  private static final double DELTA_D_MOD = 0d;
  /**
   * Allowed delta in query model calculation.
   */
  private static final double DELTA_Q_MOD = Double.valueOf("9E-40");
  /**
   * Allowed delta in clarity score calculation.
   */
  private static final double DELTA_SCORE = Double.valueOf("9E-15");
  /**
   * Allowed delta in clarity score calculation. Single term calculation.
   */
  private static final double DELTA_SCORE_SINGLE = Double.valueOf("9E-14");
  /**
   * Global singleton instance of the test-index.
   */
  private static final FixedTestIndexDataProvider FIXED_INDEX =
      FixedTestIndexDataProvider.getInstance();
  /**
   * Fixed configuration for clarity calculation.
   */
  private static final ImprovedClarityScoreConfiguration ICC_CONF;
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      DefaultClarityScoreTest.class);

  /**
   * Static initializer for the global {@link
   * ImprovedClarityScoreConfiguration}.
   */
  static {
    // static configuration to match pre-calculated values
    ICC_CONF = new ImprovedClarityScoreConfiguration();
    ICC_CONF.setMaxFeedbackDocumentsCount(
        FixedTestIndexDataProvider.KnownData.DOC_COUNT);
    ICC_CONF.setFeedbackTermSelectionThreshold(0d, 1d); // includes all
    ICC_CONF.setDocumentModelParamBeta(0.6);
    ICC_CONF.setDocumentModelParamLambda(1d);
    ICC_CONF.setDocumentModelSmoothingParameter(100d);
    ICC_CONF.setMinFeedbackDocumentsCount(1);
  }

  /**
   * Test of {@link ImprovedClarityScore#getQueryModel(ByteArray)}. Run for a
   * single term.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryModel_singleTerm()
      throws Exception {

    try (ImprovedClarityScore instance = (ImprovedClarityScore)
        getInstanceBuilder().configuration(ICC_CONF).build()) {
      final String term = FixedTestIndexDataProvider.KnownData
          .TF_DOC_0.entrySet().iterator().next().getKey();
      final ByteArray termBa =
          new ByteArray(term.getBytes(StandardCharsets.UTF_8));
      final List<ByteArray> qTermsBa = Collections.singletonList(termBa);
      final Set<String> qTerms = Collections.singleton(term);
      final Set<Integer> fbDocIds = Collections.singleton(0);

      final Map<ByteArray, Integer> qTermsMap = Collections.singletonMap(
          termBa, 1);
      instance.testSetValues(fbDocIds, qTermsMap);

      final BigDecimal result = instance.getQueryModel(termBa);
      final BigDecimal expected = calculateQueryModel(term, qTerms, fbDocIds);

      Assert.assertEquals("Single term query model value mismatch.",
          expected.doubleValue(), result.doubleValue(), DELTA_SCORE_SINGLE);
    }
  }

  /**
   * Get an instance builder for the {@link ImprovedClarityScore} loaded with
   * default values.
   *
   * @return Builder initialized with default values
   * @throws IOException Thrown on low-level I/O errors related to the Lucene
   * index
   */
  private static ImprovedClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return (ImprovedClarityScore.Builder) new ImprovedClarityScore.Builder()
        .indexDataProvider(FIXED_INDEX)
        .dataPath(FixedTestIndexDataProvider.DATA_DIR.getPath())
        .indexReader(FixedTestIndexDataProvider.TMP_IDX.getReader())
        .createCache("test-" + RandomValue.getString(16))
        .analyzer(IndexTestUtils.getAnalyzer())
        .temporary();
  }

  /**
   * Calculate the query model for a set of term and feedback documents.
   *
   * @param term Term to calculate the model for
   * @param queryTerms Set of query terms
   * @param feedbackDocumentIds Ids of documents to use as feedback
   * @return Model value
   * @throws Exception Any exception thrown indicates an error
   */
  private static BigDecimal calculateQueryModel(final String term,
      final Collection<String> queryTerms,
      final Iterable<Integer> feedbackDocumentIds)
      throws Exception {
    double modelValue = 0d;
    final Collection<String> calcTerms = new ArrayList<>(queryTerms.size() + 1);
    calcTerms.addAll(queryTerms);
    calcTerms.add(term);

    for (final Integer docId : feedbackDocumentIds) {
      double modelValuePart = 1d;
      for (final String cTerm : calcTerms) {
        Double model = KnownData.D_MODEL.get(Fun.t2(docId, cTerm));
        if (model == null) {
          // term not in document, calculate ad-hoc
          model = calculateMissingDocumentModel(docId, cTerm);
        }
        modelValuePart *= model;
      }
      modelValue += modelValuePart;
    }
    return BigDecimalCache.get(modelValue);
  }

  /**
   * Calculate the document model for a term not contained in a document.
   *
   * @param docId Id of the document whose model to get
   * @param term Term to calculate the model for
   * @return Model value for document & term
   * @throws Exception Any exception thrown indicates an error
   */
  private static double calculateMissingDocumentModel(final int docId,
      final String term)
      throws Exception {
    // relative term index frequency
    final BigDecimal termRelIdxFreq = FIXED_INDEX.getRelativeTermFrequency(new
        ByteArray(term.getBytes(StandardCharsets.UTF_8)));

    final Map<String, Integer> tfMap = FixedTestIndexDataProvider.KnownData
        .getDocumentTfMap(docId);
    final int termsInDoc = tfMap.size();

    // frequency of all terms in document
    int docTermFreq = 0;
    for (final Integer freq : tfMap.values()) {
      docTermFreq += freq;
    }

    // smoothed term document frequency
    final double smoothedTerm =
        (ICC_CONF.getDocumentModelSmoothingParameter() *
            termRelIdxFreq.doubleValue()) /
            ((double) docTermFreq + (
                ICC_CONF.getDocumentModelSmoothingParameter() *
                    (double) termsInDoc));
    // final model
    final double model =
        (ICC_CONF.getDocumentModelParamLambda() *
            ((ICC_CONF.getDocumentModelParamBeta() * smoothedTerm) +
                ((1d - ICC_CONF.getDocumentModelParamBeta()) * termRelIdxFreq
                    .doubleValue()))
        ) + ((1d - ICC_CONF.getDocumentModelParamLambda()) *
            termRelIdxFreq.doubleValue());
    assert model > 0d;
    return model;
  }

  /**
   * Test of {@link ImprovedClarityScore#getQueryModel(ByteArray)}. Random
   * terms.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryModel_random()
      throws Exception {

    try (ImprovedClarityScore instance = (ImprovedClarityScore)
        getInstanceBuilder().configuration(ICC_CONF).build()) {
      // use all documents for feedback
      final Set<Integer> fbDocIds =
          FixedTestIndexDataProvider.getDocumentIdsSet();

      // some random terms from the index will make up a query
      final Tuple.Tuple2<List<String>, List<ByteArray>> randQTerms =
          FixedTestIndexDataProvider.getRandomIndexTerms();
      final List<ByteArray> qTerms = randQTerms.b;
      final List<String> qTermsStr = randQTerms.a;

      final Map<ByteArray, Integer> qTermsMap = new HashMap<>(qTerms.size());
      for (final ByteArray qTerm : qTerms) {
        qTermsMap.put(qTerm, 1);
      }
      instance.testSetValues(fbDocIds, qTermsMap);

      // compare calculations for all terms in index
      final Iterator<ByteArray> termsIt = FIXED_INDEX.getTermsIterator();
      while (termsIt.hasNext()) {
        final ByteArray term = termsIt.next();
        final String termStr = ByteArrayUtils.utf8ToString(term);

        final BigDecimal result = instance.getQueryModel(term);

        // calculate expected result
        final BigDecimal expected =
            calculateQueryModel(termStr, qTermsStr, fbDocIds);

        Assert.assertEquals("Query model value differs.",
            expected.doubleValue(), result.doubleValue(),
            DELTA_Q_MOD);
      }
    }
  }

  /**
   * Test of {@link ImprovedClarityScore#getQueryModel(ByteArray)}. Fixed
   * terms.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testGetQueryModel_fixed()
      throws Exception {

    try (ImprovedClarityScore instance = (ImprovedClarityScore)
        getInstanceBuilder().configuration(ICC_CONF).build()) {
      // use all documents for feedback
      final Set<Integer> fbDocIds = FixedTestIndexDataProvider
          .getDocumentIdsSet();

      // fixed terms from the index will make up a query
      final List<String> sortedIdxTerms = FIXED_INDEX.getSortedTermList();
      final int termCount = 3;
      final Collection<ByteArray> qTerms = new ArrayList<>(termCount);
      final Collection<String> qTermsStr = new ArrayList<>(termCount);
      for (int i = 0; i < termCount; i++) {
        qTerms.add(new ByteArray(
            sortedIdxTerms.get(i).getBytes(StandardCharsets.UTF_8)));
        qTermsStr.add(sortedIdxTerms.get(i));
      }

      final Map<ByteArray, Integer> qTermsMap = new HashMap<>(qTerms.size());
      for (final ByteArray qTerm : qTerms) {
        qTermsMap.put(qTerm, 1);
      }
      instance.testSetValues(fbDocIds, qTermsMap);

      // compare calculations for all terms in index
      final Iterator<ByteArray> termsIt = FIXED_INDEX.getTermsIterator();
      while (termsIt.hasNext()) {
        final ByteArray term = termsIt.next();
        final String termStr = ByteArrayUtils.utf8ToString(term);

        final BigDecimal result = instance.getQueryModel(term);

        // calculate expected result
        final BigDecimal expected =
            calculateQueryModel(termStr, qTermsStr, fbDocIds);

        Assert.assertEquals("Query model value differs.",
            expected.doubleValue(), result.doubleValue(),
            DELTA_Q_MOD);
      }
    }
  }

  private Map.Entry<ByteArray, Long> mkEntry(final ByteArray term,
      final long val) {
    return new Map.Entry<ByteArray, Long>() {
      @Override
      public ByteArray getKey() {
        return term;
      }

      @Override
      public Long getValue() {
        return val;
      }

      @Override
      public Long setValue(final Long value) {
        return null;
      }
    };
  }

  /**
   * Test of {@link ImprovedClarityScore#calculateClarity(String)}. Run for a
   * single term.
   *
   * @throws Exception Any exception thrown indicates an error TODO: document
   * frequency values ignored for feedback terms
   */
  @Test
  public void testCalculateClarity_singleTerm()
      throws Exception {
    try (ImprovedClarityScore instance = (ImprovedClarityScore)
        getInstanceBuilder().configuration(ICC_CONF).build()) {
      final String term = FixedTestIndexDataProvider.KnownData
          .TF_DOC_0.entrySet().iterator().next().getKey();
      System.out.println("Term=" + term);

      // use all documents for feedback
      final Set<Integer> fbDocIds = FixedTestIndexDataProvider
          .getDocumentIdsSet();
      final Map<ByteArray, Integer> qTermsMap = Collections.singletonMap(new
          ByteArray(term.getBytes(StandardCharsets.UTF_8)), 1);
      instance.testSetValues(fbDocIds, qTermsMap);

      final ImprovedClarityScore.Result result =
          instance.calculateClarity(term);
      final BigDecimal expected = calculateScore(instance, result);

      Assert.assertEquals("Single term score value differs.",
          expected.doubleValue(), result.getScore(), DELTA_SCORE_SINGLE);
    }
  }

  /**
   * Calculate the clarity score based on a result set provided by the real
   * calculation method.
   *
   * @param result Result set
   * @return Clarity score
   * @throws Exception Any exception thrown indicates an error
   */
  private static BigDecimal calculateScore(
      final ImprovedClarityScore instance,
      final ImprovedClarityScore.Result result)
      throws Exception {
    final Collection<String> qTermsStr =
        new ArrayList<>(result.getQueryTerms().size());
    for (final ByteArray qTerm : result.getQueryTerms()) {
      qTermsStr.add(ByteArrayUtils.utf8ToString(qTerm));
    }

    final Iterator<ByteArray> fbTermsIt =
        instance.testGetVocabularyProvider().get();
    // store results for normalization (pq, pc, count)
    final Collection<Fun.Tuple2<BigDecimal, BigDecimal>> dataSet =
        new ArrayList<>();

    final BigDecimal minFreq = BigDecimalCache.get(result.getConfiguration()
        .getMinFeedbackTermSelectionThreshold());
    final BigDecimal maxFreq = BigDecimalCache.get(result.getConfiguration()
        .getMaxFeedbackTermSelectionThreshold());
    Metrics.CollectionMetrics cMetrics = new Metrics(FIXED_INDEX).collection();
    while (fbTermsIt.hasNext()) {
      final ByteArray term = fbTermsIt.next();
      final BigDecimal tfc = cMetrics.relTf(term);
      if (tfc.compareTo(minFreq) >= 0
          && tfc.compareTo(maxFreq) <= 0) {
        final String termStr = ByteArrayUtils.utf8ToString(term);
        final BigDecimal pq = calculateQueryModel(termStr, qTermsStr,
            result.getFeedbackDocuments());

        dataSet.add(Fun.t2(pq, FIXED_INDEX.getRelativeTermFrequency(term)));
      }
    }

    // normalization of calculation values
    BigDecimal pqSum = BigDecimal.ZERO;
    BigDecimal pcSum = BigDecimal.ZERO;
    for (final Fun.Tuple2<BigDecimal, BigDecimal> ds : dataSet) {
      pqSum = pqSum.add(ds.a);
      pcSum = pcSum.add(ds.b);
    }

    // scoring
//    Double score = 0d;
    BigDecimal score = BigDecimal.ZERO;
    BigDecimal pq;
    BigDecimal pc;
    for (final Fun.Tuple2<BigDecimal, BigDecimal> ds : dataSet) {
      if (ds.a.compareTo(BigDecimal.ZERO) == 0 ||
          ds.b.compareTo(BigDecimal.ZERO)
              == 0) { // ds.b == 0
        // implies ds.a == 0
        continue;
      }
      pq = ds.a.divide(pqSum, MathUtils.MATH_CONTEXT);
      pc = ds.b.divide(pcSum, MathUtils.MATH_CONTEXT);

      score = score.add(
          pq.multiply(BigDecimalMath.log(pq).subtract(BigDecimalMath.log(pc)))
      );

//      score += (pq.doubleValue() *
//          (Math.log(pq.doubleValue()) - Math.log(pc.doubleValue())));
    }
    return score.divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);
//    return score / MathUtils.LOG2;
//    for (final Fun.Tuple2<BigDecimal, BigDecimal> ds : dataSet) {
//      if (ds.a.compareTo(BigDecimal.ZERO) == 0 ||
//          ds.b.compareTo(BigDecimal.ZERO) == 0) { // ds.b == 0
//        // implies ds.a == 0
//        continue;
//      }
//      pq = ds.a.divide(pqSum, MathUtils.MATH_CONTEXT);
//      pc = ds.b.divide(pcSum, MathUtils.MATH_CONTEXT);
//
//      score += (pq.doubleValue() * (Math.log(pq.doubleValue()) -
//          Math.log(pc.doubleValue())));
//    }
//    return score / MathUtils.LOG2;
  }

  /**
   * Test of {@link ImprovedClarityScore#calculateClarity(String)}.
   *
   * @throws Exception Any exception thrown indicates an error TODO: document
   * frequency values ignored for feedback terms
   */
  @Test
  public void testCalculateClarity()
      throws Exception {

    try (ImprovedClarityScore instance = (ImprovedClarityScore)
        getInstanceBuilder().configuration(ICC_CONF).build()) {
      // some random terms from the index will make up a query
      final Tuple.Tuple2<List<String>, List<ByteArray>> randQTerms =
          FixedTestIndexDataProvider.getRandomIndexTerms();
      final List<String> qTermsStr = randQTerms.a;

      // create a query string from the list of terms
      final String queryStr = StringUtils.join(qTermsStr, " ");
      // use all documents for feedback
      final Set<Integer> fbDocIds = FixedTestIndexDataProvider
          .getDocumentIdsSet();

      final Map<ByteArray, Integer> qTermsMap = new HashMap<>(
          randQTerms.b.size());
      for (final ByteArray qTerm : randQTerms.b) {
        qTermsMap.put(qTerm, 1);
      }
      instance.testSetValues(fbDocIds, qTermsMap);

      final ImprovedClarityScore.Result result = instance.calculateClarity
          (queryStr);
      final BigDecimal score = calculateScore(instance, result);

      Assert.assertEquals("Clarity score mismatch.", score.doubleValue(),
          result.getScore(),
          DELTA_SCORE);
    }
  }

  /**
   * Data dump for (pre-calculated) results.
   */
  private static final class KnownData {
    /**
     * Document model values for each document-term pair.
     */
    static final Map<Fun.Tuple2<Integer, String>, Double> D_MODEL;

    /**
     * Static initializer for document model values.
     */
    static {
      D_MODEL = new TreeMap<>();

      for (int docId = 0;
           docId < FixedTestIndexDataProvider.KnownData.DOC_COUNT; docId++) {
        final Map<String, Integer> tfMap = FixedTestIndexDataProvider.KnownData
            .getDocumentTfMap(docId);

        // frequency of all terms in document
        int docTermFreq = 0;
        for (final Integer freq : tfMap.values()) {
          docTermFreq += freq;
        }

        // number of unique terms in document
        final int termsInDoc = tfMap.size();

        for (final Map.Entry<String, Integer> tfMapEntry : tfMap.entrySet()) {
          final String term = tfMapEntry.getKey();
          final Integer termInDocFreq = tfMapEntry.getValue();

          // relative term index frequency
          final double termRelIdxFreq = FixedTestIndexDataProvider.KnownData
              .IDX_TERMFREQ.get(term).doubleValue() /
              (double) FixedTestIndexDataProvider.KnownData.TERM_COUNT;
          // smoothed term document frequency
          final double smoothedTerm =
              (termInDocFreq + (ICC_CONF.getDocumentModelSmoothingParameter() *
                  termRelIdxFreq)) /
                  ((double) docTermFreq +
                      (ICC_CONF.getDocumentModelSmoothingParameter() * (double)
                          termsInDoc));
          // final model
          final double model =
              (ICC_CONF.getDocumentModelParamLambda() *
                  ((ICC_CONF.getDocumentModelParamBeta() * smoothedTerm) +
                      ((1d - ICC_CONF.getDocumentModelParamBeta()) *
                          termRelIdxFreq))
              ) + ((1d - ICC_CONF.getDocumentModelParamLambda()) *
                  termRelIdxFreq);

          D_MODEL.put(Fun.t2(docId, term), model);
        }
      }
    }
  }
}
