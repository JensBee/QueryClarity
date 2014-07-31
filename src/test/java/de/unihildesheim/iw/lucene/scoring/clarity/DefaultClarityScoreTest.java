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
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.FixedTestIndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexTestUtils;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Test for {@link DefaultClarityScore}. <br> The tests are run using the {@link
 * FixedTestIndexDataProvider} providing a static well known index. A fixed
 * {@link DefaultClarityScoreConfiguration} is used throughout the test to match
 * pre-calculated reference values. Any changes to those values may need to
 * re-calculate those reference values. <br> Further the whole set of documents
 * in the test-index is to be used as feedback documents set for all test to
 * match the pre-calculated values.
 *
 * @author Jens Bertram
 */
public final class DefaultClarityScoreTest
    extends TestCase {

  /**
   * Language model weighting value used for calculation.
   */
  static final double LANG_MOD_WEIGHT = 0.6d;
  /**
   * Allowed delta in query model calculation.
   */
  private static final double DELTA_Q_MOD = Double.valueOf("9E-24");
  /**
   * Allowed delta in document model calculation.
   */
  private static final double DELTA_D_MOD = Double.valueOf("0");//"9E-64");
  /**
   * Allowed delta in default document model calculation.
   */
  private static final double DELTA_N_MOD = Double.valueOf("0");//"9E-64");
  /**
   * Allowed delta in clarity score calculation.
   */
  private static final double DELTA_SCORE = Double.valueOf("9E-14");
  /**
   * Allowed delta in single term clarity score calculation.
   */
  private static final double DELTA_SCORE_SINGLE = Double.valueOf("9E-15");
  /**
   * Global singleton instance of the test-index.
   */
  private static final FixedTestIndexDataProvider FIXED_INDEX =
      FixedTestIndexDataProvider.getInstance();
  /**
   * Fixed configuration for clarity calculation.
   */
  private static final DefaultClarityScoreConfiguration DCC;
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      DefaultClarityScoreTest.class);

  /**
   * Static initializer for the global {@link DefaultClarityScoreConfiguration}.
   */
  static {
    // static configuration to match pre-calculated values
    DCC = new DefaultClarityScoreConfiguration();
    DCC.setFeedbackDocCount(FixedTestIndexDataProvider.KnownData.DOC_COUNT);
    DCC.setLangModelWeight(LANG_MOD_WEIGHT);
  }

  /**
   * Test of getDefaultDocumentModel method, of class DefaultClarityScore. Test
   * is run with all valid terms from the index.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDefaultDocumentModel()
      throws Exception {

    try (DefaultClarityScore instance = (DefaultClarityScore)
        getInstanceBuilder().configuration(DCC).build()) {
      final Iterator<ByteArray> termsIt = FIXED_INDEX.getTermsIterator();
      while (termsIt.hasNext()) {
        final ByteArray term = termsIt.next();
        final double result =
            instance.getDefaultDocumentModel(term).doubleValue();
        Assert.assertEquals(
            "Default document-model value differs.",
            KnownData.N_MODEL.get(ByteArrayUtils.utf8ToString(term)),
            result, DELTA_N_MOD
        );
      }
    }
  }

  /**
   * Get an instance builder for the {@link DefaultClarityScore} loaded with
   * default values.
   *
   * @return Builder initialized with default values
   * @throws IOException Thrown on low-level I/O errors related to the Lucene
   * index
   */
  private static DefaultClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return (DefaultClarityScore.Builder) new DefaultClarityScore.Builder()
        .indexDataProvider(FIXED_INDEX)
        .dataPath(FixedTestIndexDataProvider.DATA_DIR.getPath())
        .indexReader(FixedTestIndexDataProvider.TMP_IDX.getReader())
        .createCache("test-" + RandomValue.getString(16))
        .analyzer(IndexTestUtils.getAnalyzer())
        .temporary();
  }

  /**
   * Test of getDefaultDocumentModel method, of class DefaultClarityScore. Test
   * is run with random generated terms that may not exist in the index.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDefaultDocumentModel_illegalTerms()
      throws Exception {

    try (DefaultClarityScore instance = (DefaultClarityScore)
        getInstanceBuilder()
            .configuration(DCC).build()) {
      final Collection<ByteArray> terms = new HashSet<>(10);
      for (int i = 0; i < 10; i++) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final ByteArray term = new ByteArray(RandomValue.getString(1,
            15).getBytes(StandardCharsets.UTF_8));
        if (FIXED_INDEX.getTermFrequency(term) == 0) {
          terms.add(term);
        }
      }

      for (final ByteArray term : terms) {
        try {
          instance.getDefaultDocumentModel(term);
        } catch (final NullPointerException e) {
          // pass
        }
      }
    }
  }

  /**
   * Test of getQueryModel method, of class DefaultClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryModel()
      throws Exception {

    try (DefaultClarityScore instance = (DefaultClarityScore)
        getInstanceBuilder().configuration(DCC).build()) {
      // use all documents for feedback
      final Set<Integer> fbDocIds = FixedTestIndexDataProvider
          .getDocumentIdsSet();

      // some random terms from the index will make up a query
      final Tuple.Tuple2<Set<String>, Set<ByteArray>> randQTerms =
          FixedTestIndexDataProvider.getUniqueRandomIndexTerms();
      final Collection<ByteArray> qTerms = randQTerms.b;
      final Set<String> qTermsStr = randQTerms.a;
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
   * Calculate the query model for a set of term and feedback documents.
   *
   * @param term Term to calculate the model for
   * @param queryTerms Set of query terms
   * @param feedbackDocumentIds Ids of documents to use as feedback
   * @return Model value
   */
  private static BigDecimal calculateQueryModel(final String term,
      final Collection<String> queryTerms,
      final Iterable<Integer> feedbackDocumentIds) {
    BigDecimal bdModelValue = BigDecimal.ZERO;
    final Collection<String> calcTerms = new ArrayList<>(queryTerms.size() + 1);
    calcTerms.addAll(queryTerms);
    calcTerms.add(term);

    for (final Integer docId : feedbackDocumentIds) {
      BigDecimal bdModelValuePart = BigDecimal.ONE;
      for (final String cTerm : calcTerms) {
        final Fun.Tuple2<Integer, String> dmKey = Fun.t2(docId, cTerm);
        // check, if term is in document and we should use a specific
        // document model or a default model
        if (KnownData.D_MODEL.containsKey(dmKey)) {
          // specific model
          bdModelValuePart = bdModelValuePart.multiply(
              BigDecimal.valueOf(KnownData.D_MODEL.get(dmKey)));
        } else {
          // default model
          bdModelValuePart = bdModelValuePart.multiply(
              BigDecimal.valueOf(KnownData.N_MODEL.get(cTerm)));
        }
      }
      bdModelValue = bdModelValue.add(bdModelValuePart);
    }
    return bdModelValue;
  }

  /**
   * Test of getQueryModel method, of class DefaultClarityScore. Run for a
   * single term.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryModel_singleTerm()
      throws Exception {
    try (DefaultClarityScore instance = (DefaultClarityScore)
        getInstanceBuilder().configuration(DCC).build()) {
      final String term = FixedTestIndexDataProvider.KnownData
          .TF_DOC_0.entrySet().iterator().next().getKey();
      final ByteArray termBa =
          new ByteArray(term.getBytes(StandardCharsets.UTF_8));
      final Set<String> qTerms = Collections.singleton(term);
      final Set<Integer> fbDocIds = Collections.singleton(0);

      final Map<ByteArray, Integer> qTermsMap = Collections.singletonMap
          (termBa, 1);
      instance.testSetValues(fbDocIds, qTermsMap);

      final BigDecimal result = instance.getQueryModel(termBa);
      final BigDecimal expected = calculateQueryModel(term, qTerms, fbDocIds);
      Assert.assertEquals("Single term query model value mismatch. " +
              "qTerm=" + term + " fbDocs=" + fbDocIds + " qTermsMap=" +
              qTermsMap,
          expected.doubleValue(), result.doubleValue(), 0d);
    }
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore. Run for a
   * single term.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity_SingleTerm()
      throws Exception {

    try (DefaultClarityScore instance = (DefaultClarityScore)
        getInstanceBuilder().configuration(DCC).build()) {
      final String term = FixedTestIndexDataProvider.KnownData
          .TF_DOC_0.entrySet().iterator().next().getKey();
      final ByteArray termBa =
          new ByteArray(term.getBytes(StandardCharsets.UTF_8));
      final Set<Integer> fbDocIds = Collections.singleton(0);

      final Map<ByteArray, Integer> qTermsMap = Collections.singletonMap
          (termBa, 1);
      instance.testSetValues(fbDocIds, qTermsMap);

      final DefaultClarityScore.Result result = instance.calculateClarity();
      final BigDecimal expected = calculateScore(instance, result);

      Assert.assertEquals("Single term score value differs.",
          expected.doubleValue(), result.getScore(), DELTA_SCORE_SINGLE);
    }
  }

  /**
   * Calculate the clarity score based on a result set provided by the real
   * calculation method.
   *
   * @param instance Scoring instance
   * @param result Result set
   * @return Clarity score
   */
  private static BigDecimal calculateScore(
      final DefaultClarityScore instance,
      final DefaultClarityScore.Result result)
      throws DataProviderException {
    // use the final query terms
    final Collection<String> qTermsStrFinal = new ArrayList<>(result
        .getQueryTerms().size());
    for (final ByteArray qTerm : result.getQueryTerms()) {
      qTermsStrFinal.add(ByteArrayUtils.utf8ToString(qTerm));
    }

    final Iterator<ByteArray> fbTermsIt = instance.testGetVocabularyProvider
        ().get();
    // store results for normalization (pq, pc, count)
    final List<Fun.Tuple2<BigDecimal, BigDecimal>> dataSet = new ArrayList<>();

    while (fbTermsIt.hasNext()) {
      final ByteArray term = fbTermsIt.next();
      final String idxTerm = ByteArrayUtils.utf8ToString(term);

      final BigDecimal qMod = calculateQueryModel(idxTerm, qTermsStrFinal,
          result.getFeedbackDocuments());

      dataSet.add(Fun.t2(qMod, FIXED_INDEX.getRelativeTermFrequency(term)));
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
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity()
      throws Exception {

    try (DefaultClarityScore instance = (DefaultClarityScore)
        getInstanceBuilder().configuration(DCC).build()) {
      // some random terms from the index will make up a query
      final Tuple.Tuple2<Set<String>, Set<ByteArray>> randQTerms =
          FixedTestIndexDataProvider.getUniqueRandomIndexTerms();
      final Set<String> qTermsStr = randQTerms.a;

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

      // calculate result
      final DefaultClarityScore.Result result = instance.calculateClarity
          (queryStr);

      final BigDecimal score = calculateScore(instance, result);

      Assert.assertEquals("Score mismatch.", score.doubleValue(),
          result.getScore(),
          DELTA_SCORE);
    }
  }

  /**
   * Data dump for (pre-calculated) results.
   */
  private static final class KnownData {
    /**
     * Collection model values for each term in index. <br> A collection model
     * is simply the relative term frequency.
     */
    static final Map<String, Double> C_MODEL;

    /**
     * Static initializer for collection model values.
     */
    static {
      C_MODEL = new HashMap<>(FixedTestIndexDataProvider.KnownData
          .TERM_COUNT_UNIQUE);
      for (final Map.Entry<String, Integer> idxTfEntry :
          FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ.entrySet()) {
        // term -> ft/F
        C_MODEL.put(idxTfEntry.getKey(), idxTfEntry.getValue().doubleValue() /
            (double) FixedTestIndexDataProvider.KnownData.TERM_COUNT);
      }
    }

    /**
     * Default document model values for documents not containing a specific
     * term.
     */
    static final Map<String, Double> N_MODEL;

    /**
     * Static initializer for default document model values.
     */
    static {
      N_MODEL = new HashMap<>(FixedTestIndexDataProvider.KnownData
          .TERM_COUNT_UNIQUE);
      for (final String term :
          FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ.keySet()) {
        N_MODEL.put(term, (1d - LANG_MOD_WEIGHT) * C_MODEL.get(term));
      }
    }

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

        for (final Map.Entry<String, Integer> tfMapEntry : tfMap.entrySet()) {
          // frequency of current term in document
          final int inDocFreq = tfMapEntry.getValue();
          final String term = tfMapEntry.getKey();

          // calculate final model
//          LOG.debug("dtf={} idf={} lmf={} cmod={}", docTermFreq,
//              LANG_MOD_WEIGHT,
//              inDocFreq, C_MODEL.get(term));

          final BigDecimal bdModel =
              BigDecimal
                  .valueOf(LANG_MOD_WEIGHT)
                  .multiply(
                      BigDecimal.valueOf((long) inDocFreq)
                          .divide(BigDecimal.valueOf((long) docTermFreq),
                              MathUtils.MATH_CONTEXT))
                  .add(
                      BigDecimal.ONE
                          .subtract(BigDecimal.valueOf(LANG_MOD_WEIGHT))
                          .multiply(
                              BigDecimal.valueOf(C_MODEL.get(term)))
                  );
          D_MODEL.put(Fun.t2(docId, term), bdModel.doubleValue());
        }
      }
    }
  }
}
