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
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mapdb.Fun;

import java.io.IOException;
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
  private static final double DELTA_SCORE = Double.valueOf("9E-22");
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

    try (DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build()) {
      final Iterator<ByteArray> termsIt = FIXED_INDEX.getTermsIterator();
      while (termsIt.hasNext()) {
        final ByteArray term = termsIt.next();
        final double result = instance.getDefaultDocumentModel(term);
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
    return new DefaultClarityScore.Builder()
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

    try (DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build()) {
      final Collection<ByteArray> terms = new HashSet<>(10);
      for (int i = 0; i < 10; i++) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final ByteArray term = new ByteArray(RandomValue.getString(1,
            15).getBytes("UTF-8"));
        if (FIXED_INDEX.getTermFrequency(term) == 0) {
          terms.add(term);
        }
      }

      for (final ByteArray term : terms) {
        Assert.assertEquals("Value should be == 0.", 0d, instance
            .getDefaultDocumentModel(term), 0d);
      }
    }
  }

  /**
   * Test of getDocumentModel method, of class DefaultClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentModel()
      throws Exception {

    try (DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build()) {
      for (int docId = 0;
           docId < FixedTestIndexDataProvider.KnownData.DOC_COUNT;
           docId++) {
        final Map<ByteArray, Double> models = instance.getDocumentModel(docId);

        for (final Map.Entry<ByteArray, Double> modelEntry : models
            .entrySet()) {
          final double expected = KnownData.D_MODEL.get(Fun.t2(docId,
              ByteArrayUtils.utf8ToString(modelEntry.getKey())));
          Assert.assertEquals("Calculated document model value differs.",
              expected, modelEntry.getValue(), DELTA_D_MOD);
        }
      }
    }
  }

  /**
   * Test of getDocumentModel method, of class DefaultClarityScore. Test with
   * invalid document ids.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentModel_invalid()
      throws Exception {

    try (DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build()) {
      int docId;
      docId = -10;
      try {
        instance.getDocumentModel(docId);
        Assert.fail("Expected an Exception to be thrown");
      } catch (final IllegalArgumentException e) {
        // pass
      }

      docId = FixedTestIndexDataProvider.KnownData.DOC_COUNT + 1;
      try {
        instance.getDocumentModel(docId);
        Assert.fail("Expected an Exception to be thrown");
      } catch (final IllegalArgumentException e) {
        // pass
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

    try (DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build()) {
      // use all documents for feedback
      final Set<Integer> fbDocIds = FixedTestIndexDataProvider.getDocumentIds();

      // some random terms from the index will make up a query
      final Tuple.Tuple2<Set<String>, Set<ByteArray>> randQTerms =
          FixedTestIndexDataProvider.getUniqueRandomIndexTerms();
      final Set<ByteArray> qTerms = randQTerms.b;
      final Set<String> qTermsStr = randQTerms.a;

      // compare calculations for all terms in index
      final Iterator<ByteArray> termsIt = FIXED_INDEX.getTermsIterator();

      while (termsIt.hasNext()) {
        final ByteArray term = termsIt.next();
        final String termStr = ByteArrayUtils.utf8ToString(term);

        final double result = instance.getQueryModel(term, fbDocIds, qTerms);

        // calculate expected result
        final double expected =
            calculateQueryModel(termStr, qTermsStr, fbDocIds);

        Assert.assertEquals("Query model value differs.", expected, result,
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
  private static double calculateQueryModel(final String term,
      final Collection<String> queryTerms,
      final Iterable<Integer> feedbackDocumentIds) {
    double modelValue = 0d;
    final Collection<String> calcTerms = new ArrayList<>(queryTerms.size() + 1);
    calcTerms.addAll(queryTerms);
    calcTerms.add(term);
    for (final Integer docId : feedbackDocumentIds) {
      double modelValuePart = 1d;
      for (final String cTerm : calcTerms) {
        final Fun.Tuple2<Integer, String> dmKey = Fun.t2(docId, cTerm);
        // check, if term is in document and we should use a specific
        // document model or a default model
        if (KnownData.D_MODEL.containsKey(dmKey)) {
          // specific model
          modelValuePart *= KnownData.D_MODEL.get(dmKey);
        } else {
          // default model
          modelValuePart *= KnownData.N_MODEL.get(cTerm);
        }
      }
      modelValue += modelValuePart;
    }
    return modelValue;
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
    try (DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build()) {
      final String term = FixedTestIndexDataProvider.KnownData
          .TF_DOC_0.entrySet().iterator().next().getKey();
      final ByteArray termBa = new ByteArray(term.getBytes("UTF-8"));
      final Set<ByteArray> qTermsBa = Collections.singleton(termBa);
      final Set<String> qTerms = Collections.singleton(term);
      final List<Integer> fbDocIds = Collections.singletonList(0);

      final double result = instance.getQueryModel(termBa, fbDocIds, qTermsBa);
      final double expected = calculateQueryModel(term, qTerms, fbDocIds);
      Assert.assertEquals("Single term query model value mismatch.",
          expected, result, 0d);
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

    try (DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build()) {
      final String term = FixedTestIndexDataProvider.KnownData
          .TF_DOC_0.entrySet().iterator().next().getKey();
      final ByteArray termBa = new ByteArray(term.getBytes("UTF-8"));
      final Set<ByteArray> qTermsBa = Collections.singleton(termBa);
      final Set<Integer> fbDocIds = Collections.singleton(0);

      final DefaultClarityScore.Result result = instance.calculateClarity
          (fbDocIds, qTermsBa);
      final double expected = calculateScore(result);

      Assert.assertEquals("Single term score value differs.",
          expected, result.getScore(), 0d);
    }
  }

  /**
   * Calculate the clarity score based on a result set provided by the real
   * calculation method.
   *
   * @param result Result set
   * @return Clarity score
   */
  private static double calculateScore(
      final DefaultClarityScore.Result result) {
    // use the final query terms
    final Collection<String> qTermsStrFinal = new ArrayList<>(result
        .getQueryTerms().size());
    for (final ByteArray qTerm : result.getQueryTerms()) {
      qTermsStrFinal.add(ByteArrayUtils.utf8ToString(qTerm));
    }
    double score = 0d;
    for (final ByteArray qTerm : result.getQueryTerms()) {
      final String idxTerm = ByteArrayUtils.utf8ToString(qTerm);
      final double qMod = calculateQueryModel(idxTerm, qTermsStrFinal,
          result.getFeedbackDocuments());
      final double relTf = // relative collection term frequency
          FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ.get(idxTerm)
              .doubleValue() /
              (double) FixedTestIndexDataProvider.KnownData.TERM_COUNT;
      score += qMod * MathUtils.log2(qMod / relTf);
    }
    return score;
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity()
      throws Exception {

    try (DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build()) {
      // some random terms from the index will make up a query
      final Tuple.Tuple2<Set<String>, Set<ByteArray>> randQTerms =
          FixedTestIndexDataProvider.getUniqueRandomIndexTerms();
      final Set<String> qTermsStr = randQTerms.a;

      // create a query string from the list of terms
      final String queryStr = StringUtils.join(qTermsStr, " ");

      // calculate result
      final DefaultClarityScore.Result result = instance.calculateClarity
          (queryStr);

      final double score = calculateScore(result);
//      // use the final query terms
//      final Collection<String> qTermsStrFinal = new ArrayList<>(result
//          .getQueryTerms().size());
//      for (final ByteArray qTerm : result.getQueryTerms()) {
//        qTermsStrFinal.add(ByteArrayUtils.utf8ToString(qTerm));
//      }
//      double score = 0d;
//      for (final ByteArray qTerm : result.getQueryTerms()) {
//        final String idxTerm = ByteArrayUtils.utf8ToString(qTerm);
//        final double qMod = calculateQueryModel(idxTerm, qTermsStrFinal,
//            result.getFeedbackDocuments());
//        final double relTf = // relative collection term frequency
//            FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ.get(idxTerm)
//                .doubleValue() /
//                (double) FixedTestIndexDataProvider.KnownData.TERM_COUNT;
//        score += qMod * MathUtils.log2(qMod / relTf);
//      }

      Assert.assertEquals("Score mismatch.", score, result.getScore(),
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
          final double model =
              (LANG_MOD_WEIGHT * ((double) inDocFreq / (double) docTermFreq))
                  + ((1d - LANG_MOD_WEIGHT) * C_MODEL.get(term));
          D_MODEL.put(Fun.t2(docId, term), model);
        }
      }
    }
  }
}
