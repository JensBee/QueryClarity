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
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mapdb.Fun;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Test for {@link DefaultClarityScore}.
 * <p/>
 * The tests are run using the {@link FixedTestIndexDataProvider} providing a
 * static well known index. A fixed {@link DefaultClarityScoreConfiguration} is
 * used throughout the test to match pre-calculated reference values. Any
 * changes to those values may need to re-calculate those reference values.
 * <p/>
 * Further the whole set of documents in the test-index is to be used as
 * feedback documents set for all test to match the pre-calculated values.
 *
 * @author Jens Bertram
 */
public final class DefaultClarityScoreTest
    extends TestCase {

  /**
   * Allowed delta in query model calculation.
   */
  private static final double DELTA_Q_MOD = 0d;
  /**
   * Allowed delta in document model calculation.
   */
  private static final double DELTA_D_MOD = 0d;
  /**
   * Allowed delta in default document model calculation.
   */
  private static final double DELTA_N_MOD = 0d;
  /**
   * Allowed delta in clarity score calculation.
   */
  private static final double DELTA_SCORE = 0d;

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
   * Language model weighting value used for calculation.
   */
  private static final double LANG_MOD_WEIGHT = 0.6d;

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
   * Data dump for (pre-calculated) results.
   */
  private static final class KnownData {
    /**
     * Clarity scores pre-calculated for single term queries using a
     * language-model-weight of {@code 0.6} and {@code 10} feedback documents.
     */
    static final Map<String, Double> termScores = new HashMap<>
        (FixedTestIndexDataProvider.KnownData.TERM_COUNT_UNIQUE);

    /**
     * Collection model values for each term in index.
     * <p/>
     * A collection model is simply the relative term frequency.
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
            FixedTestIndexDataProvider.KnownData.TERM_COUNT);
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
           docId < FixedTestIndexDataProvider.KnownData.DOC_COUNT;
           docId++) {
        final Map<String, Integer> tfMap = FixedTestIndexDataProvider.KnownData
            .getDocumentTfMap(docId);

        // number of all terms in document
        int termsInDoc = 0;
        for (final Integer freq : tfMap.values()) {
          termsInDoc += freq;
        }

        for (final Map.Entry<String, Integer> tfMapEntry : tfMap.entrySet()) {
          // frequency of current term in document
          final int inDocFreq = tfMapEntry.getValue();
          final String term = tfMapEntry.getKey();

          // calculate final model
          double model = (LANG_MOD_WEIGHT * ((double) inDocFreq / termsInDoc))
              + ((1d - LANG_MOD_WEIGHT) * C_MODEL.get(term));
          D_MODEL.put(Fun.t2(docId, term), model);
        }
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
  private DefaultClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return new DefaultClarityScore.Builder()
        .indexDataProvider(FIXED_INDEX)
        .dataPath(FixedTestIndexDataProvider.DATA_DIR.getPath())
        .indexReader(FixedTestIndexDataProvider.TMP_IDX.getReader())
        .createCache("test-" + RandomValue.getString(16))
        .temporary();
  }

  /**
   * Get some random terms from the index. The amount of terms returned is the
   * half of all index terms at maximum.
   *
   * @return Tuple containing a set of terms as String and {@link ByteArray}
   * @throws UnsupportedEncodingException Thrown, if a query term could not be
   * encoded to {@code UTF-8}
   */
  private Tuple.Tuple2<Set<String>, Set<ByteArray>> getRandomIndexTerms()
      throws UnsupportedEncodingException {
    final int maxTerm = FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ
        .size() - 1;
    final int qTermCount = RandomValue.getInteger(0, maxTerm / 2);
    final Set<ByteArray> qTerms = new HashSet<>(qTermCount);
    final Set<String> qTermsStr = new HashSet<>(qTermCount);
    final List<String> idxTerms = new ArrayList<>(FixedTestIndexDataProvider
        .KnownData.IDX_TERMFREQ.keySet());

    for (int i = 0; i < qTermCount; i++) {
      final String term = idxTerms.get(RandomValue.getInteger(0, maxTerm));
      qTermsStr.add(term);
      qTerms.add(new ByteArray(term.getBytes("UTF-8")));
    }

    assert !qTerms.isEmpty();
    assert !qTermsStr.isEmpty();

    return Tuple.tuple2(qTermsStr, qTerms);
  }

  /**
   * Calculate the query model for a set of term and feedback documents.
   *
   * @param term Term to calculate the model for
   * @param queryTerms Set of query terms
   * @param feedbackDocumentIds Ids of documents to use as feedback
   * @return Model value
   */
  private double calculateQueryModel(final String term,
      final Set<String> queryTerms, final Set<Integer> feedbackDocumentIds) {
    double modelValue = 1d;
    final Collection<String> calcTerms = new ArrayList<>();
    calcTerms.addAll(queryTerms);
    calcTerms.add(term);
    for (final Integer docId : feedbackDocumentIds) {
      for (final String cTerm : calcTerms) {
        final Fun.Tuple2<Integer, String> dmKey = Fun.t2(docId, cTerm);
        // check, if term is in document and we should use a specific
        // document model or a default model
        if (KnownData.D_MODEL.containsKey(dmKey)) {
          // specific model
          modelValue *= KnownData.D_MODEL.get(dmKey);
        } else {
          // default model
          modelValue *= KnownData.N_MODEL.get(cTerm);
        }
      }
    }
    return modelValue;
  }

  @Test
  public void testGetDefaultDocumentModel()
      throws Exception {
    final DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build();

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

  @Test
  public void testGetDocumentModel()
      throws Exception {
    final DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build();

    for (int docId = 0; docId < FixedTestIndexDataProvider.KnownData.DOC_COUNT;
         docId++) {
      final Map<ByteArray, Double> models = instance.getDocumentModel(docId);

      for (final Map.Entry<ByteArray, Double> modelEntry : models.entrySet()) {
        double expected = KnownData.D_MODEL.get(Fun.t2(docId,
            ByteArrayUtils.utf8ToString(modelEntry.getKey())));
        Assert.assertEquals("Calculated document model value differs.",
            expected, modelEntry.getValue(), DELTA_D_MOD);
      }
    }
  }

  @Test
  public void testGetQueryModel()
      throws Exception {
    final DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build();

    // use all documents for feedback
    final Set<Integer> fbDocIds = FIXED_INDEX.getDocumentIds();

    // some random terms from the index will make up a query
    final Tuple.Tuple2<Set<String>, Set<ByteArray>> randQTerms =
        getRandomIndexTerms();
    final Set<ByteArray> qTerms = randQTerms.b;
    final Set<String> qTermsStr = randQTerms.a;

    // compare calculations for all terms in index
    final Iterator<ByteArray> termsIt = FIXED_INDEX.getTermsIterator();
    while (termsIt.hasNext()) {
      final ByteArray term = termsIt.next();
      final String termStr = ByteArrayUtils.utf8ToString(term);

      final double result = instance.getQueryModel(term, fbDocIds, qTerms);

      // calculate expected result
      double expected = calculateQueryModel(termStr, qTermsStr, fbDocIds);

      Assert.assertEquals("Query model value differs.", expected, result,
          DELTA_Q_MOD);
    }
  }

  @Test
  public void testCalculateClarity()
      throws Exception {
    final DefaultClarityScore instance = getInstanceBuilder()
        .configuration(DCC).build();
    final Metrics metrics = new Metrics(FIXED_INDEX);

    // some random terms from the index will make up a query
    final Tuple.Tuple2<Set<String>, Set<ByteArray>> randQTerms =
        getRandomIndexTerms();
    final Set<ByteArray> qTerms = randQTerms.b;
    final Set<String> qTermsStr = randQTerms.a;

    // create a query string from the list of terms
    final String queryStr = StringUtils.join(qTermsStr, " ");

    // calculate result
    final DefaultClarityScore.Result result = instance.calculateClarity
        (queryStr);

    Iterator<ByteArray> idxTermsIt;
    idxTermsIt = FIXED_INDEX.getTermsIterator();
    double score = 0d;
    while (idxTermsIt.hasNext()) {
      final String idxTerm = ByteArrayUtils.utf8ToString(idxTermsIt.next());
      final double qMod = calculateQueryModel(idxTerm, qTermsStr,
          FIXED_INDEX.getDocumentIds());
      final double relTf =
          FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ.get(idxTerm)
              .doubleValue() / FixedTestIndexDataProvider.KnownData.TERM_COUNT;
      score += qMod * MathUtils.log2(qMod / relTf);
    }

    Assert.assertEquals(score, result.getScore(), DELTA_SCORE);
  }
}
