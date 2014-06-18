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
import de.unihildesheim.iw.lucene.query.RuleBasedTryExactTermsQuery;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
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
   * Default configuration.
   */
  static final ImprovedClarityScoreConfiguration.Conf ICC;
  /**
   * Allowed delta in document model calculation.
   */
  private static final double DELTA_D_MOD = 0d;
  /**
   * Allowed delta in query model calculation.
   */
  private static final double DELTA_Q_MOD = 0d;
  /**
   * Allowed delta in clarity score calculation.
   */
  private static final double DELTA_SCORE = Double.valueOf("9E-127");
  /**
   * Allowed delta in clarity score calculation. Single term calculation.
   */
  private static final double DELTA_SCORE_SINGLE = Double.valueOf("9E-17");
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
   * Static initializer for the global {@link
   * ImprovedClarityScoreConfiguration}.
   */
  static {
    // static configuration to match pre-calculated values
    ICC_CONF = new ImprovedClarityScoreConfiguration();
    ICC_CONF.setMaxFeedbackDocumentsCount(
        FixedTestIndexDataProvider.KnownData.DOC_COUNT);
    ICC_CONF.setFeedbackTermSelectionThreshold(0d); // includes all
    ICC_CONF.setDocumentModelParamBeta(0.6);
    ICC_CONF.setDocumentModelParamLambda(1d);
    ICC_CONF.setDocumentModelSmoothingParameter(100d);
    ICC_CONF.setMinFeedbackDocumentsCount(1);
    ICC_CONF.setQuerySimplifyingPolicy(
        RuleBasedTryExactTermsQuery.RelaxRule.HIGHEST_DOCFREQ);

    ICC = ICC_CONF.compile();
  }

  /**
   * Test of {@link ImprovedClarityScore#getDocumentModel(int)}.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentModel()
      throws Exception {

    try (ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(ICC_CONF).build()) {
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
   * Get an instance builder for the {@link ImprovedClarityScore} loaded with
   * default values.
   *
   * @return Builder initialized with default values
   * @throws IOException Thrown on low-level I/O errors related to the Lucene
   * index
   */
  private static ImprovedClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return new ImprovedClarityScore.Builder()
        .indexDataProvider(FIXED_INDEX)
        .dataPath(FixedTestIndexDataProvider.DATA_DIR.getPath())
        .indexReader(FixedTestIndexDataProvider.TMP_IDX.getReader())
        .createCache("test-" + RandomValue.getString(16))
        .analyzer(IndexTestUtils.getAnalyzer())
        .temporary();
  }

  /**
   * Test of {@link ImprovedClarityScore#getDocumentModel}. Test with invalid
   * document ids.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetDocumentModel_invalid()
      throws Exception {

    try (ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(ICC_CONF).build()) {
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
   * Test of {@link ImprovedClarityScore#getQueryModel(ByteArray, List, Set)}.
   * Run for a single term.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryModel_singleTerm()
      throws Exception {

    try (ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(ICC_CONF).build()) {
      final String term = FixedTestIndexDataProvider.KnownData
          .TF_DOC_0.entrySet().iterator().next().getKey();
      final ByteArray termBa = new ByteArray(term.getBytes("UTF-8"));
      final List<ByteArray> qTermsBa = Collections.singletonList(termBa);
      final Set<String> qTerms = Collections.singleton(term);
      final Set<Integer> fbDocIds = Collections.singleton(0);

      final double result = instance.getQueryModel(termBa, qTermsBa, fbDocIds);
      final double expected = calculateQueryModel(term, qTerms, fbDocIds);

      Assert.assertEquals("Single term query model value mismatch.",
          expected, result, 0d);
    }
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
  private static double calculateQueryModel(final String term,
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
    return modelValue;
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
    final double termRelIdxFreq = FIXED_INDEX.getRelativeTermFrequency(new
        ByteArray(term.getBytes("UTF-8")));

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
        (ICC.smoothing * termRelIdxFreq) /
            ((double) docTermFreq + (ICC.smoothing * (double) termsInDoc));
    // final model
    final double model =
        (ICC.lambda *
            ((ICC.beta * smoothedTerm) +
                ((1d - ICC.beta) * termRelIdxFreq))
        ) + ((1d - ICC.lambda) * termRelIdxFreq);
    assert model > 0d;
    return model;
  }

  /**
   * Test of {@link ImprovedClarityScore#getQueryModel(ByteArray, List, Set)}.
   * Random terms.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetQueryModel_random()
      throws Exception {

    try (ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(ICC_CONF).build()) {
      // use all documents for feedback
      final Set<Integer> fbDocIds = FixedTestIndexDataProvider.getDocumentIds();

      // some random terms from the index will make up a query
      final Tuple.Tuple2<List<String>, List<ByteArray>> randQTerms =
          FixedTestIndexDataProvider.getRandomIndexTerms();
      final List<ByteArray> qTerms = randQTerms.b;
      final List<String> qTermsStr = randQTerms.a;

      // compare calculations for all terms in index
      final Iterator<ByteArray> termsIt = FIXED_INDEX.getTermsIterator();
      while (termsIt.hasNext()) {
        final ByteArray term = termsIt.next();
        final String termStr = ByteArrayUtils.utf8ToString(term);

        final double result = instance.getQueryModel(term, qTerms, fbDocIds);

        // calculate expected result
        final double expected =
            calculateQueryModel(termStr, qTermsStr, fbDocIds);

        Assert.assertEquals("Query model value differs.", expected, result,
            DELTA_Q_MOD);
      }
    }
  }

  /**
   * Test of {@link ImprovedClarityScore#getQueryModel(ByteArray, List, Set)}.
   * Fixed terms.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testGetQueryModel_fixed()
      throws Exception {

    try (ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(ICC_CONF).build()) {
      // use all documents for feedback
      final Set<Integer> fbDocIds = FixedTestIndexDataProvider.getDocumentIds();

      // fixed terms from the index will make up a query
      final List<String> sortedIdxTerms = FIXED_INDEX.getSortedTermList();
      final int termCount = 3;
      final List<ByteArray> qTerms = new ArrayList<>(termCount);
      final Collection<String> qTermsStr = new ArrayList<>(termCount);
      for (int i = 0; i < termCount; i++) {
        qTerms.add(new ByteArray(sortedIdxTerms.get(i).getBytes("UTF-8")));
        qTermsStr.add(sortedIdxTerms.get(i));
      }

      // compare calculations for all terms in index
      final Iterator<ByteArray> termsIt = FIXED_INDEX.getTermsIterator();
      while (termsIt.hasNext()) {
        final ByteArray term = termsIt.next();
        final String termStr = ByteArrayUtils.utf8ToString(term);

        final double result = instance.getQueryModel(term, qTerms, fbDocIds);

        // calculate expected result
        final double expected =
            calculateQueryModel(termStr, qTermsStr, fbDocIds);

        Assert.assertEquals("Query model value differs.", expected, result,
            DELTA_Q_MOD);
      }
    }
  }

  /**
   * Test of {@link ImprovedClarityScore.FbTermReducerTarget}.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testTermReducerTarget()
      throws Exception {
    final Metrics metrics = new Metrics(FIXED_INDEX);
    LinkedList<ByteArray> reducedFbTerms;
    ImprovedClarityScore.FbTermReducerTarget trTarget;
    Iterator<ByteArray> idxTermsIt;

    // all terms should be included (minDocFreq == 0)
    reducedFbTerms = new LinkedList<>();
    trTarget = new ImprovedClarityScore.FbTermReducerTarget(
        metrics.collection(), 0, reducedFbTerms);
    idxTermsIt = FIXED_INDEX.getTermsIterator();
    while (idxTermsIt.hasNext()) {
      final ByteArray term = idxTermsIt.next();
      trTarget.call(term);
    }

    idxTermsIt = FIXED_INDEX.getTermsIterator();
    while (idxTermsIt.hasNext()) {
      Assert.assertTrue("Term should be in reduced set with threshold == 0.",
          reducedFbTerms.contains(idxTermsIt.next()));
    }

    // test for each known document frequency value
    final Iterable<Integer> docFreqs =
        new HashSet<>(FixedTestIndexDataProvider.KnownData
            .IDX_DOCFREQ.values());
    for (final Integer filterDocFreq : docFreqs) {
      reducedFbTerms = new LinkedList<>();
      trTarget = new ImprovedClarityScore.FbTermReducerTarget(
          metrics.collection(), filterDocFreq, reducedFbTerms);

      idxTermsIt = FIXED_INDEX.getTermsIterator();
      while (idxTermsIt.hasNext()) {
        final ByteArray term = idxTermsIt.next();
        trTarget.call(term);
      }

      for (final Map.Entry<String, Integer> docFreqEntry :
          FixedTestIndexDataProvider
              .KnownData.IDX_DOCFREQ.entrySet()) {
        final int freq = docFreqEntry.getValue();
        @SuppressWarnings("ObjectAllocationInLoop")
        final ByteArray termBytes = new ByteArray(docFreqEntry.getKey()
            .getBytes("UTF-8"));
        if (freq >= filterDocFreq) {
          Assert.assertTrue(
              "DocFreq (" + freq + ", " + metrics.collection().df(termBytes) +
                  ") " +
                  "over or equal to threshold " +
                  "(" + filterDocFreq + "). Term (" + docFreqEntry.getKey() +
                  ") should  be in reduced set.",
              reducedFbTerms.contains(termBytes)
          );
        } else {
          Assert.assertFalse("DocFreq (" + freq + ") below threshold " +
                  "(" + filterDocFreq + "). Term (" + docFreqEntry.getKey() +
                  ") should not be in reduced set.",
              reducedFbTerms.contains(termBytes)
          );
        }
      }
    }
  }

  /**
   * Test of {@link ImprovedClarityScore#calculateClarity(String)}. Run for a
   * single term.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity_singleTerm()
      throws Exception {
    try (ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(ICC_CONF).build()) {
      final String term = FixedTestIndexDataProvider.KnownData
          .TF_DOC_0.entrySet().iterator().next().getKey();
      System.out.println("Term=" + term);

      final ImprovedClarityScore.Result result =
          instance.calculateClarity(term);
      final double expected = calculateScore(result);

      Assert.assertEquals("Single term score value differs.",
          expected, result.getScore(), DELTA_SCORE_SINGLE);
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
  private static double calculateScore(
      final ImprovedClarityScore.Result result)
      throws Exception {
    double score = 0d;
    final Collection<String> qTermsStr =
        new ArrayList<>(result.getQueryTerms().size());
    for (final ByteArray qTerm : result.getQueryTerms()) {
      qTermsStr.add(ByteArrayUtils.utf8ToString(qTerm));
    }
    for (final ByteArray term : result.getFeedbackTerms()) {
      final String termStr = ByteArrayUtils.utf8ToString(term);
      final double pq = calculateQueryModel(termStr, qTermsStr,
          result.getFeedbackDocuments());
      score += pq * MathUtils.log2(pq / FIXED_INDEX.getRelativeTermFrequency
          (term));
    }
    return score;
  }

  /**
   * Test of {@link ImprovedClarityScore#calculateClarity(String)}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testCalculateClarity()
      throws Exception {

    try (ImprovedClarityScore instance = getInstanceBuilder()
        .configuration(ICC_CONF).build()) {
      // some random terms from the index will make up a query
      final Tuple.Tuple2<List<String>, List<ByteArray>> randQTerms =
          FixedTestIndexDataProvider.getRandomIndexTerms();
      final List<String> qTermsStr = randQTerms.a;

      // create a query string from the list of terms
      final String queryStr = StringUtils.join(qTermsStr, " ");

      final ImprovedClarityScore.Result result = instance.calculateClarity
          (queryStr);
      final double score = calculateScore(result);

      Assert.assertEquals("Clarity score mismatch.", score, result.getScore(),
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
              (termInDocFreq + (ICC.smoothing * termRelIdxFreq)) /
                  ((double) docTermFreq + (ICC.smoothing * (double)
                      termsInDoc));
          // final model
          final double model =
              (ICC.lambda *
                  ((ICC.beta * smoothedTerm) +
                      ((1d - ICC.beta) * termRelIdxFreq))
              ) + ((1d - ICC.lambda) * termRelIdxFreq);

          D_MODEL.put(Fun.t2(docId, term), model);
        }
      }
    }
  }
}
