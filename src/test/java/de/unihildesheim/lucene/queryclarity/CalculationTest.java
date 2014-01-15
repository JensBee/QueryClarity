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
package de.unihildesheim.lucene.queryclarity;

import de.unihildesheim.lucene.queryclarity.documentmodel.DocumentModel;
import de.unihildesheim.lucene.queryclarity.indexdata.IndexDataProvider;
import de.unihildesheim.lucene.queryclarity.indexdata.DefaultIndexDataProvider;
import de.unihildesheim.lucene.queryclarity.indexdata.DocFieldsTermsEnum;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static org.junit.Assert.fail;
import org.apache.lucene.search.Query;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class CalculationTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          CalculationTest.class);

  /**
   * Test configration for this test.
   */
  private static final TestConfiguration CONF = new TestConfiguration();
  /**
   * Test utility class instance used by this test.
   */
  private static final TestUtility UTIL = new TestUtility(CONF);

  /**
   * Shared instance of calculation class.
   */
  private static Calculation calcInstance = null;

  private static IndexDataProvider dataProv;

  /**
   * Random generated queries run throughout a test single test run.
   */
  private static Set<Set<String>> randQueries;

  private static DocFieldsTermsEnum docFieldsTermsEnum;

  /**
   * Random generated queries wich should create errors because it uses terms
   * not from collection.
   */
  private static Set<Set<String>> randErrorQueries = new HashSet(5);

  private static Collection<DocumentModel> docMap;

  /**
   * Get a collection of document models for all documents in index matching one
   * of the given terms or all documents in index, if no terms are given.
   *
   * @param terms Restrict the collection of documents to only those which match
   * one of the given terms. If this is empty, all documents in index will be
   * used.
   * @return Collection of document models for all documents in index matching
   * one of the given terms
   * @throws java.io.IOException Thrown, if index could not be accessed
   */
  private void updateDocMap(final Collection<String> terms) throws IOException {
    if (terms == null || terms.isEmpty()) {
      // no terms where specified - return all documents from index
      LOG.debug("(getDocModels) No terms where specified "
              + "- return all documents from index");
      this.docMap = TestIndex.getDocumentModels();
    } else {
      this.docMap = TestIndex.getDocumentModelsMatching(terms);
    }
  }

  /**
   * Calculate the probability values for documents specified by their model.
   * The calculation can be restricted to some terms only, or all terms in
   * index.
   *
   * @param docModels Document models for which the calculation should be done
   * @param terms List of terms for which the calculation should be done. If
   * this is null or empty, all terms in index will be used.
   */
  private void calculateDocumentProbabilities(Collection<String> terms)
          throws IOException {

    // default to all terms, if none are given
    if (terms == null || terms.isEmpty()) {
      terms = CONF.getIndex().getTerms();
      LOG.trace("(calculateDocumentProbabilities) No terms where specified "
              + "- using all terms from index");
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Calculating document probability for {} terms", terms.size());
      LOG.trace("terms={}", terms);
    }

    // iterate over all terms in index
    for (String term : terms) {
      // iterate over all documents
      for (DocumentModel docModel : docMap) {
        // calculate
        docModel.termProbability(term);
      }
    }
  }

  @BeforeClass
  public static final void setUp() throws Exception {
    final String[] fields = new String[]{"text"};
    final ArrayList<String[]> documents = new ArrayList<String[]>(4);
    documents.add(new String[]{"Lucene in Action"});
    documents.add(new String[]{"Lucene for Dummies"});
    documents.add(new String[]{"Managing Gigabytes"});
    documents.add(new String[]{"The Art of Computer Science"});

    // create needed instances
    CONF.setIndex(new TestIndex(fields, documents, fields));
    calcInstance = new Calculation(CONF.getIndex(), CONF.getIndex().getReader());
    dataProv = new DefaultIndexDataProvider(CONF.getIndex().getReader(), fields);

    docFieldsTermsEnum = new DocFieldsTermsEnum(CONF.getIndex().getReader(),
            fields);

    docMap = new HashSet(documents.size());

    // generate random queries
    CalculationTest.randQueries = UTIL.generateRandomQueries(1);

    CalculationTest.randErrorQueries = UTIL.generateRandomTermsQueries(1);

    TestUtility.dumpQueries(randQueries);
    TestUtility.dumpQueries(randErrorQueries);
  }

  @AfterClass
  public static final void tearDown() throws Exception {
    calcInstance.dispose();
  }

  /**
   * Test overall collection term frequency counting.
   *
   * @throws java.io.IOException Thrown, if index could not be accessed
   */
  @Test
  public final void collectionTermFreq() throws IOException {
    TestUtility.logHeader(LOG, "Collection term frequency");

    final long result = dataProv.getTermFrequency();
    final long expResult = CONF.getIndex().getTermFrequency();

    LOG.info("[F] exp-F={} F={}", expResult, result);

    assertEquals("should be same value", expResult, result);
  }

  /**
   * Test individual collection term frequency counting.
   *
   * @throws java.io.IOException Thrown, if index could not be accessed
   */
  @Test
  public final void getTermCollectionFrequency() throws IOException {
    TestUtility.logHeader(LOG, "Term collection frequency");

    double result;
    double expResult;
    for (String term : CONF.getIndex().getTerms()) {
      result = dataProv.getTermFrequency(term);
      expResult = CONF.getIndex().getTermFrequency(term);
      assertEquals("should be same value (t=" + term + ")",
              expResult, result, 0);
    }
  }

  /**
   * Test of calculateCollectionProbability method, of class Calculation.
   *
   * @throws java.io.IOException Thrown, if index could not be accessed
   */
  @Test
  public final void calculateCollectionProbability() throws IOException {
    TestUtility.logHeader(LOG, "Term collection probability");

    double result;
    double expResult;
    for (String term : CONF.getIndex().getTerms()) {
      result = dataProv.getRelativeTermFrequency(term);
      expResult = CONF.getIndex().getRelativeTermFrequency(term);
      assertEquals("should be same value", expResult, result, 0);
    }
  }

  /**
   * Test calculation of probability of term in document.
   *
   * @throws java.io.IOException Thrown, if index could not be accessed
   */
  @Test
  public final void calculateDocumentProbability() throws IOException {
    TestUtility.logHeader(LOG, "Term document probability");

    this.updateDocMap(null);
    Double result;
    Double expResult;

    // do needed calculations
    this.calculateDocumentProbabilities(null);

    // iterate over all terms in index
    for (String term : CONF.getIndex().getTerms()) {
      // iterate over all documents
      for (DocumentModel docModel : docMap) {
        // calculation already done - check result
        result = docModel.termProbability(term);
        expResult = TestIndex.getDocumentProbability(term, docModel.id());
        LOG.info("[pdt] docId={} term={} exp-pdt={} pdt={}", docModel.id(),
                term, expResult, result);
        assertEquals("should be same value", expResult, result);
      }
    }
  }

  /**
   * Test of calculateQueryProbability method, of class Calculation.
   *
   * @throws java.io.IOException Thrown, if index could not be accessed
   */
  @Test
  public final void calculateQueryProbability() throws IOException {
    TestUtility.logHeader(LOG, "Query probability");

    double result;
    double expResult;
    // allowed difference between result and expected result
    double delta = 0.0000000000000001;

    // first pass - use all documents in index
    // get document models for all documents in index
    this.updateDocMap(null);

    for (Set<String> query : CalculationTest.randQueries) {
      LOG.info("Calculating pqt probability for all ({}) document models. "
              + "Query '{}'", docMap.size(), query);

      // do needed calculations
      this.calculateDocumentProbabilities(query);

      // calculate pq for each term
      for (String qTerm : query) {
        result = calcInstance.calculateQueryProbability(CalculationTest.docMap,
                query, qTerm);
        expResult = TestIndex.calcPQT(query, qTerm, this.docMap);

        LOG.info("[pqt] query={} term={} docs=all exp-pqt={} pqt={}", query,
                qTerm, expResult, result);

        assertEquals("should be same value (delta: " + delta + ")", expResult,
                result, delta);
      }
    }

    // second pass - use only matching documents from index
    Collection<DocumentModel> docModelsMatch;
    // collect document ids for debug output
    final List<Integer> docModelsMatchIds = new ArrayList();

    for (Set<String> query : CalculationTest.randQueries) {
      // get all documents from index matching at least one query term
      this.updateDocMap(query);

      // get document ids from all matching documents (for debug output)
      if (LOG.isDebugEnabled()) {
        docModelsMatchIds.clear();
        for (DocumentModel docModel : docMap) {
          docModelsMatchIds.add(docModel.id());
        }
      }

      LOG.info("Calculating pqt probability for matching ({}) document models "
              + "matching query '{}'", docMap.size(), query);

      // do needed calculations
      this.calculateDocumentProbabilities(query);

      // calculate pq for each term
      for (String qTerm : query) {
        result = calcInstance.calculateQueryProbability(docMap, query, qTerm);
        expResult = CONF.getIndex().calcPQT(query, qTerm, docMap);

        LOG.info("[pqt] query={} term={} docs={} exp-pqt={} pqt={}", query,
                qTerm, docModelsMatchIds, expResult, result);

        assertEquals("should be same value (delta: " + delta + ")", expResult,
                result, delta);
      }
    }
  }

  /**
   * Test the calculation of the clarity score.
   *
   * @throws java.io.IOException Thrown, if index could not be accessed
   */
  @Test
  public final void calculateClarityScore() throws IOException {
    TestUtility.logHeader(LOG, "Clarity score");

    // ### first pass - use all documents in index
    this.updateDocMap(null);

    // run calculation for terms in query - this may produce errors, if a term
    // from the query is not present in the collection
    for (Set<String> query : CalculationTest.randQueries) {
      if (!CONF.getIndex().getTerms().containsAll(query)) {
        exception.expect(IllegalArgumentException.class);
      }
      runCalculateClarityScore("all", "query", query, query);
    }

    // run calculation for all terms in index
    for (Set<String> query : CalculationTest.randQueries) {
      runCalculateClarityScore("all", "idx", query, CONF.getIndex().getTerms());
    }

    for (Set<String> query : CalculationTest.randErrorQueries) {
      exception.expect(IllegalArgumentException.class);
      runCalculateClarityScore("all", "error-query", query, CONF.getIndex().
              getTerms());
    }

    // ### second pass - use all documents in index
    Collection<DocumentModel> docModelsMatch;
    // collect document ids for debug output
    final List<Integer> docModelsMatchIds = new ArrayList();

    // run calculation for terms in query - this may produce errors, if a term
    // from the query is not present in the collection
    for (Set<String> query : CalculationTest.randQueries) {
      this.updateDocMap(query);
      // get document ids from all matching documents (for debug output)
      if (LOG.isDebugEnabled()) {
        docModelsMatchIds.clear();
        for (DocumentModel docModel : docMap) {
          docModelsMatchIds.add(docModel.id());
        }
      }
      if (!CONF.getIndex().getTerms().containsAll(query)) {
        exception.expect(IllegalArgumentException.class);
      }
      this.runCalculateClarityScore(docModelsMatchIds, "query", query, query);
    }

    // run calculation for all terms in index
    for (Set<String> query : CalculationTest.randQueries) {
      this.updateDocMap(query);

      // get document ids from all matching documents (for debug output)
      if (LOG.isDebugEnabled()) {
        docModelsMatchIds.clear();
        for (DocumentModel docModel : docMap) {
          docModelsMatchIds.add(docModel.id());
        }
      }
      this.runCalculateClarityScore(docModelsMatchIds, "all", query, CONF.
              getIndex().getTerms());
    }
  }

  /**
   * Run the calculation of the clarity score.
   *
   * @param docsType String or Collection to show which documents are used while
   * calculating (for debugging purpose)
   * @param termType String indicating which term source was used (usually
   * <code>all</code> - for all terms in index or <code>query</code> - for terms
   * in original query)
   * @param query Original query string
   * @param terms Terms actually used for calculation
   * @param docModels Document models to use for calculation
   * @throws java.io.IOException Thrown, if index could not be accessed
   */
  private void runCalculateClarityScore(final Object docsType,
          final String termType, final Set<String> query,
          final Set<String> terms) throws
          IOException {
    double result;
    double expResult;
    // allowed difference between result and expected result
    double delta = 0.0000000000000001;

    // do needed calculations
    this.calculateDocumentProbabilities(terms);

    result = calcInstance.calculateClarityScore(terms, query, docMap);
    expResult = CONF.getIndex().calcClarity(terms, query, docMap);

    LOG.info("[clarity] query={} docs={} terms={} score={} exp-score={}",
            query, docsType, termType, result, expResult);

    assertEquals("should be same value (delta: " + delta + ")", expResult,
            result, delta);
  }

  /**
   * Test of calculateClarity method, of class Calculation.
   */
  @Test
  @Ignore
  public void calculateClarity() throws Exception {
    LOG.info("calculateClarity");
//    Query query = null;
//    Calculation instance = null;
//    instance.calculateClarity(query);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

}
