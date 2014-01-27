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
package de.unihildesheim.lucene.scoring.clarity;

import de.unihildesheim.lucene.LuceneDefaults;
import de.unihildesheim.lucene.TestUtility;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.DocumentModelException;
import de.unihildesheim.lucene.index.AbstractIndexDataProvider;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.MemoryIndex;
import de.unihildesheim.lucene.index.TestIndexDataProvider;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.StringUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
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
   * {@link IndexDataProvider} used for this test.
   */
  private static AbstractIndexDataProvider dataProv;

  /**
   * Index used by this test.
   */
  private MemoryIndex idx;

  /**
   * Document fields send to index.
   */
  private final String[] fields;

  /**
   * Documents send to index.
   */
  private final List<String[]> documents;

  /**
   * Used to catch expected exceptions.
   */
  @Rule
  public final ExpectedException exception;

  /**
   * Setup environment.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  public DefaultClarityScoreTest() {
    this.exception = ExpectedException.none();
    // test index data
    fields = new String[]{"title", "text", "id"};
    documents = new ArrayList(4);
    documents.add(new String[]{"A Book", "Lucene in Action", "title1"});
    documents.add(new String[]{"Another book", "Lucene for Dummies", "title2"});
    documents.add(new String[]{"Just a bunch of papers", "Managing Gigabytes",
      "title3"});
    documents.add(new String[]{"Literature", "The Art of Computer Science",
      "title4"});
  }


  @Before
  public void setUp() throws IOException {
    // index should be unchanged through the test
    idx = new MemoryIndex(fields, documents);
  }

  private int fieldToIdx(final String field) {
    int fieldIdx = -1;
    // get the field index
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].equals(field)) {
        fieldIdx = i;
        break;
      }
    }
    return fieldIdx;
  }

  /**
   * Get a random field name from the list of fields passed to the index.
   *
   * @return Random field name
   */
  private String getRandomField() {
    // get a random field index
    int fieldIdx = TestUtility.getRandInt(0, fields.length - 1);
    return fields[fieldIdx];
  }

  /**
   * Get a random term of one of the documents from any field passed to the
   * index.
   *
   * @return Random term from indexed documents
   */
  private String getRandomTerm() {
    // get a random field..
    int fieldIdx = TestUtility.getRandInt(0, fields.length - 1);
    // ..and a random document..
    final int docIdx = TestUtility.getRandInt(0, documents.size() - 1);
    // ..and its field content..
    final String docContent = documents.get(docIdx)[fieldIdx];
    // ..and a random term from it
    final String[] docTerms = StringUtils.lowerCase(docContent.trim()).split(
            "\\s+");
    final int termIdx = TestUtility.getRandInt(0, docTerms.length - 1);

    return docTerms[termIdx];
  }

  /**
   * Get a random term of one of the documents from a named field passed to the
   * index.
   *
   * @return Random term from indexed documents and the named field
   */
  private String getRandomTerm(final String field) {
    int fieldIdx = fieldToIdx(field);

    if (fieldIdx == -1) {
      throw new IllegalArgumentException("Unknown field '" + field
              + "' specified");
    }

    // get random document..
    final int docIdx = TestUtility.getRandInt(0, documents.size() - 1);
    // ..and its field content..
    final String docContent = documents.get(docIdx)[fieldIdx];
    // ..and a random term from it
    final String[] docTerms = StringUtils.lowerCase(docContent.trim()).split(
            "\\s+");
    final int termIdx = TestUtility.getRandInt(0, docTerms.length - 1);

    return docTerms[termIdx];
  }

  /**
   * Get a new instance of the default clarity score calculator using the
   * test-index and test-data-provider.
   *
   * @return new instance of the default clarity score calculator
   * @throws IOException Thrown on low-level I/O errors
   * @throws DocumentModelException Thrown if document models could not be
   * created
   */
  private DefaultClarityScore getInstance() throws IOException,
          DocumentModelException {
    // data-provider gets initialized with all fields enabled
    dataProv = new TestIndexDataProvider(idx);
    return new DefaultClarityScore(idx.getReader(), dataProv);
  }

  /**
   * Calculate the document language model for a term
   *
   * @param langmodelWeight
   * @param docModel
   * @param term
   * @return
   */
  private double calcDocLangModel(final double langmodelWeight,
          final DocumentModel docModel, final BytesWrap term) {
    double weight = (double) (1 - langmodelWeight) * dataProv.
            getRelativeTermFrequency(term);

    if (docModel.containsTerm(term)) {
      return (langmodelWeight * ((double) docModel.getTermFrequency(term)
              / (double) docModel.getTermFrequency())) + weight;
    }
    return weight;
  }

  /**
   * Calculate clarity score
   *
   * @param langmodelWeight
   * @param queryTerms
   * @param feedback
   * @return
   */
  private double calculateClarity(final double langmodelWeight,
          final String[] queryTerms, final Collection<Integer> feedback) {
    // collect feedback document models
    final Set<DocumentModel> docModels = new HashSet(feedback.size());
    for (Integer docId : feedback) {
      docModels.add(dataProv.getDocumentModel(docId));
    }

    // calculate query model weights, this values are used more times
    final Map<DocumentModel, Double> weights = new HashMap(docModels.size());
    double modelWeight;
    for (DocumentModel docModel : docModels) {
      modelWeight = 1d;
      for (String term : queryTerms) {
        modelWeight *= calcDocLangModel(langmodelWeight, docModel, BytesWrap.
                duplicate(new BytesRef(term)));
      }
      weights.put(docModel, modelWeight);
    }

    // iterate over all terms in index
    Iterator<BytesWrap> idxTermsIt = this.dataProv.getTermsIterator();
    double qLangMod; // query language model
    double score = 0d;
    double log;
    while (idxTermsIt.hasNext()) {
      final BytesWrap term = idxTermsIt.next();

      // calculate the query probability of the current term
      qLangMod = 0d;
      final StringBuilder sb = new StringBuilder("QueryLangMod: ");
      for (DocumentModel docModel : docModels) {
        double langMod = calcDocLangModel(langmodelWeight, docModel, term);
        double weight = weights.get(docModel);
        if (LOG.isDebugEnabled()) {
          sb.append('(').append(docModel.getDocId()).append(':').append(term).
                  append('=').append(langMod).append(" * ").append(weight).
                  append('=').append(langMod * weight).append(") + ");
        }
        qLangMod += langMod * weight;
      }
      final String sbStr = sb.toString();
      LOG.debug(sbStr.substring(0, sbStr.length() - 2) + "= " + qLangMod);

      // calculate logarithmic part of the formular
      log = (Math.log(qLangMod) / Math.log(2)) / (Math.log(
              dataProv.getRelativeTermFrequency(term)) / Math.log(2));
      // add up final score for each term
      score += qLangMod * log;
    }

    return score;
  }

  /**
   * Test of preCalcDocumentModels method, of class DefaultClarityScore.
   *
   * @throws IOException Thrown on low-level I/O errors
   * @throws DocumentModelException Thrown if document models could not be
   * created
   */
  @Test
  public void testPreCalcDocumentModels() throws IOException,
          DocumentModelException,
          ParseException {
    TestUtility.logHeader(LOG, "preCalcDocumentModels");
    boolean force = false;
    final DefaultClarityScore instance = getInstance();
    LOG.info("force: {}", force);
    instance.preCalcDocumentModels(force);

    force = true;
    LOG.info("force: {}", force);
    instance.preCalcDocumentModels(force);
  }

  /**
   * Test of calculateClarity method, of class DefaultClarityScore.
   *
   * @throws ParseException Thrown, if query could not be parsed
   * @throws IOException Thrown on low-level I/O errors
   * @throws DocumentModelException Thrown if document models could not be
   * created
   */
  @Test
  public void testCalculateClarity() throws ParseException, IOException,
          DocumentModelException {
    TestUtility.logHeader(LOG, "calculateClarity");

    // query setup
    final String queryField = "title";//getRandomField();
    final String queryString = "another";//getRandomTerm(queryField);

    // build query
    // analyzer without stopword elimination
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
            CharArraySet.EMPTY_SET);
    final QueryParser parser = new QueryParser(LuceneDefaults.VERSION,
            queryField, analyzer);
    final Query query = parser.parse(queryString);

    // dataprovider should be initialized with all document fields available
    final DefaultClarityScore instance = getInstance();

    LOG.debug(
            "Calculate clarity for field '{}' and term '{}' query={} weight={}",
            queryField, queryString, query, instance.getLangmodelWeight());

    // call calculation with pre-set feedback documents (all), so
    // we can predict the results
    final double result = instance.calculateClarity(query, idx.getDocumentIds().
            toArray(new Integer[idx.getDocumentIds().size()])).getScore();

    double expResult = calculateClarity(instance.getLangmodelWeight(),
            new String[]{queryString}, idx.getDocumentIds());

    assertEquals(expResult, result, 0.0000000000000009);
    // TODO review the generated test code and remove the default call to fail.
  }

  /**
   * Test of getFbDocCount method, of class DefaultClarityScore.
   *
   * @throws IOException Thrown on low-level I/O errors
   * @throws DocumentModelException Thrown if document models could not be
   * created
   */
  @Test
  public void testGetFbDocCount() throws IOException, DocumentModelException {
    TestUtility.logHeader(LOG, "getFbDocCount");
    final DefaultClarityScore instance = getInstance();
    final int result = instance.getFbDocCount();
    if (result <= 0) {
      fail("Feedback document count should never be 0 or less.");
    }
  }

  /**
   * Test of setFbDocCount method, of class DefaultClarityScore.
   *
   * @throws IOException Thrown on low-level I/O errors
   * @throws DocumentModelException Thrown if document models could not be
   * created
   */
  @Test
  public void testSetFbDocCount() throws IOException, DocumentModelException {
    TestUtility.logHeader(LOG, "setFbDocCount");
    int feedbackDocCount;
    final DefaultClarityScore instance = getInstance();

    feedbackDocCount = 100;
    LOG.info("Set feedback to {} - should throw no exception.",
            feedbackDocCount);
    instance.setFbDocCount(feedbackDocCount);

    feedbackDocCount = 0;
    LOG.info("Set feedback to {} - should throw exception.", feedbackDocCount);
    exception.expect(IllegalArgumentException.class);
    instance.setFbDocCount(feedbackDocCount);
  }

  /**
   * Test of getLangmodelWeight method, of class DefaultClarityScore.
   *
   * @throws IOException Thrown on low-level I/O errors
   * @throws DocumentModelException Thrown if document models could not be
   * created
   */
  @Test
  public void testGetLangmodelWeight() throws IOException,
          DocumentModelException {
    TestUtility.logHeader(LOG, "getLangmodelWeight");
    final DefaultClarityScore instance = getInstance();
    final double result = instance.getLangmodelWeight();

    if (result < 0 || result > 1) {
      fail("Language modelweight should be in range 0-1.");
    }
  }

  /**
   * Test of setLangmodelWeight method, of class DefaultClarityScore.
   *
   * @throws IOException Thrown on low-level I/O errors
   * @throws DocumentModelException Thrown if document models could not be
   * created
   */
  @Test
  public void testSetLangmodelWeight() throws IOException,
          DocumentModelException {
    TestUtility.logHeader(LOG, "setLangmodelWeight");
    double newLangmodelWeight;
    final DefaultClarityScore instance = getInstance();

    newLangmodelWeight = 0.8;
    LOG.info("Set langmodelWeight to {} - should throw no exception.",
            newLangmodelWeight);
    instance.setLangmodelWeight(newLangmodelWeight);

    try {
      newLangmodelWeight = 0;
      LOG.info("Set langmodelWeight to {} - should throw an exception.",
              newLangmodelWeight);
      instance.setLangmodelWeight(newLangmodelWeight);
      fail("Exception not caught.");
    } catch (IllegalArgumentException ex) {
    }

    try {
      newLangmodelWeight = 1.6;
      LOG.info("Set langmodelWeight to {} - should throw an exception.",
              newLangmodelWeight);
      instance.setLangmodelWeight(newLangmodelWeight);
      fail("Exception not caught.");
    } catch (IllegalArgumentException ex) {
    }

    try {
      newLangmodelWeight = -1.2;
      LOG.info("Set langmodelWeight to {} - should throw an exception.",
              newLangmodelWeight);
      instance.setLangmodelWeight(newLangmodelWeight);
      fail("Exception not caught.");
    } catch (IllegalArgumentException ex) {
    }
  }

}
