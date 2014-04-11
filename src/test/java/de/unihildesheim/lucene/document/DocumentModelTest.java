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
package de.unihildesheim.lucene.document;

import de.unihildesheim.ByteArray;
import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.IndexTestUtil;
import de.unihildesheim.lucene.index.TestIndexDataProvider;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.metrics.DocumentMetrics;
import de.unihildesheim.util.RandomValue;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DocumentModel}.
 *
 *
 */
@RunWith(Parameterized.class)
public final class DocumentModelTest extends MultiIndexDataProviderTestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DocumentModelTest.class);

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    index = new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
            isInitialized());
  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static void tearDownClass() {
    // close the test index
    index.dispose();
  }

  /**
   * Get parameters for parameterized test.
   *
   * @return Test parameters
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return getCaseParameters();
  }

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public DocumentModelTest(final Class<? extends IndexDataProvider> dataProv,
          final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public void setUp() throws Exception {
    caseSetUp();
  }

  /**
   * Test method for contains method, of class DocumentModel.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private void _testContains() throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      final DocumentModel docModel = DocumentMetrics.getModel(i);
      final DocumentModel docModelExp = index.getDocumentModel(i);
      for (ByteArray bw : docModel.termFreqMap.keySet()) {
        assertEquals(getDataProviderName()
                + ": Document contains term mismatch (stopped: "
                + excludeStopwords + ").", docModelExp.contains(bw), docModel.
                contains(bw));
        if (excludeStopwords) {
          assertFalse(getDataProviderName()
                  + ": Document contains stopword (stopped: "
                  + excludeStopwords + ").", stopwords.contains(bw));
        }
      }
    }
  }

  /**
   * Test of contains method, of class DocumentModel.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testContains() throws Exception {
    LOG.info("Test contains");
    _testContains();
  }

  /**
   * Test of contains method, of class DocumentModel.
   * <p>
   * Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testContains_randFields() throws Exception {
    LOG.info("Test contains (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testContains();
  }

  /**
   * Test of contains method, of class DocumentModel.
   * <p>
   * Using stopwords.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testContains_stopped() throws Exception {
    LOG.info("Test contains (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    _testContains();
  }

  /**
   * Test method for termFreqMap field, of class DocumentModel.
   *
   * @throws Exception Any exception indicates an error
   */
  @SuppressWarnings("null")
  private void _testTermFreqMap() throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      final Map<ByteArray, Long> tfMap
              = DocumentMetrics.getModel(i).termFreqMap;
      final Map<ByteArray, Long> tfMapExp = index.getDocumentTermFrequencyMap(
              i);

      assertEquals(getDataProviderName()
              + ": Term count mismatch between index and model.", tfMapExp.
              size(), tfMap.size());
      assertTrue(getDataProviderName()
              + ": Term mismatch between index and model.", tfMapExp.keySet().
              containsAll(tfMap.keySet()));

      for (Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
        assertEquals(getDataProviderName()
                + ": Document term frequency mismatch "
                + "between index and model.", tfMapExp.get(tfEntry.getKey()),
                tfEntry.getValue());
        if (excludeStopwords) {
          assertFalse("Stopword found in model.", stopwords.contains(tfEntry.
                  getKey()));
        }
      }
    }
  }

  /**
   * Test of termFreqMap field, of class DocumentModel.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testTermFreqMap() throws Exception {
    LOG.info("Test termFreqMap");
    _testTermFreqMap();
  }

  /**
   * Test of termFreqMap field, of class DocumentModel.
   * <p>
   * Using random fields.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testTermFreqMap_randFields() throws Exception {
    LOG.info("Test termFreqMap (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testTermFreqMap();
  }

  /**
   * Test of termFreqMap field, of class DocumentModel.
   * <p>
   * Using stopwords.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testTermFreqMap_stopped() throws Exception {
    LOG.info("Test termFreqMap (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    _testTermFreqMap();
  }

  /**
   * Test method for equals method, of class DocumentModel.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings({"DM_DEFAULT_ENCODING"})
  public void _testEquals() {
    final int firstDocId = RandomValue.getInteger(0, CollectionMetrics.
            numberOfDocuments().intValue() - 1);
    int secondDocId = RandomValue.getInteger(0, CollectionMetrics.
            numberOfDocuments().intValue() - 1);
    while (secondDocId == firstDocId) {
      secondDocId = RandomValue.getInteger(0, CollectionMetrics.
              numberOfDocuments().intValue() - 1);
    }

    final DocumentModel firstDocModel = DocumentMetrics.getModel(firstDocId);
    final DocumentModel secondDocModel = DocumentMetrics.getModel(secondDocId);

    assertFalse(getDataProviderName() + ": DocModels should not be the same.",
            firstDocModel.equals(secondDocModel));

    final DocumentModel.DocumentModelBuilder derivedDocModel
            = new DocumentModel.DocumentModelBuilder(firstDocModel);
    // add a new term with it's frequency value, to make this model different
    derivedDocModel.setTermFrequency(
            new ByteArray("foo#Bar#Value".getBytes()), 10);
    assertFalse(getDataProviderName()
            + ": Derived DocumentModel should not be the same "
            + "as the original one.", firstDocModel.equals(
                    derivedDocModel.getModel()));
  }

  /**
   * Test of equals method, of class DocumentModel.
   */
  @Test
  public void testEquals() {
    LOG.info("Test equals");
    _testEquals();
  }

  /**
   * Test of equals method, of class DocumentModel.
   * <p>
   * Using random fields.
   */
  @Test
  public void testEquals_randFields() {
    LOG.info("Test equals (random fields)");
    IndexTestUtil.setRandomFields(index);
    _testEquals();
  }

  /**
   * Test of equals method, of class DocumentModel.
   * <p>
   * Using random fields.
   */
  @Test
  public void testEquals_stopped() {
    LOG.info("Test equals (stopped)");
    IndexTestUtil.setRandomStopWords(MultiIndexDataProviderTestCase.index);
    _testEquals();
  }

  /**
   * Test of hashCode method, of class DocumentModel.
   */
  @Test
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_DEFAULT_ENCODING")
  public void testHashCode() {
    LOG.info("Test hashCode");
    final int firstDocId = RandomValue.getInteger(0, CollectionMetrics.
            numberOfDocuments().intValue());
    int secondDocId = RandomValue.getInteger(0, CollectionMetrics.
            numberOfDocuments().intValue());
    while (secondDocId == firstDocId) {
      secondDocId = RandomValue.getInteger(0, CollectionMetrics.
              numberOfDocuments().intValue());
    }

    final DocumentModel firstDocModel = DocumentMetrics.getModel(firstDocId);
    final DocumentModel secondDocModel = DocumentMetrics.getModel(secondDocId);

    // test two different models
    assertNotEquals(getDataProviderName()
            + ": DocModels hashCode should not be the same. ("
            + firstDocModel.id + ", " + secondDocModel.id + ")",
            firstDocModel.hashCode(), secondDocModel.hashCode());

    // get the same model again an test
    assertEquals(getDataProviderName()
            + ": DocModels hashCode should be the same "
            + "for the same document.", firstDocModel.hashCode(),
            DocumentMetrics.getModel(firstDocId).hashCode());

    // change a model
    final DocumentModel.DocumentModelBuilder derivedDocModel
            = new DocumentModel.DocumentModelBuilder(firstDocModel);
    // add a new term with it's frequency value, to make this model different
    derivedDocModel.
            setTermFrequency(new ByteArray("foo#Bar#Value".getBytes()), 10);

    assertNotEquals(
            getDataProviderName()
            + ": HashCode of derived DocumentModel should "
            + "not be the same as the original one.", firstDocModel.hashCode(),
            derivedDocModel.getModel().hashCode());
  }

  /**
   * Test of getSmoothedTermFrequency method, of class DocumentModel.
   */
  @Test
  public void testGetSmoothedRelativeTermFrequency() {
    LOG.info("Test getSmoothedRelativeTermFrequency");
    final int smoothingAmount = 100;
    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      final DocumentModel docModel = DocumentMetrics.getModel(i);
      final DocumentMetrics dm = docModel.metrics();
      for (ByteArray bw : docModel.termFreqMap.keySet()) {
        assertNotEquals(getDataProviderName()
                + ": Smoothed and absolute relative term frequency "
                + "should not be the same.", dm.relTf(bw), dm.
                smoothedRelativeTermFrequency(bw, smoothingAmount));
      }
    }
  }

  /**
   * Test method for tf method, of class DocumentModel.
   *
   * @throws Exception Any exception indicates an error
   */
  @SuppressWarnings("null")
  private void _testTf() throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      final DocumentModel docModel = DocumentMetrics.getModel(i);
      final Map<ByteArray, Long> tfMap = index.getDocumentTermFrequencyMap(i);
      for (Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
        assertEquals("Term frequency mismatch.", docModel.tf(tfEntry.getKey()),
                tfEntry.getValue());
        if (excludeStopwords) {
          assertFalse("Stopword found in model.", stopwords.contains(tfEntry.
                  getKey()));
        }
      }
    }
  }

  /**
   * Test of tf method, of class DocumentModel.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testTf() throws Exception {
    LOG.info("tf");
    _testTf();
  }

  /**
   * Test of tf method, of class DocumentModel.
   *
   * @throws java.lang.Exception Any exception indicates an error
   */
  @Test
  public void testTf_stopped() throws Exception {
    LOG.info("tf (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    _testTf();
  }

  /**
   * Test method for termCount method, of class DocumentModel.
   *
   * @throws java.lang.Exception Any exception indicates an error
   */
  @SuppressWarnings("null")
  private void _testTermCount() throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordsFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      final DocumentModel docModel = DocumentMetrics.getModel(i);
      final Map<ByteArray, Long> tfMap = index.getDocumentTermFrequencyMap(i);
      assertEquals("Unique term count mismatch.", docModel.termCount(),
              tfMap.size());
      if (excludeStopwords) {
        for (ByteArray term : tfMap.keySet()) {
          assertFalse("Stopword found in model.", stopwords.contains(term));
        }
      }
    }
  }

  /**
   * Test of termCount method, of class DocumentModel.
   *
   * @throws java.lang.Exception Any exception indicates an error
   */
  @Test
  public void testTermCount() throws Exception {
    LOG.info("Test termCount");
    _testTermCount();
  }

  /**
   * Test of termCount method, of class DocumentModel.
   * <p>
   * Using stopwords.
   *
   * @throws java.lang.Exception Any exception indicates an error
   */
  @Test
  public void testTermCount_stopped() throws Exception {
    LOG.info("Test termCount (stopped)");
    IndexTestUtil.setRandomStopWords(index);
    _testTermCount();
  }

  /**
   * Test of metrics method, of class DocumentModel.
   */
  @Test
  public void testMetrics() {
    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      final DocumentModel docModel = DocumentMetrics.getModel(i);
      assertNotNull("Metrics not found.", docModel.metrics());
    }
  }

}
