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

import de.unihildesheim.TestConfig;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.RandomValue;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DocumentModel}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
@RunWith(Parameterized.class)
public final class DocumentModelTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DocumentModelTest.class);
  /**
   * Test documents index.
   */
  private static TestIndex index;
  /**
   * DataProvider instance currently in use.
   */
  private final Class<? extends IndexDataProvider> dataProvType;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    index = new TestIndex(TestIndex.IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndex.test_isInitialized());
  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static void tearDownClass() {
    // close the test index
    index.dispose();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = TestConfig.getDataProviderParameter();
    params.add(new Object[]{null});
    return params;
  }

  public DocumentModelTest(final Class<? extends IndexDataProvider> dataProv) {
    this.dataProvType = dataProv;
  }

  /**
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public void setUp() throws Exception {
    Environment.clear();
    if (this.dataProvType == null) {
      index.setupEnvironment();
    } else {
      index.setupEnvironment(this.dataProvType);
    }
  }

  /**
   * Test of contains method, of class DocumentModel.
   */
  @Test
  public void testContains() {
    LOG.info("Test contains");

    for (int i = 0; i < Environment.getDataProvider().getDocumentCount(); i++) {
      final DocumentModel docModel = Environment.getDataProvider().
              getDocumentModel(i);
      final DocumentModel docModelExp = index.getDocumentModel(i);
      for (BytesWrap bw : docModel.termFreqMap.keySet()) {
        assertEquals("Document contains term mismatch.", docModelExp.contains(
                bw), docModel.contains(bw));
      }
    }
  }

  /**
   * Test of termFrequency method, of class DocumentModel.
   */
  @Test
  @SuppressWarnings("BX_UNBOXING_IMMEDIATELY_REBOXED")
  public void testTermFrequency() {
    LOG.info("Test termFrequency");

    for (int i = 0; i < Environment.getDataProvider().getDocumentCount(); i++) {
      final Map<BytesWrap, Long> tfMap = Environment.getDataProvider().
              getDocumentModel(i).termFreqMap;
      final Map<BytesWrap, Long> tfMapExp = index.getDocumentTermFrequencyMap(
              i);

      assertEquals("Term count mismatch between index and model.", tfMapExp.
              size(), tfMap.size());
      assertTrue("Term mismatch between index and model.", tfMapExp.keySet().
              containsAll(tfMap.keySet()));

      for (Entry<BytesWrap, Long> tfEntry : tfMap.entrySet()) {
        assertEquals("Document term frequency mismatch "
                + "between index and model.", tfMapExp.get(tfEntry.getKey()),
                tfEntry.getValue());
      }
    }
  }

  /**
   * Test of equals method, of class DocumentModel.
   */
  @Test
  public void testEquals() {
    LOG.info("Test equals");

    final int firstDocId = RandomValue.getInteger(0, (int) Environment.
            getDataProvider().getDocumentCount());
    int secondDocId = RandomValue.getInteger(0, (int) Environment.
            getDataProvider().getDocumentCount());
    while (secondDocId == firstDocId) {
      secondDocId = RandomValue.getInteger(0, (int) Environment.
              getDataProvider().getDocumentCount());
    }

    final DocumentModel firstDocModel = Environment.getDataProvider().
            getDocumentModel(firstDocId);
    final DocumentModel secondDocModel = Environment.getDataProvider().
            getDocumentModel(secondDocId);

    assertFalse("DocModels should not be the same.", firstDocModel.equals(
            secondDocModel));

    final DocumentModel.DocumentModelBuilder derivedDocModel
            = new DocumentModel.DocumentModelBuilder(firstDocModel);
    // add a new term with it's frequency value, to make this model different
    derivedDocModel.setTermFrequency(
            new BytesWrap("foo#Bar#Value".getBytes()), 10);
    assertFalse("Derived DocumentModel should not be the same "
            + "as the original one.", firstDocModel.equals(
                    derivedDocModel.getModel()));
  }

  /**
   * Test of hashCode method, of class DocumentModel.
   */
  @Test
  public void testHashCode() {
    LOG.info("Test hashCode");
    final int firstDocId = RandomValue.getInteger(0, (int) Environment.
            getDataProvider().
            getDocumentCount());
    int secondDocId = RandomValue.getInteger(0, (int) Environment.
            getDataProvider().
            getDocumentCount());
    while (secondDocId == firstDocId) {
      secondDocId = RandomValue.getInteger(0, (int) Environment.
              getDataProvider().getDocumentCount());
    }

    final DocumentModel firstDocModel = Environment.getDataProvider().
            getDocumentModel(firstDocId);
    final DocumentModel secondDocModel = Environment.getDataProvider().
            getDocumentModel(secondDocId);

    // test two different models
    assertNotEquals("DocModels hashCode should not be the same. ("
            + firstDocModel.id + ", " + secondDocModel.id + ")",
            firstDocModel.hashCode(), secondDocModel.hashCode());

    // get the same model again an test
    assertEquals("DocModels hashCode should be the same "
            + "for the same document.", firstDocModel.hashCode(),
            Environment.getDataProvider().getDocumentModel(firstDocId).
            hashCode());

    // change a model
    final DocumentModel.DocumentModelBuilder derivedDocModel
            = new DocumentModel.DocumentModelBuilder(firstDocModel);
    // add a new term with it's frequency value, to make this model different
    derivedDocModel.setTermFrequency(
            new BytesWrap("foo#Bar#Value".getBytes()), 10);

    assertNotEquals(
            "HashCode of derived DocumentModel should "
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
    for (int i = 0; i < Environment.getDataProvider().getDocumentCount(); i++) {
      final DocumentModel docModel = Environment.getDataProvider().
              getDocumentModel(i);
      for (BytesWrap bw : docModel.termFreqMap.keySet()) {
        assertNotEquals("Smoothed and absolute relative term frequency "
                + "should not be the same.", docModel.
                getRelativeTermFrequency(bw), docModel.
                getSmoothedRelativeTermFrequency(Environment.getDataProvider(),
                        bw, smoothingAmount));
      }
    }
  }

}
