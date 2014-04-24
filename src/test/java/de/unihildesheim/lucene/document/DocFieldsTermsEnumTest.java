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
import de.unihildesheim.lucene.util.BytesRefUtil;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DocFieldsTermsEnum}.
 *
 *
 */
@RunWith(Parameterized.class)
public final class DocFieldsTermsEnumTest
        extends MultiIndexDataProviderTestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DocFieldsTermsEnumTest.class);

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
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public void setUp() throws Exception {
    caseSetUp();
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
  public DocFieldsTermsEnumTest(
          final Class<? extends IndexDataProvider> dataProv,
          final MultiIndexDataProviderTestCase.RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Test of setDocument method, of class DocFieldsTermsEnum.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetDocument() throws Exception {
    LOG.info("Test setDocument");

    DocFieldsTermsEnum instance = new DocFieldsTermsEnum();
    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      instance.setDocument(i);
    }
  }

  /**
   * Test of reset method, of class DocFieldsTermsEnum.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testReset() throws Exception {
    LOG.info("Test reset");
    DocFieldsTermsEnum instance = new DocFieldsTermsEnum();
    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      instance.setDocument(i);
      int count = 0;
      int oldCount = 0;
      BytesRef br = instance.next();
      while (br != null) {
        oldCount++;
        br = instance.next();
      }
      instance.reset();
      br = instance.next();
      while (br != null) {
        count++;
        br = instance.next();
      }
      assertEquals(getDataProviderName()
              + ": Resetted iteration yields different count.", oldCount,
              count);
    }
  }

  /**
   * Test of next method, of class DocFieldsTermsEnum.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testNext() throws Exception {
    LOG.info("Test next");
    final DocFieldsTermsEnum instance = new DocFieldsTermsEnum();

    final Collection<ByteArray> stopwords = IndexTestUtil.getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      final Map<ByteArray, Long> tfMap
              = DocumentMetrics.getModel(i).termFreqMap;
      final Map<ByteArray, Long> dftMap = new HashMap<>(tfMap.size());

      instance.setDocument(i);
      BytesRef br = instance.next();
      while (br != null) {
        final ByteArray bytes = BytesRefUtil.toByteArray(br);
        if (excludeStopwords && stopwords.contains(bytes)) {
          br = instance.next();
          continue;
        }
        if (dftMap.containsKey(bytes)) {
          dftMap.put(bytes.clone(), dftMap.get(bytes) + instance.
                  getTotalTermFreq());
        } else {
          dftMap.put(bytes.clone(), instance.getTotalTermFreq());
        }
        br = instance.next();
      }

      assertEquals(getDataProviderName()
              + ": Term map sizes differs (stopped: " + excludeStopwords
              + ").", tfMap.size(), dftMap.size());
      assertTrue(getDataProviderName()
              + ": Not all terms are present (stopped: " + excludeStopwords
              + ").", dftMap.keySet().containsAll(tfMap.keySet()));

      for (Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
        assertEquals(getDataProviderName()
                + ": Term frequency values differs (stopped: "
                + excludeStopwords + "). docId=" + i + " term="
                + tfEntry.toString(), tfEntry.getValue(), dftMap.get(tfEntry.
                        getKey()));
      }
    }
  }

  /**
   * Test of getTotalTermFreq method, of class DocFieldsTermsEnum.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTotalTermFreq() throws Exception {
    LOG.info("Test getTotalTermFreq");
    final DocFieldsTermsEnum instance = new DocFieldsTermsEnum();

    final Collection<ByteArray> stopwords = IndexTestUtil.getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    for (int i = 0; i < index.getDocumentCount(); i++) {
      final Map<ByteArray, Long> tfMap = index.getDocumentTermFrequencyMap(i);
      final Map<ByteArray, Long> dftMap = new HashMap<>(tfMap.size());

      instance.setDocument(i);
      BytesRef br = instance.next();
      while (br != null) {
        final ByteArray bytes = BytesRefUtil.toByteArray(br);
        if (excludeStopwords && stopwords.contains(bytes)) {
          br = instance.next();
          continue;
        }
        if (dftMap.containsKey(bytes)) {
          dftMap.put(bytes, dftMap.get(bytes) + instance.getTotalTermFreq());
        } else {
          dftMap.put(bytes, instance.getTotalTermFreq());
        }
        br = instance.next();
      }

      assertEquals(getDataProviderName()
              + ": Term map sizes differ (stopped: " + excludeStopwords + ").",
              tfMap.size(), dftMap.size());
      assertTrue(getDataProviderName()
              + ": Not all terms are present (stopped: " + excludeStopwords
              + ").", dftMap.keySet().containsAll(tfMap.keySet()));

      for (Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
        assertEquals(getDataProviderName()
                + ": Term frequency values differ (stopped: "
                + excludeStopwords + "). docId=" + i + " term="
                + tfEntry.toString(), tfEntry.getValue(), dftMap.get(
                        tfEntry.getKey()));
      }
    }
  }
}
