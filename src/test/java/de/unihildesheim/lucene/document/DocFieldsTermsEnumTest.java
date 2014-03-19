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
import de.unihildesheim.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.RandomValue;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DocFieldsTermsEnum}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
@RunWith(Parameterized.class)
public class DocFieldsTermsEnumTest extends MultiIndexDataProviderTestCase {

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

  /**
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public void setUp() throws Exception {
    caseSetUp();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return getCaseParameters();
  }

  public DocFieldsTermsEnumTest(
          final Class<? extends IndexDataProvider> dataProv) {
    super(dataProv);
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
      assertEquals("Resetted iteration yields different count.", oldCount,
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
    DocFieldsTermsEnum instance;

    Map<BytesWrap, Long> dftMap;
    Map<BytesWrap, Long> tfMap;

    // test with all fields enabled
    instance = new DocFieldsTermsEnum();
    for (int i = 0; i < CollectionMetrics.numberOfDocuments(); i++) {
      tfMap = Environment.getDataProvider().getDocumentModel(i).termFreqMap;
      dftMap = new HashMap<>(tfMap.size());

      instance.setDocument(i);
      BytesRef br = instance.next();
      while (br != null) {
        final BytesWrap bw = new BytesWrap(br);
        if (dftMap.containsKey(bw)) {
          dftMap.put(bw.clone(), dftMap.get(bw) + instance.getTotalTermFreq());
        } else {
          dftMap.put(bw.clone(), instance.getTotalTermFreq());
        }
        br = instance.next();
      }

      assertEquals("Term map sizes differ.", tfMap.size(), dftMap.size());
      assertTrue("Not all terms are present.", dftMap.keySet().containsAll(
              tfMap.keySet()));

      for (Entry<BytesWrap, Long> tfEntry : tfMap.entrySet()) {
        assertEquals("Term frequency values differ. docId=" + i + " term="
                + tfEntry.toString(), tfEntry.getValue(), dftMap.get(tfEntry.
                        getKey()));
      }
    }

    // test with some fields enabled
    final int[] fieldState = index.getFieldState();
    int[] newFieldState = new int[fieldState.length];

    if (fieldState.length > 1) {
      // toggle some fields
      newFieldState = fieldState.clone();
      // ensure both states are not the same
      while (Arrays.equals(newFieldState, fieldState)) {
        for (int i = 0; i < fieldState.length; i++) {
          newFieldState[i] = RandomValue.getInteger(0, 1);
        }
      }

      index.setFieldState(newFieldState);
      final Collection<String> fields = index.test_getActiveFields();
      instance = new DocFieldsTermsEnum(Environment.getIndexReader(), fields.
              toArray(new String[fields.size()]));

      for (int i = 0; i < index.getDocumentCount(); i++) {
        tfMap = index.getDocumentTermFrequencyMap(i);
        dftMap = new HashMap<>(tfMap.size());

        instance.setDocument(i);
        BytesRef br = instance.next();
        while (br != null) {
          final BytesWrap bw = new BytesWrap(br);
          if (dftMap.containsKey(bw)) {
            dftMap.put(bw.clone(), dftMap.get(bw) + instance.
                    getTotalTermFreq());
          } else {
            dftMap.put(bw.clone(), instance.getTotalTermFreq());
          }
          br = instance.next();
        }

        assertEquals("Term map sizes differ.", tfMap.size(), dftMap.size());
        assertTrue("Not all terms are present.", dftMap.keySet().containsAll(
                tfMap.keySet()));

        for (Entry<BytesWrap, Long> tfEntry : tfMap.entrySet()) {
          assertEquals("Term frequency values differ. docId=" + i + " term="
                  + tfEntry.toString(), tfEntry.getValue(),
                  (Long) dftMap.get(tfEntry.getKey()));
        }
      }
      index.setFieldState(fieldState);
    } else {
      LOG.warn("Skip test section. Field count == 1.");
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

    Map<BytesWrap, Long> dftMap;
    Map<BytesWrap, Long> tfMap;

    final DocFieldsTermsEnum instance = new DocFieldsTermsEnum();
    for (int i = 0; i < index.getDocumentCount(); i++) {
      tfMap = index.getDocumentTermFrequencyMap(i);
      dftMap = new HashMap<>(tfMap.size());

      instance.setDocument(i);
      BytesRef br = instance.next();
      while (br != null) {
        final BytesWrap bw = new BytesWrap(br);
        if (dftMap.containsKey(bw)) {
          dftMap.put(bw.clone(), dftMap.get(bw) + instance.getTotalTermFreq());
        } else {
          dftMap.put(bw.clone(), instance.getTotalTermFreq());
        }
        br = instance.next();
      }

      assertEquals("Term map sizes differ.", tfMap.size(), dftMap.size());
      assertTrue("Not all terms are present.", dftMap.keySet().containsAll(
              tfMap.keySet()));

      for (Entry<BytesWrap, Long> tfEntry : tfMap.entrySet()) {
        assertEquals("Term frequency values differ. docId=" + i + " term="
                + tfEntry.toString(), tfEntry.getValue(), (Long) dftMap.get(
                        tfEntry.getKey()));
      }
    }
  }

}
