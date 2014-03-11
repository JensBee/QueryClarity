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

import de.unihildesheim.lucene.index.TestIndex;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.StringUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DocFieldsTermsEnum}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DocFieldsTermsEnumTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DocFieldsTermsEnumTest.class);

  /**
   * Test documents index.
   */
  private static TestIndex index;

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    index = new TestIndex();
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
   * Test of setDocument method, of class DocFieldsTermsEnum.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetDocument() throws Exception {
    LOG.info("Test setDocument");

    DocFieldsTermsEnum instance = new DocFieldsTermsEnum(index.getReader(),
            index.getFields());
    for (int i = 0; i < index.getDocumentCount(); i++) {
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
    DocFieldsTermsEnum instance = new DocFieldsTermsEnum(index.getReader(),
            index.getFields());
    for (int i = 0; i < index.getDocumentCount(); i++) {
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
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testNext() throws Exception {
    LOG.info("Test next");
    DocFieldsTermsEnum instance;

    Map<BytesWrap, Long> dftMap;
    Map<BytesWrap, Number> tfMap;

    // test with all fields enabled
    instance = new DocFieldsTermsEnum(index.getReader(), index.getFields());
    for (int i = 0; i < index.getDocumentCount(); i++) {
      tfMap = index.getDocumentTermFrequencyMap(i);
      dftMap = new HashMap<>(tfMap.size());

      instance.setDocument(i);
      BytesRef br = instance.next();
      while (br != null) {
        final BytesWrap bw = new BytesWrap(br);
        if (dftMap.containsKey(bw)) {
          dftMap.put(bw, dftMap.get(bw) + instance.getTotalTermFreq());
        } else {
          dftMap.put(bw, instance.getTotalTermFreq());
        }
        br = instance.next();
      }

      assertEquals("Term map sizes differ.", tfMap.size(), dftMap.size());
      assertTrue("Not all terms are present.", dftMap.keySet().containsAll(
              tfMap.keySet()));

      for (Entry<BytesWrap, Number> tfEntry : tfMap.entrySet()) {
        assertEquals("Term frequency values differ. docId=" + i + " term="
                + tfEntry.toString(), (Long) tfEntry.
                getValue().longValue(), (Long) dftMap.get(tfEntry.getKey()));
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
      instance = new DocFieldsTermsEnum(index.getReader(),
              index.getFields());

      for (int i = 0; i < index.getDocumentCount(); i++) {
        tfMap = index.getDocumentTermFrequencyMap(i);
        dftMap = new HashMap<>(tfMap.size());

        instance.setDocument(i);
        BytesRef br = instance.next();
        while (br != null) {
          final BytesWrap bw = new BytesWrap(br);
          if (dftMap.containsKey(bw)) {
            dftMap.put(bw, dftMap.get(bw) + instance.getTotalTermFreq());
          } else {
            dftMap.put(bw, instance.getTotalTermFreq());
          }
          br = instance.next();
        }

        assertEquals("Term map sizes differ.", tfMap.size(), dftMap.size());
        assertTrue("Not all terms are present.", dftMap.keySet().containsAll(
                tfMap.keySet()));

        for (Entry<BytesWrap, Number> tfEntry : tfMap.entrySet()) {
          assertEquals("Term frequency values differ. docId=" + i + " term="
                  + tfEntry.toString(), (Long) tfEntry.
                  getValue().longValue(), (Long) dftMap.get(tfEntry.getKey()));
        }
      }
      index.setFieldState(fieldState);
    } else {
      LOG.warn("Skip test section. Field count == 1.");
    }
  }

  /**
   * Test of getTotalTermFreq method, of class DocFieldsTermsEnum.
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTotalTermFreq() throws Exception {
    LOG.info("Test getTotalTermFreq");

    Map<BytesWrap, Long> dftMap;
    Map<BytesWrap, Number> tfMap;

    final DocFieldsTermsEnum instance = new DocFieldsTermsEnum(index.
            getReader(), index.getFields());
    for (int i = 0; i < index.getDocumentCount(); i++) {
      tfMap = index.getDocumentTermFrequencyMap(i);
      dftMap = new HashMap<>(tfMap.size());

      instance.setDocument(i);
      BytesRef br = instance.next();
      while (br != null) {
        final BytesWrap bw = new BytesWrap(br);
        if (dftMap.containsKey(bw)) {
          dftMap.put(bw, dftMap.get(bw) + instance.getTotalTermFreq());
        } else {
          dftMap.put(bw, instance.getTotalTermFreq());
        }
        br = instance.next();
      }

      assertEquals("Term map sizes differ.", tfMap.size(), dftMap.size());
      assertTrue("Not all terms are present.", dftMap.keySet().containsAll(
              tfMap.keySet()));

      for (Entry<BytesWrap, Number> tfEntry : tfMap.entrySet()) {
        assertEquals("Term frequency values differ. docId=" + i + " term="
                + tfEntry.toString(), (Long) tfEntry.
                getValue().longValue(), (Long) dftMap.get(tfEntry.getKey()));
      }
    }
  }

}
