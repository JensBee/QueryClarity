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
import de.unihildesheim.lucene.metrics.DocumentMetrics;
import de.unihildesheim.lucene.util.BytesRefUtil;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.util.BytesRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Test for {@link DocFieldsTermsEnum}.
 *
 * @author Jens Bertram
 */
public final class DocFieldsTermsEnumTest
        extends MultiIndexDataProviderTestCase {

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
    DocFieldsTermsEnum instance = new DocFieldsTermsEnum();
    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      instance.setDocument(docIdIt.next());
    }
  }

  /**
   * Test of reset method, of class DocFieldsTermsEnum.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testReset() throws Exception {
    DocFieldsTermsEnum instance = new DocFieldsTermsEnum();
    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      instance.setDocument(docIdIt.next());
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
      assertEquals(msg("Resetted iteration yields different count."), oldCount,
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
    final DocFieldsTermsEnum instance = new DocFieldsTermsEnum();

    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final Map<ByteArray, Long> tfMap
              = DocumentMetrics.getModel(docId).termFreqMap;
      final Map<ByteArray, Long> dftMap = new HashMap<>(tfMap.size());

      instance.setDocument(docId);
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

      assertEquals(msg("Term map sizes differs (stopped: " + excludeStopwords
              + ")."), tfMap.size(), dftMap.size());
      assertTrue(msg("Not all terms are present (stopped: " + excludeStopwords
              + ")."), dftMap.keySet().containsAll(tfMap.keySet()));

      for (Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
        assertEquals(msg("Term frequency values differs (stopped: "
                + excludeStopwords + "). docId=" + docId + " term="
                + tfEntry.toString()), tfEntry.getValue(), dftMap.get(tfEntry.
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
    final DocFieldsTermsEnum instance = new DocFieldsTermsEnum();

    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final Map<ByteArray, Long> tfMap = index.getDocumentTermFrequencyMap(
              docId);
      final Map<ByteArray, Long> dftMap = new HashMap<>(tfMap.size());

      instance.setDocument(docId);
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

      assertEquals(msg("Term map sizes differ (stopped: " + excludeStopwords
              + ")."), tfMap.size(), dftMap.size());
      assertTrue(msg("Not all terms are present (stopped: " + excludeStopwords
              + ")."), dftMap.keySet().containsAll(tfMap.keySet()));

      for (Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
        assertEquals(msg("Term frequency values differ (stopped: "
                + excludeStopwords + "). docId=" + docId + " term="
                + tfEntry.toString()), tfEntry.getValue(), dftMap.get(
                        tfEntry.getKey()));
      }
    }
  }
}
