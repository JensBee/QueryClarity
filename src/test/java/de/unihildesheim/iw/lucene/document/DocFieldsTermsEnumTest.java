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
package de.unihildesheim.iw.lucene.document;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.MultiIndexDataProviderTestCase;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Test for {@link DocFieldsTermsEnum}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("ParameterizedParametersStaticCollection")
@RunWith(Parameterized.class)
public final class DocFieldsTermsEnumTest
    extends MultiIndexDataProviderTestCase {

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public DocFieldsTermsEnumTest(final DataProviders dataProv,
      final RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Test of setDocument method, of class DocFieldsTermsEnum.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetDocument()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final DocFieldsTermsEnum instance = new DocFieldsTermsEnum(
          TestIndexDataProvider.getIndexReader(),
          index.getDocumentFields());
      final Iterator<Integer> docIdIt = index.getDocumentIds();
      while (docIdIt.hasNext()) {
        instance.setDocument(docIdIt.next());
      }
    }
  }

  /**
   * Test of reset method, of class DocFieldsTermsEnum.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testReset()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final DocFieldsTermsEnum instance = new DocFieldsTermsEnum(
          TestIndexDataProvider.getIndexReader(), index.getDocumentFields());
      final Iterator<Integer> docIdIt = index.getDocumentIds();

      BytesRef br;
      while (docIdIt.hasNext()) {
        instance.setDocument(docIdIt.next());
        int count = 0;
        int oldCount = 0;

        br = instance.next();
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
        Assert.assertEquals(
            msg("Re-set iteration yields different count.", index),
            (long) oldCount, (long) count);
      }
    }
  }

  /**
   * Test of next method, of class DocFieldsTermsEnum.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testNext()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final DocFieldsTermsEnum instance =
          new DocFieldsTermsEnum(TestIndexDataProvider.getIndexReader(),
              index.getDocumentFields());
      final Collection<ByteArray> stopwords =
          this.referenceIndex.getStopwordsBytes();
      final boolean excludeStopwords = this.referenceIndex.hasStopwords();
      final Metrics metrics = new Metrics(index);

      final Iterator<Integer> docIdIt = index.getDocumentIds();
      while (docIdIt.hasNext()) {
        final int docId = docIdIt.next();
        final Map<ByteArray, Long> tfMap =
            metrics.getDocumentModel(docId).getTermFreqMap();
        final Map<ByteArray, Long> dftMap = new HashMap<>(tfMap.size());

        instance.setDocument(docId);
        BytesRef br = instance.next();
        while (br != null) {
          final ByteArray bytes = BytesRefUtils.toByteArray(br);
          if (excludeStopwords && stopwords.contains(bytes)) {
            br = instance.next();
            continue;
          }
          if (dftMap.containsKey(bytes)) {
            dftMap.put(new ByteArray(bytes), dftMap.get(bytes) + instance.
                getTotalTermFreq());
          } else {
            dftMap.put(new ByteArray(bytes), instance.getTotalTermFreq());
          }
          br = instance.next();
        }

        Assert.assertEquals(
            msg("Term map sizes differs (stopped: " + excludeStopwords
                + ").", index), (long) tfMap.size(), (long) dftMap.size());
        Assert.assertTrue(msg("Not all terms are present (stopped: " +
            excludeStopwords + ").", index), dftMap.keySet().containsAll
            (tfMap.keySet()));

        for (final Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
          Assert.assertEquals(msg("Term frequency values differs (stopped: " +
                  excludeStopwords + "). docId=" + docId + " term=" +
                  tfEntry, index), tfEntry.getValue(),
              dftMap.get(tfEntry.getKey()));
        }
      }
    }
  }

  /**
   * Test of getTotalTermFreq method, of class DocFieldsTermsEnum.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetTotalTermFreq()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final DocFieldsTermsEnum instance =
          new DocFieldsTermsEnum(TestIndexDataProvider.getIndexReader(),
              index.getDocumentFields());
      final Collection<ByteArray> stopwords =
          this.referenceIndex.getStopwordsBytes();
      final boolean excludeStopwords = this.referenceIndex.hasStopwords();

      final Iterator<Integer> docIdIt = index.getDocumentIds();
      while (docIdIt.hasNext()) {
        final int docId = docIdIt.next();
        final Map<ByteArray, Long> tfMap = this.referenceIndex
            .getDocumentTermFrequencyMap(docId);
        @SuppressWarnings("ObjectAllocationInLoop")
        final Map<ByteArray, Long> dftMap = new HashMap<>(tfMap.size());

        instance.setDocument(docId);
        BytesRef br = instance.next();
        while (br != null) {
          final ByteArray bytes = BytesRefUtils.toByteArray(br);
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

        Assert.assertEquals(msg("Term map sizes differ (stopped: " +
                excludeStopwords + ").", index), (long) tfMap.size(),
            (long) dftMap.size()
        );
        Assert.assertTrue(
            msg("Not all terms are present (stopped: " + excludeStopwords
                + ").", index), dftMap.keySet().containsAll(tfMap.keySet())
        );

        for (final Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
          Assert.assertEquals(
              msg("Term frequency values differ (stopped: " + excludeStopwords +
                  "). docId=" + docId + " term=" + tfEntry, index),
              tfEntry.getValue(), dftMap.get(tfEntry.getKey())
          );
        }
      }
    }
  }
}
