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
import de.unihildesheim.iw.lucene.index.IndexTestUtil;
import de.unihildesheim.iw.lucene.metrics.CollectionMetrics;
import de.unihildesheim.iw.lucene.metrics.DocumentMetrics;
import de.unihildesheim.iw.util.RandomValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.*;

/**
 * Test for {@link DocumentModel}.
 *
 * @author Jens Bertram
 */
@RunWith(Parameterized.class)
public final class DocumentModelTest
    extends MultiIndexDataProviderTestCase {

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
   * Test of contains method, of class DocumentModel.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("null")
  public void testContains()
      throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
        getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = DocumentMetrics.getModel(docId);
      final DocumentModel docModelExp = index.getDocumentModel(docId);
      for (ByteArray bw : docModel.termFreqMap.keySet()) {
        assertEquals(msg("Document contains term mismatch (stopped: "
                         + excludeStopwords + ")."), docModelExp.contains(bw),
            docModel.contains(bw)
        );
        if (excludeStopwords && stopwords != null) {
          assertFalse(msg("Document contains stopword (stopped: "
                          + excludeStopwords + ")."), stopwords.contains(bw));
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
  @SuppressWarnings("null")
  public void testTermFreqMap()
      throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
        getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final Map<ByteArray, Long> tfMap
          = DocumentMetrics.getModel(docId).termFreqMap;
      final Map<ByteArray, Long> tfMapExp = index.getDocumentTermFrequencyMap(
          docId);

      assertEquals(msg("Term count mismatch between index and model."),
          tfMapExp.size(), tfMap.size());
      assertTrue(msg("Term mismatch between index and model."), tfMapExp.
          keySet().containsAll(tfMap.keySet()));

      for (Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
        assertEquals(msg("Document term frequency mismatch "
                         + "between index and model."),
            tfMapExp.get(tfEntry.getKey()),
            tfEntry.getValue()
        );
        if (excludeStopwords && stopwords != null) {
          assertFalse(msg("Stopword found in model."), stopwords.contains(
              tfEntry.getKey()));
        }
      }
    }
  }

  /**
   * Test of equals method, of class DocumentModel.
   */
  @Test
  @SuppressWarnings({"DM_DEFAULT_ENCODING", "checkstyle:magicnumber"})
  public void testEquals() {
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

    assertFalse(msg("DocModels should not be the same."),
        firstDocModel.equals(secondDocModel));

    final DocumentModel.DocumentModelBuilder derivedDocModel
        = new DocumentModel.DocumentModelBuilder(firstDocModel);
    // add a new term with it's frequency value, to make this model different
    final byte[] termBytes = "foo#Bar#Value".getBytes();
    derivedDocModel.setTermFrequency(new ByteArray(termBytes), 10);
    assertFalse(msg("Derived DocumentModel should not be the same "
                    + "as the original one."), firstDocModel.equals(
        derivedDocModel.getModel()));
  }

  /**
   * Test of hashCode method, of class DocumentModel.
   */
  @Test
  @SuppressWarnings("DM_DEFAULT_ENCODING")
  public void testHashCode() {
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

    // test two different models
    assertNotEquals(msg("DocModels hashCode should not be the same. ("
                        + firstDocModel.id + ", " + secondDocModel.id + ")"),
        firstDocModel.hashCode(), secondDocModel.hashCode()
    );

    // get the same model again an test
    assertEquals(msg("DocModels hashCode should be the same "
                     + "for the same document."), firstDocModel.hashCode(),
        DocumentMetrics.getModel(firstDocId).hashCode()
    );

    // change a model
    final DocumentModel.DocumentModelBuilder derivedDocModel
        = new DocumentModel.DocumentModelBuilder(firstDocModel);
    // add a new term with it's frequency value, to make this model different
    final byte[] termBytes = "foo#Bar#Value".getBytes();
    derivedDocModel.setTermFrequency(new ByteArray(termBytes), 10);

    assertNotEquals(msg("HashCode of derived DocumentModel should "
                        + "not be the same as the original one."),
        firstDocModel.
            hashCode(),
        derivedDocModel.getModel().hashCode()
    );
  }

  /**
   * Test of getSmoothedTermFrequency method, of class DocumentModel.
   */
  @Test
  public void testGetSmoothedRelativeTermFrequency() {
    final int smoothingAmount = 100;
    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = DocumentMetrics.getModel(docId);
      final DocumentMetrics dm = docModel.metrics();
      for (ByteArray bw : docModel.termFreqMap.keySet()) {
        assertNotEquals(msg("Smoothed and absolute relative term frequency "
                            + "should not be the same."), dm.relTf(bw), dm.
            smoothedRelativeTermFrequency(bw, smoothingAmount));
      }
    }
  }

  /**
   * Test of tf method, of class DocumentModel.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  @SuppressWarnings({"null", "ConstantConditions"})
  public void testTf()
      throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
        getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = DocumentMetrics.getModel(docId);
      final Map<ByteArray, Long> tfMap = index.getDocumentTermFrequencyMap(
          docId);
      for (Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
        assertEquals(msg("Term frequency mismatch."), docModel.tf(tfEntry.
            getKey()), tfEntry.getValue());
        if (excludeStopwords) {
          assertFalse(msg("Stopword found in model."), stopwords.contains(
              tfEntry.getKey()));
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
  @SuppressWarnings("null")
  public void testTermCount()
      throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
        getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = DocumentMetrics.getModel(docId);
      final Map<ByteArray, Long> tfMap = index.getDocumentTermFrequencyMap(
          docId);
      assertEquals(msg("Unique term count mismatch."), docModel.termCount(),
          tfMap.size());
      if (excludeStopwords) {
        for (ByteArray term : tfMap.keySet()) {
          assertFalse(msg("Stopword found in model."), stopwords.
              contains(term));
        }
      }
    }
  }

  /**
   * Test of metrics method, of class DocumentModel.
   */
  @Test
  public void testMetrics() {
    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = DocumentMetrics.getModel(docId);
      assertNotNull(msg("Metrics not found."), docModel.metrics());
    }
  }

}
