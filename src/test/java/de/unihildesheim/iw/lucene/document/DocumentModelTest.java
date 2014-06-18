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
import de.unihildesheim.iw.util.RandomValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Test for {@link DocumentModel}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("ParameterizedParametersStaticCollection")
@RunWith(Parameterized.class)
public final class DocumentModelTest
    extends MultiIndexDataProviderTestCase {

  /**
   * Initialize test with the current parameter.
   *
   * @param dataProv {@link IndexDataProvider} to use
   * @param rType Data provider configuration
   */
  public DocumentModelTest(final DataProviders dataProv,
      final RunType rType) {
    super(dataProv, rType);
  }

  /**
   * Test of contains method, of class DocumentModel.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testContains()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final Collection<ByteArray> stopwords =
          this.referenceIndex.getStopwordsBytes();
      final boolean excludeStopwords = stopwords != null;
      final Metrics metrics = new Metrics(index);

      final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
      while (docIdIt.hasNext()) {
        final int docId = docIdIt.next();
        final DocumentModel docModel = metrics.getDocumentModel(docId);
        final DocumentModel docModelExp = index.getDocumentModel(docId);
        for (final ByteArray bw : docModel.getTermFreqMap().keySet()) {
          Assert.assertEquals(msg("Document contains term mismatch (stopped: "
                  + excludeStopwords + ")."),
              docModelExp.contains(bw), docModel.contains(bw));
          if (excludeStopwords && stopwords != null) {
            Assert.assertFalse(
                msg("Document contains stopword (stopped: true)."),
                stopwords.contains(bw));
          }
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
  public void testTermFreqMap()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final Collection<ByteArray> stopwords =
          this.referenceIndex.getStopwordsBytes();
      final boolean excludeStopwords = stopwords != null;
      final Metrics metrics = new Metrics(index);

      final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
      while (docIdIt.hasNext()) {
        final int docId = docIdIt.next();
        final Map<ByteArray, Long> tfMap =
            metrics.getDocumentModel(docId).getTermFreqMap();
        final Map<ByteArray, Long> tfMapExp = this.referenceIndex
            .getDocumentTermFrequencyMap(docId);

        Assert.assertEquals(
            msg("Term count mismatch between referenceIndex and model."),
            (long) tfMapExp.size(), (long) tfMap.size());
        Assert
            .assertTrue(msg("Term mismatch between referenceIndex and model."),
                tfMapExp.keySet().containsAll(tfMap.keySet()));

        for (final Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
          Assert.assertEquals(msg("Document term frequency mismatch "
                  + "between referenceIndex and model."),
              tfMapExp.get(tfEntry.getKey()), tfEntry.getValue());
          if (excludeStopwords && stopwords != null) {
            Assert.assertFalse(msg("Stopword found in model."),
                stopwords.contains(tfEntry.getKey()));
          }
        }
      }
    }
  }

  /**
   * Test of equals method, of class DocumentModel.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testEquals()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final Metrics metrics = new Metrics(index);

      final int firstDocId = RandomValue.getInteger(0, metrics.collection().
          numberOfDocuments().intValue() - 1);
      int secondDocId = RandomValue.getInteger(0, metrics.collection().
          numberOfDocuments().intValue() - 1);
      while (secondDocId == firstDocId) {
        secondDocId = RandomValue.getInteger(0, metrics.collection().
            numberOfDocuments().intValue() - 1);
      }

      final DocumentModel firstDocModel = metrics.getDocumentModel(firstDocId);
      final DocumentModel secondDocModel =
          metrics.getDocumentModel(secondDocId);

      Assert.assertFalse(msg("DocModels should not be the same."),
          firstDocModel.equals(secondDocModel));

      final DocumentModel.Builder derivedDocModel
          = new DocumentModel.Builder(firstDocModel);
      // add a new term with it's frequency value, to make this model different
      final byte[] termBytes = "foo#Bar#Value".getBytes();
      derivedDocModel.setTermFrequency(new ByteArray(termBytes), 10L);
      Assert.assertFalse(msg("Derived DocumentModel should not be the same "
          + "as the original one."), firstDocModel.equals(
          derivedDocModel.getModel()));
    }
  }

  /**
   * Test of hashCode method, of class DocumentModel.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testHashCode()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final Metrics metrics = new Metrics(index);
      final int firstDocId = RandomValue.getInteger(0, metrics.collection().
          numberOfDocuments().intValue() - 1);
      int secondDocId = RandomValue.getInteger(0, metrics.collection().
          numberOfDocuments().intValue() - 1);
      while (secondDocId == firstDocId) {
        secondDocId = RandomValue.getInteger(0, metrics.collection().
            numberOfDocuments().intValue() - 1);
      }

      final DocumentModel firstDocModel = metrics.getDocumentModel(firstDocId);
      final DocumentModel secondDocModel =
          metrics.getDocumentModel(secondDocId);

      // test two different models
      Assert.assertNotEquals(msg("DocModels hashCode should not be the same. ("
              + firstDocModel.id + ", " + secondDocModel.id + ")"),
          (long) firstDocModel.hashCode(), (long) secondDocModel.hashCode());

      // get the same model again an test
      Assert.assertEquals(msg("DocModels hashCode should be the same "
              + "for the same document."),
          (long) firstDocModel.hashCode(),
          (long) metrics.getDocumentModel(firstDocId).hashCode());

      // change a model
      final DocumentModel.Builder derivedDocModel
          = new DocumentModel.Builder(firstDocModel);
      // add a new term with it's frequency value, to make this model different
      final byte[] termBytes = "foo#Bar#Value".getBytes();
      derivedDocModel.setTermFrequency(new ByteArray(termBytes), 10L);

      Assert.assertNotEquals(msg("HashCode of derived DocumentModel should "
              + "not be the same as the original one."),
          (long) firstDocModel.hashCode(),
          (long) derivedDocModel.getModel().hashCode());
    }
  }

  /**
   * Test of getSmoothedTermFrequency method, of class DocumentModel.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testGetSmoothedRelativeTermFrequency()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final Metrics metrics = new Metrics(index);
      final int smoothingAmount = 100;
      final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
      while (docIdIt.hasNext()) {
        final int docId = docIdIt.next();
        final DocumentModel docModel = metrics.getDocumentModel(docId);
        final Metrics.DocumentMetrics dm = docModel.metrics();
        for (final ByteArray bw : docModel.getTermFreqMap().keySet()) {
          Assert.assertNotEquals(
              msg("Smoothed and absolute relative term frequency "
                  + "should not be the same."), dm.relTf(bw), dm.
                  smoothedRelativeTermFrequency(bw, (double) smoothingAmount));
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
  @SuppressWarnings("ConstantConditions")
  public void testTf()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final Metrics metrics = new Metrics(index);
      final Collection<ByteArray> stopwords =
          this.referenceIndex.getStopwordsBytes();
      final boolean excludeStopwords = stopwords != null;

      final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
      while (docIdIt.hasNext()) {
        final int docId = docIdIt.next();
        final DocumentModel docModel = metrics.getDocumentModel(docId);
        final Map<ByteArray, Long> tfMap = this.referenceIndex
            .getDocumentTermFrequencyMap(docId);
        for (final Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
          Assert.assertEquals(msg("Term frequency mismatch."),
              docModel.tf(tfEntry.getKey()), tfEntry.getValue());
          if (excludeStopwords) {
            Assert.assertFalse(msg("Stopword found in model."),
                stopwords.contains(tfEntry.getKey()));
          }
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
  public void testTermCount()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final Metrics metrics = new Metrics(index);
      final Collection<ByteArray> stopwords =
          this.referenceIndex.getStopwordsBytes();
      final boolean excludeStopwords = stopwords != null;

      final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
      while (docIdIt.hasNext()) {
        final int docId = docIdIt.next();
        final DocumentModel docModel = metrics.getDocumentModel(docId);
        final Map<ByteArray, Long> tfMap = this.referenceIndex
            .getDocumentTermFrequencyMap(docId);
        Assert.assertEquals(msg("Unique term count mismatch."),
            docModel.termCount(), (long) tfMap.size());
        if (excludeStopwords) {
          for (final ByteArray term : tfMap.keySet()) {
            Assert.assertFalse(msg("Stopword found in model."),
                stopwords.contains(term));
          }
        }
      }
    }
  }

  /**
   * Test of metrics method, of class DocumentModel.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testMetrics()
      throws Exception {
    try (final IndexDataProvider index = getInstance()) {
      final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
      final Metrics metrics = new Metrics(index);
      while (docIdIt.hasNext()) {
        final int docId = docIdIt.next();
        final DocumentModel docModel = metrics.getDocumentModel(docId);
        Assert.assertNotNull(msg("Metrics not found."),
            docModel.metrics());
      }
    }
  }

}
