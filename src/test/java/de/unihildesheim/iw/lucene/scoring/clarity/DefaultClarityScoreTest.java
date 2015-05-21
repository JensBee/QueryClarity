/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

package de.unihildesheim.iw.lucene.scoring.clarity;

import de.unihildesheim.iw.util.Buildable;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.scoring.clarity.DefaultClarityScore
    .ModelHighPrecision;
import de.unihildesheim.iw.lucene.scoring.clarity.DefaultClarityScore
    .ModelLowPrecision;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RoaringDocIdSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test for {@link DefaultClarityScore}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class DefaultClarityScoreTest
    extends TestCase {
  public DefaultClarityScoreTest() {
    super(LoggerFactory.getLogger(DefaultClarityScoreTest.class));
  }

  private static final double LANG_MODEL_WEIGHT = 0.6;

  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  @Test
  public void testBuilder()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      new DefaultClarityScore.Builder()
          .configuration(new DefaultClarityScoreConfiguration())
          .analyzer(new WhitespaceAnalyzer())
          .indexDataProvider(idx.getIdp())
          .indexReader(idx.getReader())
          .build();
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testModelLowPrecision_constructor()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final IndexDataProvider idp = idx.getIdp();
      // query
      final BytesRefArray qt = new BytesRefArray(Counter.newCounter(false));
      qt.append(new BytesRef("document1"));
      qt.append(new BytesRef("value"));
      // feedback documents
      @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
      final DocIdSet fb = new RoaringDocIdSet.Builder(3)
          .add(0).add(1).add(2).build();
      final ModelLowPrecision lpMod = new ModelLowPrecision(
          idp, LANG_MODEL_WEIGHT, qt, fb);

      Assert.assertEquals("Number of cached document models differs.",
          idx.docs, lpMod.docModels.size());
      Assert.assertEquals("Wrong number of feedback documents.",
          3L, lpMod.feedbackDocs.length);
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testModelLowPrecision_docModel()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final IndexDataProvider idp = idx.getIdp();
      // query
      final BytesRefArray qt = new BytesRefArray(Counter.newCounter(false));
      qt.append(new BytesRef("document1"));
      qt.append(new BytesRef("value"));
      // feedback documents
      @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
      final DocIdSet fb = new RoaringDocIdSet.Builder(3)
          .add(0).add(1).add(2).build();
      final ModelLowPrecision lpMod = new ModelLowPrecision(
          idp, LANG_MODEL_WEIGHT, qt, fb);

      final BytesRef term = new BytesRef("document1");
      // (0.6 * (3/18)) + (1 - 0.6) * relTf(term)
      final double expected =
          (0.6 * (3d / 18d)) + (1d - 0.6) * idp.getRelativeTermFrequency(term);
      final double result = lpMod.document(idp.getDocumentModel(0), term);
      Assert.assertEquals("Document model result differs.",
          expected, result, 0d);
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testModelLowPrecision_queryModel()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final IndexDataProvider idp = idx.getIdp();
      // query
      final BytesRef qt1 = new BytesRef("document1");
      final BytesRef qt2 = new BytesRef("value");
      final BytesRefArray qt = new BytesRefArray(Counter.newCounter(false));
      qt.append(qt1);
      qt.append(qt2);
      // feedback documents
      @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
      final DocIdSet fb = new RoaringDocIdSet.Builder(3)
          .add(0).add(1).add(2).build();
      final ModelLowPrecision lpMod = new ModelLowPrecision(
          idp, LANG_MODEL_WEIGHT, qt, fb);

      final BytesRef term = new BytesRef("document1");
      final double expected =
          // doc-0: docModel query term
          (((0.6 * (3d / 18d)) + (1d - 0.6) *
              idp.getRelativeTermFrequency(term)) *
              // doc-0: docModel for all query terms
              ((0.6 * (3d / 18d)) + (1d - 0.6) *
                  idp.getRelativeTermFrequency(qt1)) *
              ((0.6 * (3d / 18d)) + (1d - 0.6) *
                  idp.getRelativeTermFrequency(qt2))) +
              // doc-1: docModel query term
              ((1d - 0.6) * idp.getRelativeTermFrequency(term) *
                  // doc-1: docModel for all query terms
                  ((1d - 0.6) * idp.getRelativeTermFrequency(qt1)) *
                  ((0.6 * (3d / 18d)) + (1d - 0.6) *
                      idp.getRelativeTermFrequency(qt2))) +
              // doc-2: docModel query term
              ((1d - 0.6) * idp.getRelativeTermFrequency(term) *
                  // doc-2: docModel for all query terms
                  ((1d - 0.6) * idp.getRelativeTermFrequency(qt1)) *
                  ((0.6 * (3d / 18d)) + (1d - 0.6) *
                      idp.getRelativeTermFrequency(qt2)));
      final double result = lpMod.query(term);
      Assert.assertEquals("Query model result differs.",
          expected, result, 0.1e10);
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testModelHighPrecision_constructor()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final IndexDataProvider idp = idx.getIdp();
      // query
      final BytesRefArray qt = new BytesRefArray(Counter.newCounter(false));
      qt.append(new BytesRef("document1"));
      qt.append(new BytesRef("value"));
      // feedback documents
      @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
      final DocIdSet fb = new RoaringDocIdSet.Builder(3)
          .add(0).add(1).add(2).build();
      final ModelHighPrecision hpMod = new ModelHighPrecision(
          idp, BigDecimal.valueOf(LANG_MODEL_WEIGHT), qt, fb);

      Assert.assertEquals("Number of cached document models differs.",
          idx.docs, hpMod.docModels.size());
      Assert.assertEquals("Wrong number of feedback documents.",
          3L, hpMod.feedbackDocs.length);
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testModelHighPrecision_docModel()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final IndexDataProvider idp = idx.getIdp();
      // query
      final BytesRefArray qt = new BytesRefArray(Counter.newCounter(false));
      qt.append(new BytesRef("document1"));
      qt.append(new BytesRef("value"));
      // feedback documents
      @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
      final DocIdSet fb = new RoaringDocIdSet.Builder(3)
          .add(0).add(1).add(2).build();
      final ModelHighPrecision hpMod = new ModelHighPrecision(
          idp, BigDecimal.valueOf(LANG_MODEL_WEIGHT), qt, fb);

      final BytesRef term = new BytesRef("document1");
      // (0.6 * (3/18)) + (1 - 0.6) * relTf(term)
      final double expected =
          (0.6 * (3d / 18d)) + (1d - 0.6) * idp.getRelativeTermFrequency(term);
      final double result = hpMod.document(idp.getDocumentModel(0), term)
          .doubleValue();
      Assert.assertEquals("Document model result differs.",
          expected, result, 0.1e10);
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testModelHighPrecision_queryModel()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final IndexDataProvider idp = idx.getIdp();
      // query
      final BytesRef qt1 = new BytesRef("document1");
      final BytesRef qt2 = new BytesRef("value");
      final BytesRefArray qt = new BytesRefArray(Counter.newCounter(false));
      qt.append(qt1);
      qt.append(qt2);
      // feedback documents
      @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
      final DocIdSet fb = new RoaringDocIdSet.Builder(3)
          .add(0).add(1).add(2).build();
      final ModelHighPrecision hpMod = new ModelHighPrecision(
          idp, BigDecimal.valueOf(LANG_MODEL_WEIGHT), qt, fb);

      final BytesRef term = new BytesRef("document1");
      final double expected =
          // doc-0: docModel query term
          (((0.6 * (3d / 18d)) + (1d - 0.6) *
              idp.getRelativeTermFrequency(term)) *
              // doc-0: docModel for all query terms
              ((0.6 * (3d / 18d)) + (1d - 0.6) *
                  idp.getRelativeTermFrequency(qt1)) *
              ((0.6 * (3d / 18d)) + (1d - 0.6) *
                  idp.getRelativeTermFrequency(qt2))) +
              // doc-1: docModel query term
              ((1d - 0.6) * idp.getRelativeTermFrequency(term) *
                  // doc-1: docModel for all query terms
                  ((1d - 0.6) * idp.getRelativeTermFrequency(qt1)) *
                  ((0.6 * (3d / 18d)) + (1d - 0.6) *
                      idp.getRelativeTermFrequency(qt2))) +
              // doc-2: docModel query term
              ((1d - 0.6) * idp.getRelativeTermFrequency(term) *
                  // doc-2: docModel for all query terms
                  ((1d - 0.6) * idp.getRelativeTermFrequency(qt1)) *
                  ((0.6 * (3d / 18d)) + (1d - 0.6) *
                      idp.getRelativeTermFrequency(qt2)));
      final double result = hpMod.query(term).doubleValue();
      Assert.assertEquals("Query model result differs.",
          expected, result, 0.1e10);
    }
  }

  /**
   * Simple static memory index for testing.
   *
   * @author Jens Bertram (code@jens-bertram.net)
   */
  @SuppressWarnings("JavaDoc")
  static final class TestMemIndex
      implements AutoCloseable {
    final Directory dir;
    /**
     * Document fields.
     */
    List<String> flds;
    /**
     * Number of document.
     */
    int docs;

    @SuppressWarnings("resource")
    TestMemIndex()
        throws IOException {
      this.dir = new RAMDirectory();
      final IndexWriter wrtr = new IndexWriter(this.dir,
          new IndexWriterConfig(
              new org.apache.lucene.analysis.core.WhitespaceAnalyzer()));
      wrtr.addDocuments(getIndexDocs());
      wrtr.close();
    }

    DirectoryReader getReader()
        throws IOException {
      return DirectoryReader.open(this.dir);
    }

    @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
    IndexDataProvider getIdp()
        throws IOException, Buildable.ConfigurationException,
               Buildable.BuildException {
      final DirectoryReader reader = DirectoryReader.open(this.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      return new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();
    }

    Iterable<Document> getIndexDocs() {
      this.flds = Arrays.asList("f1", "f2", "f3");

      final Collection<Document> docs = new ArrayList<>(3);

      final Document doc1 = new Document();
      doc1.add(new VecTextField("f1",
          "first field value document1 field1 document1field1", Store.NO));
      doc1.add(new VecTextField("f2",
          "second field value document1 field2 document1field2", Store.NO));
      doc1.add(new VecTextField("f3",
          "third field value document1 field3 document1field3", Store.NO));
      docs.add(doc1);

      final Document doc2 = new Document();
      doc2.add(new VecTextField("f1",
          "first field value document2 field1 document2field1", Store.NO));
      doc2.add(new VecTextField("f2",
          "second field value document2 field2 document2field2", Store.NO));
      doc2.add(new VecTextField("f3",
          "third field value document2 field3 document2field3", Store.NO));
      docs.add(doc2);

      final Document doc3 = new Document();
      doc3.add(new VecTextField("f1",
          "first field value document3 field1 document3field1", Store.NO));
      doc3.add(new VecTextField("f2",
          "second field value document3 field2 document3field2", Store.NO));
      doc3.add(new VecTextField("f3",
          "third field value document3 field3 document3field3", Store.NO));
      docs.add(doc3);

      this.docs = docs.size();
      return docs;
    }

    @Override
    public void close()
        throws Exception {
      this.dir.close();
    }
  }

  private static final class WhitespaceAnalyzer
      extends Analyzer {

    WhitespaceAnalyzer() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
      return new TokenStreamComponents(new WhitespaceTokenizer());
    }
  }
}