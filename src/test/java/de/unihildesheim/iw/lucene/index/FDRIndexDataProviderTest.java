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

package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProviderTest.TestMemIndex.Index;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RoaringDocIdSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Test for {@link FDRIndexDataProvider}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings({"JavaDoc", "UnnecessarilyQualifiedInnerClassAccess"})
public class FDRIndexDataProviderTest
    extends TestCase {
  public FDRIndexDataProviderTest() {
    super(LoggerFactory.getLogger(FDRIndexDataProviderTest.class));
  }

  @Test
  public void testBuilder_tvIndex()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();
    }
  }

  @Test
  public void testBuilder_noTvIndex()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.NO_TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      try {
        new FDRIndexDataProvider.Builder()
            .indexReader(idxReader)
            .build();
        Assert.fail("Expected an IllegalStateException to be thrown.");
      } catch (final IllegalStateException e) {
        // pass
      }
    }
  }

  @Test
  public void testGetTermFrequency()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      final long result = idp.getTermFrequency();
      Assert.assertEquals("Term frequency mismatch.", 54L, result);
    }
  }

  @Test
  public void testGetTermFrequency_term()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      Assert.assertEquals("Term frequency mismatch.",
          9L, idp.getTermFrequency(new BytesRef("value")));
      Assert.assertEquals("Term frequency mismatch.",
          3L, idp.getTermFrequency(new BytesRef("document1")));
      Assert.assertEquals("Term frequency mismatch.",
          3L, idp.getTermFrequency(new BytesRef("document2")));
      Assert.assertEquals("Term frequency mismatch.",
          3L, idp.getTermFrequency(new BytesRef("document3")));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetDocumentFrequency()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      Assert.assertEquals("Document frequency mismatch.",
          3L, idp.getDocumentFrequency(new BytesRef("value")));
      Assert.assertEquals("Document frequency mismatch.",
          1L, idp.getDocumentFrequency(new BytesRef("document1")));
      Assert.assertEquals("Document frequency mismatch.",
          1L, idp.getDocumentFrequency(new BytesRef("document2")));
      Assert.assertEquals("Document frequency mismatch.",
          1L, idp.getDocumentFrequency(new BytesRef("document3")));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetDocumentIds()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      final List<Integer> docIds =
          idp.getDocumentIds().collect(
              ArrayList<Integer>::new, List::add, List::addAll);

      Assert.assertEquals("Number of docIds differ.", idx.docs, docIds.size());
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetDocumentModel()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      final DocumentModel dm1 = idp.getDocumentModel(0);

      Assert.assertEquals("DocumentModel tf value differs.",
          3L, dm1.tf(new BytesRef("document1")));
      Assert.assertEquals("DocumentModel rel-tf value differs.",
          3d / 18d, dm1.relTf(new BytesRef("document1")), 0d);
      Assert.assertEquals("DocumentModel id differs.", 0L, dm1.id);
      Assert.assertEquals("DocumentModel term count differs.",
          12L, dm1.termCount());
    }
  }

  @Test
  public void testHasDocument()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      for (int i = 0; i < idx.docs; i++) {
        Assert.assertTrue("Document missing.", idp.hasDocument(i));
      }

      Assert.assertFalse("Unknown document found.",
          idp.hasDocument(idx.docs + 1));
    }
  }

  @Test
  public void testGetDocumentsTerms_single()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      Stream<BytesRef> terms;
      terms = idp.getDocumentsTerms(
          new RoaringDocIdSet.Builder(1).add(0).build());
      Assert.assertEquals("Number of terms mismatch.", 12L, terms.count());

      terms = idp.getDocumentsTerms(
          new RoaringDocIdSet.Builder(1).add(0).build());
      Assert.assertEquals("Number of terms mismatch.", 1L,
          terms.filter(t -> "document1".equals(t.utf8ToString())).count());
    }
  }

  @Test
  public void testGetDocumentsTerms_multi()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      Stream<BytesRef> terms;
      terms = idp.getDocumentsTerms(
          new RoaringDocIdSet.Builder(3).add(0).add(2).build());
      Assert.assertEquals("Number of terms mismatch.", 16L, terms.count());

      terms = idp.getDocumentsTerms(
          new RoaringDocIdSet.Builder(3).add(0).add(2).build());
      Assert.assertEquals("Number of terms mismatch.", 1L,
          terms.filter(t -> "document1".equals(t.utf8ToString())).count());

      terms = idp.getDocumentsTerms(
          new RoaringDocIdSet.Builder(3).add(0).add(2).build());
      Assert.assertEquals("Number of terms mismatch.", 1L,
          terms.filter(t -> "document3".equals(t.utf8ToString())).count());
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetDocumentCount()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      Assert.assertEquals("Unknown document found.",
          idx.docs, idp.getDocumentCount());
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetDocumentFields()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      Assert.assertEquals("Field list mismatch.",
          idx.flds.size(), idp.getDocumentFields().length);
      Assert.assertTrue("Field list mismatch.",
          idx.flds.containsAll(Arrays.asList(idp.getDocumentFields())));
    }
  }

  @Test
  public void testMetrics()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.TVECTORS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      final IndexDataProvider idp = new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
          .build();

      Assert.assertNotNull("Failed to get Metrics.", idp.metrics());
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
    TestMemIndex(final Index idxType)
        throws IOException {
      this.dir = new RAMDirectory();
      final IndexWriter wrtr = new IndexWriter(this.dir,
          new IndexWriterConfig(new WhiteSpaceAnalyzer()));
      switch (idxType) {
        case TVECTORS:
          wrtr.addDocuments(getTVIndexDocs());
          break;
        case NO_TVECTORS:
          wrtr.addDocuments(getNoTVIndexDocs());
          break;
      }
      wrtr.close();
    }

    Iterable<Document> getTVIndexDocs() {
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

    Iterable<Document> getNoTVIndexDocs() {
      this.flds = Arrays.asList("f1", "f2", "f3");

      final Collection<Document> docs = new ArrayList<>(1);

      final Document doc1 = new Document();
      doc1.add(new TextField("f1",
          "first field value document1 field1 document1field1", Store.NO));
      doc1.add(new TextField("f2",
          "second field value document1 field2 document1field2", Store.NO));
      doc1.add(new TextField("f3",
          "third field value document1 field3 document1field3", Store.NO));
      docs.add(doc1);

      this.docs = docs.size();
      return docs;
    }

    @Override
    public void close()
        throws Exception {
      this.dir.close();
    }

    enum Index {
      TVECTORS, NO_TVECTORS
    }

    private static final class WhiteSpaceAnalyzer
        extends Analyzer {

      WhiteSpaceAnalyzer() {
      }

      @Override
      protected TokenStreamComponents createComponents(final String fieldName) {
        return new TokenStreamComponents(new WhitespaceTokenizer());
      }
    }
  }
}