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

package de.unihildesheim.iw.lucene.document;

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.lucene.document.FeedbackQueryTest.TestIndex
    .IndexType;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.RelaxableQuery;
import de.unihildesheim.iw.lucene.query.TryExactTermsQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.RoaringDocIdSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test for {@link FeedbackQuery}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class FeedbackQueryTest
    extends TestCase {
  public FeedbackQueryTest() {
    super(LoggerFactory.getLogger(FeedbackQueryTest.class));
  }

  private static int getNumDocsFromSet(final DocIdSet set)
      throws IOException {
    final DocIdSetIterator disi = set.iterator();
    int count = 0;
    if (disi != null) {
      while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        count++;
      }
    }
    return count;
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetMinMax()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.FULL)) {
      final RelaxableQuery rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "field", "f1");
      final IndexSearcher src = new IndexSearcher(idx.getReader());

      DocIdSet result;

      result = FeedbackQuery.getMinMax(src, rq, 1, idx.docs);
      Assert.assertEquals("Number of documents retrieved do not match.",
          idx.docs, getNumDocsFromSet(result));

      result = FeedbackQuery.getMinMax(src, rq, 1, idx.docs << 1);
      Assert.assertEquals("Number of documents retrieved do not match.",
          idx.docs, getNumDocsFromSet(result));

      result = FeedbackQuery.getMinMax(src, rq, 1, 1);
      Assert.assertEquals("Number of documents retrieved do not match.",
          1L, getNumDocsFromSet(result));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetMinMax_illegal()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.FULL)) {
      final RelaxableQuery rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "field", "f1");
      final IndexSearcher src = new IndexSearcher(idx.getReader());

      // zero is too low
      try {
        FeedbackQuery.getMinMax(src, rq, 0, 0);
        Assert.fail("Expected an IllegalArgumentException to be thrown.");
      } catch (final IllegalArgumentException e) {
        // pass
      }

      // min > max
      try {
        FeedbackQuery.getMinMax(src, rq, 10, 1);
        Assert.fail("Expected an IllegalArgumentException to be thrown.");
      } catch (final IllegalArgumentException e) {
        // pass
      }
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetMinMax_maxAllDocs()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.FULL)) {
      final RelaxableQuery rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "field", "f1");
      final IndexSearcher src = new IndexSearcher(idx.getReader());

      // -1 for max means all possible matches
      final DocIdSet result = FeedbackQuery.getMinMax(src, rq, 1, -1);
      Assert.assertEquals("Number of documents retrieved do not match.",
          3L, getNumDocsFromSet(result));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetFixed()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.FULL)) {
      final RelaxableQuery rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "field", "f1");
      final IndexSearcher src = new IndexSearcher(idx.getReader());

      DocIdSet result;

      result = FeedbackQuery.getFixed(src, idx.getIdp(), rq, idx.docs);
      Assert.assertEquals("Number of documents retrieved do not match.",
          3L, getNumDocsFromSet(result));

      result = FeedbackQuery.getFixed(src, idx.getIdp(), rq, idx.docs << 1);
      Assert.assertEquals("Number of documents retrieved do not match.",
          3L, getNumDocsFromSet(result));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetFixed_withField()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.SPARE)) {
      final RelaxableQuery rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "field", "f1");
      final IndexSearcher src = new IndexSearcher(idx.getReader());

      DocIdSet result;

      result = FeedbackQuery.getFixed(src, idx.getIdp(), rq, idx.docs, "f1");
      Assert.assertEquals("Number of documents retrieved do not match.",
          2L, getNumDocsFromSet(result));

      result = FeedbackQuery
          .getFixed(src, idx.getIdp(), rq, idx.docs << 1, "f1");
      Assert.assertEquals("Number of documents retrieved do not match.",
          2L, getNumDocsFromSet(result));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetFixed_useRandom()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.FULL)) {
      final RelaxableQuery rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "document1", "f1");
      final IndexSearcher src = new IndexSearcher(idx.getReader());

      DocIdSet result;

      result = FeedbackQuery.getFixed(src, idx.getIdp(), rq, idx.docs);
      Assert.assertEquals("Number of documents retrieved do not match.",
          3L, getNumDocsFromSet(result));

      result = FeedbackQuery.getFixed(src, idx.getIdp(), rq, idx.docs << 1);
      Assert.assertEquals("Number of documents retrieved do not match.",
          3L, getNumDocsFromSet(result));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetRandom()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.FULL)) {
      @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
      final DocIdSet dis = new RoaringDocIdSet.Builder(3)
          .add(1).build();

      final DocIdSet result = FeedbackQuery.getRandom(
          idx.getIdp(), idx.docs, dis);
      Assert.assertEquals("Number of documents retrieved do not match.",
          3L, getNumDocsFromSet(result));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetRandom_withFields()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.SPARE)) {
      @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
      final DocIdSet dis = new RoaringDocIdSet.Builder(3)
          .add(1).build();

      final DocIdSet result = FeedbackQuery.getRandom(
          idx.getIdp(), idx.docs, dis, "f3");
      Assert.assertEquals("Number of documents retrieved do not match.",
          2L, getNumDocsFromSet(result));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetMaxDocs()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.FULL)) {
      int max;

      max = FeedbackQuery.getMaxDocs(idx.getReader(), 10);
      Assert.assertEquals("MaxDoc value differs.", 3L, max);

      max = FeedbackQuery.getMaxDocs(idx.getReader(), 1);
      Assert.assertEquals("MaxDoc value differs.", 1L, max);

      max = FeedbackQuery.getMaxDocs(idx.getReader(), 3);
      Assert.assertEquals("MaxDoc value differs.", 3L, max);
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetDocs()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.FULL)) {
      final IndexSearcher src = new IndexSearcher(idx.getReader());

      Query rq;
      int[] docs;

      rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "document1", "f1").getQueryObj();
      docs = FeedbackQuery.getDocs(src, rq, 10);
      Assert.assertEquals("Number of docs returned differs.", 1L, docs.length);

      rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "field", "f1").getQueryObj();
      docs = FeedbackQuery.getDocs(src, rq, 10);
      Assert.assertEquals("Number of docs returned differs.", 3L, docs.length);

      rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "foo", "f1").getQueryObj();
      docs = FeedbackQuery.getDocs(src, rq, 10);
      Assert.assertEquals("Number of docs returned differs.", 0L, docs.length);
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetDocs_illegalField()
      throws Exception {
    try (TestIndex idx = new TestIndex(IndexType.FULL)) {
      final IndexSearcher src = new IndexSearcher(idx.getReader());

      final Query rq = new TryExactTermsQuery(
          new WhitespaceAnalyzer(), "document1", "foo").getQueryObj();
      int[] docs = FeedbackQuery.getDocs(src, rq, 10);
      Assert.assertEquals("Number of docs returned differs.", 0L, docs.length);
    }
  }

  /**
   * Simple static memory index for testing.
   *
   * @author Jens Bertram (code@jens-bertram.net)
   */
  @SuppressWarnings("JavaDoc")
  static final class TestIndex
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
    TestIndex(final IndexType idxType)
        throws IOException {
      this.dir = new RAMDirectory();
      final IndexWriter wrtr = new IndexWriter(this.dir,
          new IndexWriterConfig(new WhitespaceAnalyzer()));
      if (idxType == IndexType.FULL) {
        wrtr.addDocuments(getTVIndexDocs());
      } else {
        wrtr.addDocuments(getSpareIndexDocs());
      }
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
      return new FDRIndexDataProvider.Builder()
          .indexReader(
              new FilteredDirectoryReader.Builder(getReader())
                  .fields(this.flds)
                  .build())
          .build();
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

    Iterable<Document> getSpareIndexDocs() {
      this.flds = Arrays.asList("f1", "f2", "f3");

      final Collection<Document> docs = new ArrayList<>(3);

      final Document doc1 = new Document();
      doc1.add(new VecTextField("f1",
          "first field value document1 field1 document1field1", Store.NO));
      doc1.add(new VecTextField("f3",
          "third field value document1 field3 document1field3", Store.NO));
      docs.add(doc1);

      final Document doc2 = new Document();
      doc2.add(new VecTextField("f1",
          "first field value document2 field1 document2field1", Store.NO));
      doc2.add(new VecTextField("f3",
          "third field value document2 field3 document2field3", Store.NO));
      docs.add(doc2);

      final Document doc3 = new Document();
      doc3.add(new VecTextField("f2",
          "second field value document3 field2 document3field2", Store.NO));
      docs.add(doc3);

      this.docs = docs.size();
      return docs;
    }

    enum IndexType {
      FULL, SPARE
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