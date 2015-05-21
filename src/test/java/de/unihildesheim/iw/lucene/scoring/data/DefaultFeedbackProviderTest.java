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

package de.unihildesheim.iw.lucene.scoring.data;

import de.unihildesheim.iw.util.Buildable;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultFeedbackProviderTest
    .TestMemIndex.IndexType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test for {@link DefaultFeedbackProvider}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class DefaultFeedbackProviderTest
    extends TestCase {
  public DefaultFeedbackProviderTest() {
    super(LoggerFactory.getLogger(DefaultFeedbackProviderTest.class));
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
  public void testGet_fixedAmount_all()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(IndexType.FULL)) {
      final DefaultFeedbackProvider dfp = new DefaultFeedbackProvider();
      dfp.indexReader(idx.getReader())
          .analyzer(new WhitespaceAnalyzer())
          .query("value")
          .dataProvider(idx.getIdp())
          .amount(idx.docs << 1);
      final DocIdSet fb = dfp.get();

      Assert.assertEquals("Amount of feedback mismatch.",
          idx.docs, getNumDocsFromSet(fb));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGet_fixedAmount_all_withField()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(IndexType.SPARE)) {
      final DefaultFeedbackProvider dfp = new DefaultFeedbackProvider();
      dfp.indexReader(idx.getReader())
          .analyzer(new WhitespaceAnalyzer())
          .query("document1")
          .fields("f1")
          .dataProvider(idx.getIdp())
          .amount(1, idx.docs << 1);
      final DocIdSet fb = dfp.get();

      Assert.assertEquals("Amount of feedback mismatch.",
          1L, getNumDocsFromSet(fb));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGet_fixedAmount_single()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(IndexType.FULL)) {
      final DefaultFeedbackProvider dfp = new DefaultFeedbackProvider();
      dfp.indexReader(idx.getReader())
          .analyzer(new WhitespaceAnalyzer())
          .query("value")
          .dataProvider(idx.getIdp())
          .amount(1);
      final DocIdSet fb = dfp.get();

      Assert.assertEquals("Amount of feedback mismatch.",
          1L, getNumDocsFromSet(fb));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGet_fixedAmount_zero()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(IndexType.FULL)) {
      final DefaultFeedbackProvider dfp = new DefaultFeedbackProvider();
      dfp.indexReader(idx.getReader())
          .analyzer(new WhitespaceAnalyzer())
          .query("value")
          .dataProvider(idx.getIdp())
          .amount(0);

      try {
        dfp.get();
        Assert.fail("Expected an IllegalArgumentException to be thrown.");
      } catch (final IllegalArgumentException e) {
        // pass
      }
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGet_minMax_all()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(IndexType.FULL)) {
      final DefaultFeedbackProvider dfp = new DefaultFeedbackProvider();
      dfp.indexReader(idx.getReader())
          .analyzer(new WhitespaceAnalyzer())
          .query("value")
          .amount(1, idx.docs << 1);
      final DocIdSet fb = dfp.get();

      Assert.assertEquals("Amount of feedback mismatch.",
          idx.docs, getNumDocsFromSet(fb));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGet_minMax_zero()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(IndexType.FULL)) {
      final DefaultFeedbackProvider dfp = new DefaultFeedbackProvider();
      dfp.indexReader(idx.getReader())
          .analyzer(new WhitespaceAnalyzer())
          .query("value");

      dfp.amount(0, 0);
      try {
        dfp.get();
        Assert.fail("Expected an IllegalArgumentException to be thrown.");
      } catch (final IllegalArgumentException e) {
        // pass
      }

      dfp.amount(1, 0);
      try {
        dfp.get();
        Assert.fail("Expected an IllegalArgumentException to be thrown.");
      } catch (final IllegalArgumentException e) {
        // pass
      }

      dfp.amount(0, 1);
      try {
        dfp.get();
        Assert.fail("Expected an IllegalArgumentException to be thrown.");
      } catch (final IllegalArgumentException e) {
        // pass
      }

      dfp.dataProvider(idx.getIdp())
          .amount(0);
      try {
        dfp.get();
        Assert.fail("Expected an IllegalArgumentException to be thrown.");
      } catch (final IllegalArgumentException e) {
        // pass
      }
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGet_minMax_single()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(IndexType.FULL)) {
      final DefaultFeedbackProvider dfp = new DefaultFeedbackProvider();
      dfp.indexReader(idx.getReader())
          .analyzer(new WhitespaceAnalyzer())
          .query("value")
          .amount(1, 1);
      final DocIdSet fb = dfp.get();

      Assert.assertEquals("Amount of feedback mismatch.",
          1L, getNumDocsFromSet(fb));
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
    TestMemIndex(final IndexType idxType)
        throws IOException {
      this.dir = new RAMDirectory();
      final IndexWriter wrtr = new IndexWriter(this.dir,
          new IndexWriterConfig(
              new org.apache.lucene.analysis.core.WhitespaceAnalyzer()));
      if (idxType == IndexType.FULL) {
        wrtr.addDocuments(getIndexDocs());
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
      doc3.add(new VecTextField("f1",
          "first field value document3 field1 document3field1", Store.NO));
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