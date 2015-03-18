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

import de.unihildesheim.iw.Buildable.BuildException;
import de.unihildesheim.iw.Buildable.ConfigurationException;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test for {@link CollectionMetrics}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class CollectionMetricsTest
    extends TestCase {
  public CollectionMetricsTest() {
    super(LoggerFactory.getLogger(CollectionMetricsTest.class));
  }

  @Test
  public void testTf()
      throws Exception {
    try (TestFDRIDPInstance idx = new TestFDRIDPInstance()) {
      final IndexDataProvider idp = idx.getIdp();
      final CollectionMetrics m = idp.metrics();

      Assert.assertEquals("TF differs.", 9L, m.tf(new BytesRef("value")));
    }
  }

  @Test
  public void testRelTf()
      throws Exception {
    try (TestFDRIDPInstance idx = new TestFDRIDPInstance()) {
      final IndexDataProvider idp = idx.getIdp();
      final CollectionMetrics m = idp.metrics();

      Assert.assertEquals("Rel-TF differs.",
          3d / 54d, m.relTf(new BytesRef("document1")), 0d);
    }
  }

  @Test
  public void testDf()
      throws Exception {
    try (TestFDRIDPInstance idx = new TestFDRIDPInstance()) {
      final IndexDataProvider idp = idx.getIdp();
      final CollectionMetrics m = idp.metrics();

      Assert.assertEquals("Document frequency mismatch.",
          3L, (long) m.df(new BytesRef("value")));
      Assert.assertEquals("Document frequency mismatch.",
          1L, (long) m.df(new BytesRef("document1")));
      Assert.assertEquals("Document frequency mismatch.",
          1L, (long) m.df(new BytesRef("document2")));
      Assert.assertEquals("Document frequency mismatch.",
          1L, (long) m.df(new BytesRef("document3")));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testRelDf()
      throws Exception {
    try (TestFDRIDPInstance idx = new TestFDRIDPInstance()) {
      final IndexDataProvider idp = idx.getIdp();
      final CollectionMetrics m = idp.metrics();

      Assert.assertEquals("Relative document frequency mismatch.",
          3d / (double) idx.docs, m.relDf(new BytesRef("value")), 0d);
      Assert.assertEquals("Relative document frequency mismatch.",
          1d / (double) idx.docs, m.relDf(new BytesRef("document1")), 0d);
      Assert.assertEquals("Relative document frequency mismatch.",
          1d / (double) idx.docs, m.relDf(new BytesRef("document2")), 0d);
      Assert.assertEquals("Relative document frequency mismatch.",
          1d / (double) idx.docs, m.relDf(new BytesRef("document3")), 0d);
    }
  }

  @SuppressWarnings({"ImplicitNumericConversion", "ObjectAllocationInLoop"})
  @Test
  public void testDocData()
      throws Exception {
    try (TestFDRIDPInstance idx = new TestFDRIDPInstance()) {
      final IndexDataProvider idp = idx.getIdp();
      final CollectionMetrics m = idp.metrics();

      for (int i = 0; i < idx.docs; i++) {
        final DocumentModel dm = m.docData(i);
        Assert.assertEquals("Document id mismatch.", i, dm.id);
        Assert.assertEquals("Term frequency test failed.",
            3, dm.tf(new BytesRef("document" + (i + 1))));
      }
    }
  }

  /**
   * Simple static memory index for testing.
   *
   * @author Jens Bertram (code@jens-bertram.net)
   */
  @SuppressWarnings("JavaDoc")
  static final class TestFDRIDPInstance
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
    TestFDRIDPInstance()
        throws IOException {
      this.dir = new RAMDirectory();
      final IndexWriter wrtr = new IndexWriter(this.dir,
          new IndexWriterConfig(new WhitespaceAnalyzer()));
      wrtr.addDocuments(getTVIndexDocs());
      wrtr.close();
    }

    @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
    IndexDataProvider getIdp()
        throws IOException, ConfigurationException, BuildException {
      final DirectoryReader reader = DirectoryReader.open(this.dir);
      final FilteredDirectoryReader idxReader =
          new FilteredDirectoryReader.Builder(reader).build();
      return new FDRIndexDataProvider.Builder()
          .indexReader(idxReader)
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

    @Override
    public void close()
        throws Exception {
      this.dir.close();
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
}