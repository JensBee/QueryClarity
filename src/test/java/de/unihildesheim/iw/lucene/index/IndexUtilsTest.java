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
import de.unihildesheim.iw.lucene.search.FDRDefaultSimilarity;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Test for {@link IndexUtils}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class IndexUtilsTest
    extends TestCase {
  public IndexUtilsTest() {
    super(LoggerFactory.getLogger(IndexUtilsTest.class));
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetFields()
      throws Exception {
    try (TestIndex idx = new TestIndex()) {
      final IndexReader r = idx.getReader();

      final Collection<String> result = IndexUtils.getFields(r);

      Assert.assertEquals("Field count mismatch.",
          idx.flds.size(), result.size());

      Assert.assertTrue("Field content mismatch.",
          result.containsAll(idx.flds));
    }
  }

  @Test
  public void testCheckFields()
      throws Exception {
    try (TestIndex idx = new TestIndex()) {
      final IndexReader r = idx.getReader();

      IndexUtils.checkFields(r, new HashSet<>(idx.flds));

      try {
        IndexUtils.checkFields(r, Collections.singleton("foo"));
        Assert.fail("Unknown field found.");
      } catch (final IllegalStateException e) {
        // pass
      }
    }
  }

  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  @Test
  public void testGetSearcher()
      throws Exception {
    try (TestIndex idx = new TestIndex()) {
      final IndexReader r = idx.getReader();
      final FilteredDirectoryReader fdR =
          new FilteredDirectoryReader.Builder(idx.getReader()).build();

      final Similarity defaultSim = IndexUtils.getSearcher(r).getSimilarity();
      final Similarity fdrSim = IndexUtils.getSearcher(fdR).getSimilarity();

      Assert.assertTrue("Expected a DefaultSimilarity instance.",
          DefaultSimilarity.class.isInstance(defaultSim));

      Assert.assertTrue("Expected a FDRDefaultSimilarity instance.",
          FDRDefaultSimilarity.class.isInstance(fdrSim));
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
    TestIndex()
        throws IOException {
      this.dir = new RAMDirectory();
      final IndexWriter wrtr = new IndexWriter(this.dir,
          new IndexWriterConfig(new WhiteSpaceAnalyzer()));
      wrtr.addDocuments(getTVIndexDocs());
      wrtr.close();
    }

    DirectoryReader getReader()
        throws IOException {
      return DirectoryReader.open(this.dir);
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