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

package de.unihildesheim.iw.lucene.query;

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.VecTextField;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
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
 * Test for {@link TryExactTermsQuery}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class TryExactTermsQueryTest
    extends TestCase {
  public TryExactTermsQueryTest() {
    super(LoggerFactory.getLogger(TryExactTermsQueryTest.class));
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testRelax()
      throws Exception {
    @SuppressWarnings("TypeMayBeWeakened")
    final TryExactTermsQuery teq= new TryExactTermsQuery(
        new WhiteSpaceAnalyzer(),
        "foo bar baz bam",
        "foo");

    for (int i = 4; i > 0; i--) {
      Assert.assertEquals("Number of terms mismatch.",
          i, teq.getQueryObj().getMinimumNumberShouldMatch());
      teq.relax();
    }
  }

  @Test
  public void testGetQueryObj()
      throws Exception {
    final BooleanQuery bq= new TryExactTermsQuery(
        new WhiteSpaceAnalyzer(),
        "foo bar baz bam",
        "foo").getQueryObj();
    Assert.assertNotNull("Query object was null.", bq);
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetQueryTerms()
      throws Exception {
    final Collection<String> qt = new TryExactTermsQuery(
        new WhiteSpaceAnalyzer(),
        "foo bar baz bam",
        "foo").getQueryTerms();
    Assert.assertEquals("Query terms mismatch.", 4L, qt.size());
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void test_usage()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final RelaxableQuery teq = new TryExactTermsQuery(
          new WhiteSpaceAnalyzer(),
          "field value document1 field1",
          "f1");
      final IndexSearcher src = new IndexSearcher(idx.getReader());

      TopDocs results;

      results = src.search(teq.getQueryObj(), 3);
      Assert.assertEquals("Expected results mismatch.",
          1L, results.totalHits);

      teq.relax();
      results = src.search(teq.getQueryObj(), 3);
      Assert.assertEquals("Expected results mismatch.",
          3L, results.totalHits);
    }
  }

  /**
   * Simple {@link Analyzer} splitting terms at whitespaces.
   */
  private static final class WhiteSpaceAnalyzer
      extends Analyzer {

    WhiteSpaceAnalyzer() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
      return new TokenStreamComponents(new WhitespaceTokenizer());
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
          new IndexWriterConfig(new WhiteSpaceAnalyzer()));
      wrtr.addDocuments(getIndexDocs());
      wrtr.close();
    }

    DirectoryReader getReader()
        throws IOException {
      return DirectoryReader.open(this.dir);
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
}