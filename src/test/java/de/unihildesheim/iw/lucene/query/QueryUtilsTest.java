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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.lucene.index.CollectionMetrics;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
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
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefIterator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Test for {@link QueryUtils}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class QueryUtilsTest
    extends TestCase {
  public QueryUtilsTest() {
    super(LoggerFactory.getLogger(QueryUtilsTest.class));
  }

  @SuppressWarnings("resource")
  private static final Analyzer ANALYZER = new WhiteSpaceAnalyzer();

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testTokenizeQuery_noMetrics()
      throws Exception {
    final BytesRefArray bra = QueryUtils.tokenizeQuery(
        "foo bar baz", ANALYZER, null);

    Assert.assertEquals("Extracted terms count mismatch.", 3L, bra.size());

    final BytesRefIterator braIt = bra.iterator();
    BytesRef term;
    while ((term = braIt.next()) != null) {
      final String termStr = term.utf8ToString();
      switch (termStr) {
        case "foo":
        case "bar":
        case "baz":
          break;
        default:
          Assert.fail("Unknown term found.");
          break;
      }
    }
  }

  /**
   * Test tokenizing with skipping terms not present in index.
   *
   * @throws Exception
   */
  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testTokenizeQuery()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final IndexDataProvider idp = idx.getIdp();
      final CollectionMetrics m = idp.metrics();

      final BytesRefArray bra = QueryUtils.tokenizeQuery(
          "foo bar field baz value", ANALYZER, m);

      Assert.assertEquals("Extracted terms count mismatch.", 2L, bra.size());

      final BytesRefIterator braIt = bra.iterator();
      BytesRef term;
      while ((term = braIt.next()) != null) {
        final String termStr = term.utf8ToString();
        switch (termStr) {
          case "foo":
          case "bar":
          case "baz":
            Assert.fail("Non-index term found.");
            break;
          case "value":
          case "field":
            // pass
            break;
          default:
            Assert.fail("Unknown term found.");
            break;
        }
      }
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testTokenizeQueryString_noMetrics()
      throws Exception {
    final List<String> result = QueryUtils.tokenizeQueryString(
        "foo bar baz", ANALYZER, null);

    Assert.assertEquals("Extracted terms count mismatch.", 3L, result.size());

    for (final String s : result) {
      switch (s) {
        case "foo":
        case "bar":
        case "baz":
          break;
        default:
          Assert.fail("Unknown term found.");
          break;
      }
    }
  }

  /**
   * Test tokenizing with skipping terms not present in index.
   *
   * @throws Exception
   */
  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testTokenizeQueryString()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final IndexDataProvider idp = idx.getIdp();
      final CollectionMetrics m = idp.metrics();

      final List<String> result = QueryUtils.tokenizeQueryString(
          "foo bar field baz value", ANALYZER, m);

      Assert.assertEquals("Extracted terms count mismatch.", 2L, result.size());

      for (final String s : result) {
        switch (s) {
          case "foo":
          case "bar":
          case "baz":
            Assert.fail("Non-index term found.");
            break;
          case "value":
          case "field":
            // pass
            break;
          default:
            Assert.fail("Unknown term found.");
            break;
        }
      }
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testTokenizeAndMapQuery_noMetrics()
      throws Exception {
    final Map<BytesRef, Integer> map = QueryUtils.tokenizeAndMapQuery(
        "foo bar baz foo bar bar foo bar bam", ANALYZER, null);

    Assert.assertEquals("Extracted terms count mismatch.", 4L, map.size());

    for (final Entry<BytesRef, Integer> e : map.entrySet()) {
      switch (e.getKey().utf8ToString()) {
        case "foo":
          Assert.assertEquals("Term count mismatch.", 3L, (long) e.getValue());
          break;
        case "bam":
          Assert.assertEquals("Term count mismatch.", 1L, (long) e.getValue());
          break;
        case "bar":
          Assert.assertEquals("Term count mismatch.", 4L, (long) e.getValue());
          break;
        case "baz":
          Assert.assertEquals("Term count mismatch.", 1L, (long) e.getValue());
          break;
        default:
          Assert.fail("Unknown term found.");
          break;
      }
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testTokenizeAndMapQuery()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final IndexDataProvider idp = idx.getIdp();
      final CollectionMetrics m = idp.metrics();

      final Map<BytesRef, Integer> map = QueryUtils.tokenizeAndMapQuery(
          "foo field bar baz value foo bar bar value foo bar bam", ANALYZER, m);

      Assert.assertEquals("Extracted terms count mismatch.", 2L, map.size());

      for (final Entry<BytesRef, Integer> e : map.entrySet()) {
        switch (e.getKey().utf8ToString()) {
          case "field":
            Assert
                .assertEquals("Term count mismatch.", 1L, (long) e.getValue());
            break;
          case "value":
            Assert
                .assertEquals("Term count mismatch.", 2L, (long) e.getValue());
            break;
          default:
            Assert.fail("Unknown term found.");
            break;
        }
      }
    }
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
}