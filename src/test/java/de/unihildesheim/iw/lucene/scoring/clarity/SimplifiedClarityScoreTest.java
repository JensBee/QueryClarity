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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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
 * Test for {@link SimplifiedClarityScore}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class SimplifiedClarityScoreTest
    extends TestCase {
  public SimplifiedClarityScoreTest() {
    super(LoggerFactory.getLogger(SimplifiedClarityScoreTest.class));
  }

  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  @Test
  public void testBuilder()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      new SimplifiedClarityScore.Builder()
          .analyzer(new WhitespaceAnalyzer())
          .indexDataProvider(idx.getIdp())
          .build();
    }
  }

  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  @Test
  public void testCalcScorePortion() throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final SimplifiedClarityScore dcs = new SimplifiedClarityScore.Builder()
          .analyzer(new WhitespaceAnalyzer())
          .indexDataProvider(idx.getIdp())
          .build();

      final BytesRef term = new BytesRef("document1");
      final long inQueryFreq = 3L;
      final long queryLength = 8L;
      final ClarityScoreCalculation.ScoreTupleHighPrecision result =
          dcs.calcScorePortion(term, inQueryFreq, queryLength);

      final double expectedQMod = (double) inQueryFreq / (double) queryLength;
      final double expectedCMod = idx.getIdp().getRelativeTermFrequency(term);

      Assert.assertEquals("Query model value differs.",
          expectedQMod, result.qModel.doubleValue(), 0.1e10);
      Assert.assertEquals("Collection model value differs.",
          expectedCMod, result.cModel.doubleValue(), 0.1e10);
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
          "first field value document1 field1 document1field1", Field.Store.NO));
      doc1.add(new VecTextField("f2",
          "second field value document1 field2 document1field2", Field.Store.NO));
      doc1.add(new VecTextField("f3",
          "third field value document1 field3 document1field3", Field.Store.NO));
      docs.add(doc1);

      final Document doc2 = new Document();
      doc2.add(new VecTextField("f1",
          "first field value document2 field1 document2field1", Field.Store.NO));
      doc2.add(new VecTextField("f2",
          "second field value document2 field2 document2field2", Field.Store.NO));
      doc2.add(new VecTextField("f3",
          "third field value document2 field3 document2field3", Field.Store.NO));
      docs.add(doc2);

      final Document doc3 = new Document();
      doc3.add(new VecTextField("f1",
          "first field value document3 field1 document3field1", Field.Store.NO));
      doc3.add(new VecTextField("f2",
          "second field value document3 field2 document3field2", Field.Store.NO));
      doc3.add(new VecTextField("f3",
          "third field value document3 field3 document3field3", Field.Store.NO));
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