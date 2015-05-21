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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DocIdSet;
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
 * Test for {@link DefaultVocabularyProvider}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class DefaultVocabularyProviderTest
    extends TestCase {
  public DefaultVocabularyProviderTest() {
    super(LoggerFactory.getLogger(DefaultVocabularyProviderTest.class));
  }

  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  @Test
  public void testGet()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final VocabularyProvider dvp = new DefaultVocabularyProvider();
      final DocIdSet dis = new RoaringDocIdSet.Builder(3)
          .add(0).add(1).add(2).build();
      dvp.indexDataProvider(idx.getIdp())
          .documentIds(dis);

      final long termCount = dvp.get().count();

      Assert.assertEquals("Number of terms mismatch.", 20L, termCount);
    }
  }

  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  @Test
  public void testGet_single()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex()) {
      final VocabularyProvider dvp = new DefaultVocabularyProvider();
      final DocIdSet dis = new RoaringDocIdSet.Builder(2).add(1).build();
      dvp.indexDataProvider(idx.getIdp())
          .documentIds(dis);

      final long termCount = dvp.get().count();

      Assert.assertEquals("Number of terms mismatch.", 12L, termCount);
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