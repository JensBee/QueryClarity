/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net>
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
package de.unihildesheim.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing index that keeps all data in memory and provides utility functions to
 * test data retrieval.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class MemoryIndex {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(MemoryIndex.class);

  /**
   * Lucene index.
   */
  private final Directory index = new RAMDirectory();

  /**
   * internally used reader for the index.
   */
  private final IndexReader reader;

  /**
   * Fields available on this index.
   */
  private final String[] idxFields;

  /**
   * Create a new in-memory index.
   * @param fields Per-document fields to generate
   * @param documents Documents to add
   * @throws IOException Thrown on low-level I/O-errors
   */
  public MemoryIndex(final String[] fields, final List<String[]> documents)
          throws IOException {
    this.idxFields = fields.clone();
    createIndex(documents);
    this.reader = getReader();
  }

  /**
   * Get field names available in this index.
   *
   * @return Available field names
   */
  public final String[] getIdxFields() {
    return this.idxFields.clone();
  }

  /**
   * Get a {@link IndexReader} for this index.
   *
   * @return Reader for this index
   * @throws IOException Thrown on low-level I/O errors
   */
  public final IndexReader getReader() throws IOException {
    return DirectoryReader.open(index);
  }

  /**
   * Get the ids of all available docments.
   *
   * @return Ids of all available documents. Deleted documents are not included.
   */
  public final Collection<Integer> getDocumentIds() {
    final Bits liveDocs = MultiFields.getLiveDocs(this.reader); // NOPMD
    final Collection<Integer> ids = new ArrayList(this.reader.maxDoc());

    for (int docId = 0; docId < this.reader.maxDoc(); docId++) {
      // check if document is deleted
      if (liveDocs == null) {
        ids.add(docId);
      } else if (liveDocs.get(docId)) {
        ids.add(docId);
      }
    }
    return ids;
  }

  /**
   * Create the simple in-memory test index.
   *
   * @param documents Documents to add to the index
   * @throws IOException Thrown on low-level I/O errors
   */
  private void createIndex(final List<String[]> documents) throws
          IOException {
    final StandardAnalyzer analyzer = new StandardAnalyzer(
            Version.LUCENE_46, CharArraySet.EMPTY_SET);
    final IndexWriterConfig config
            = new IndexWriterConfig(Version.LUCENE_46, analyzer);

    // index documents
    int newIdx = 0;
    try (IndexWriter writer = new IndexWriter(index, config)) {
      for (String[] doc : documents) {
        LOG.info("Adding document docId={} content='{}'", newIdx++, doc);
        addDoc(writer, doc);
      }
      writer.close();
      LOG.info("Added {} documents to index", documents.size());
    }
  }

  /**
   * Add a document to the index.
   *
   * @param writer Index writer instance
   * @param content Content of the document
   * @throws IOException Thrown, if index could not be accessed
   */
  private void addDoc(final IndexWriter writer, final String[] content)
          throws IOException {
    Document doc = new Document();
    for (int i = 0; i < content.length; i++) {
      doc.add(new VecTextField(this.idxFields[i], content[i],
              Field.Store.YES));
    }
    writer.addDocument(doc);
  }
}
