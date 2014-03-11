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

import de.unihildesheim.lucene.LuceneDefaults;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing index that keeps all data temporary on disk and provides utility
 * functions to test data retrieval.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TempDiskIndex {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          TempDiskIndex.class);

  /**
   * Lucene index.
   */
  private final Directory index;

  /**
   * Fields available on this index.
   */
  private final String[] idxFields;

//  private final StandardAnalyzer analyzer;
//  private final IndexWriterConfig config;
  private final IndexWriter writer;

  private Thread shutdowHandler = new Thread(new Runnable() {
    @Override
    public void run() {
      try {
        writer.close();
      } catch (IOException ex) {
        LOG.error("Caught exception while closing index writer.", ex);
      }
      try {
        index.close();
      } catch (IOException ex) {
        LOG.error("Caught exception while closing index.", ex);
      }
    }
  }, "CachedIndexDataProvider_shutdownHandler");

  /**
   * Create a new temporary index.
   *
   * @param fields Per-document fields to generate
   * @param documents Documents to add
   * @throws IOException Thrown on low-level I/O-errors
   */
  public TempDiskIndex(final String[] fields,
          final Collection<String[]> documents) throws IOException {
    this(fields);
    addDocs(documents);
  }

  /**
   * Create a new empty temporary index.
   *
   * @param fields Per-document fields to generate
   * @throws IOException Thrown on low-level I/O-errors
   */
  public TempDiskIndex(final String[] fields) throws IOException {
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
            CharArraySet.EMPTY_SET);
    final IndexWriterConfig config = new IndexWriterConfig(
            LuceneDefaults.VERSION, analyzer);

    final Path tmpPath = Files.createTempDirectory("idx" + Long.toString(
            System.nanoTime()));
    this.index = FSDirectory.open(tmpPath.toFile());

    this.writer = new IndexWriter(this.index, config);

    this.idxFields = fields.clone();

    try {
      Runtime.getRuntime().addShutdownHook(this.shutdowHandler);
    } catch (IllegalArgumentException ex) {
      // already registered, or shutdown is currently happening
    }
  }

  /**
   * Get a {@link IndexReader} for this index.
   *
   * @return Reader for this index
   * @throws IOException Thrown on low-level I/O errors
   */
  public IndexReader getReader() throws IOException {
    return DirectoryReader.open(this.index);
  }

  public void flush() throws IOException {
    this.writer.commit();
  }

  /**
   * Add a document to the index
   *
   * @param content Document content. The number of fields must match the
   * initial specified fields list.
   * @throws IOException Thrown on low-level I/O errors
   */
  public void addDoc(final String[] content) throws IOException {
    addDoc(this.writer, content);
  }

  /**
   * Create the simple in-memory test index.
   *
   * @param documents Documents to add to the index
   * @throws IOException Thrown on low-level I/O errors
   */
  public void addDocs(final Collection<String[]> documents) throws IOException {
    // index documents
    for (String[] doc : documents) {
      addDoc(this.writer, doc);
    }
    LOG.info("Added {} documents to index", documents.size());
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
              Field.Store.NO));
    }
    writer.addDocument(doc);
  }
}
