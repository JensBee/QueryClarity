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
package de.unihildesheim.iw.lucene.util;

import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.lucene.VecTextField;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;

/**
 * Testing index that keeps all data temporary on disk and provides utility
 * functions to test data retrieval.
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
  final Directory index;
  /**
   * Writer for the temporary index.
   */
  final IndexWriter writer;
  /**
   * File directory of the index.
   */
  private final String indexDir;
  /**
   * Fields available on this index.
   */
  private final String[] idxFields;

  /**
   * Create a new temporary index.
   *
   * @param fields Per-document fields to generate
   * @param documents Documents to add
   * @throws IOException Thrown on low-level I/O-errors
   */
  public TempDiskIndex(final String[] fields,
      final Collection<String[]> documents)
      throws IOException {
    this(Objects.requireNonNull(fields, "Fields were null."));
    if (Objects.requireNonNull(documents, "Documents were null.").isEmpty()) {
      throw new IllegalArgumentException("Documents were empty.");
    }
    addDocs(documents);
  }

  /**
   * Create a new empty temporary index.
   *
   * @param fields Per-document fields to generate
   * @throws IOException Thrown on low-level I/O-errors
   */
  public TempDiskIndex(final String[] fields)
      throws IOException {
    if (Objects.requireNonNull(fields, "Fields were null.").length == 0) {
      throw new IllegalArgumentException("Fields were empty.");
    }
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
        CharArraySet.EMPTY_SET);
    final IndexWriterConfig config = new IndexWriterConfig(
        LuceneDefaults.VERSION, analyzer);

    final Path tmpPath = Files.createTempDirectory("idx" + Long.toString(
        System.nanoTime()));
    this.indexDir = tmpPath.toString();
    this.index = FSDirectory.open(tmpPath.toFile());
    this.writer = new IndexWriter(this.index, config);
    this.idxFields = fields.clone();

    try {
      // shutdown handler closing the index
      final Thread shutdownHandler = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            writer.close();
          } catch (IOException ex) {
            LOG.error("Caught exception while closing index writer.",
                ex);
          }
          try {
            index.close();
          } catch (IOException ex) {
            LOG.error("Caught exception while closing index.", ex);
          }
        }
      }, TempDiskIndex.class.getSimpleName() + "_shutdownHandler");
      Runtime.getRuntime().addShutdownHook(shutdownHandler);
    } catch (IllegalArgumentException ex) {
      // already registered, or shutdown is currently happening
    }
  }

  /**
   * Create the simple in-memory test index.
   *
   * @param documents Documents to add to the index
   * @throws IOException Thrown on low-level I/O errors
   */
  public void addDocs(final Collection<String[]> documents)
      throws IOException {
    if (Objects.requireNonNull(documents, "Documents were null.").isEmpty()) {
      throw new IllegalArgumentException("Empty documents list.");
    }
    // index documents
    for (final String[] doc : documents) {
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
  @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
  private void addDoc(final IndexWriter writer, final String[] content)
      throws IOException {
    final Document doc = new Document();
    for (int i = 0; i < content.length; i++) {
      doc.add(new VecTextField(this.idxFields[i], content[i],
          Field.Store.NO));
    }
    writer.addDocument(doc);
  }

  /**
   * Get the file directory where the temporary index resides in.
   *
   * @return Index directory path as string
   */
  public String getIndexDir() {
    return this.indexDir;
  }

  /**
   * Get a {@link IndexReader} for this index.
   *
   * @return Reader for this index
   * @throws IOException Thrown on low-level I/O errors
   */
  public IndexReader getReader()
      throws IOException {
    return DirectoryReader.open(this.index);
  }

  /**
   * Commit all pending data.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  public void flush()
      throws IOException {
    this.writer.commit();
  }

  /**
   * Add a document to the index
   *
   * @param content Document content. The number of fields must match the
   * initial specified fields list.
   * @throws IOException Thrown on low-level I/O errors
   */
  public void addDoc(final String[] content)
      throws IOException {
    if (Objects.requireNonNull(content, "Content was null.").length == 0) {
      throw new IllegalArgumentException("Empty content.");
    }
    addDoc(this.writer, content);
  }
}
