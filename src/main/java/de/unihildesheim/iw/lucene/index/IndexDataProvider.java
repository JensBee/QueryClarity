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
package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.util.concurrent.processing.Source;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * IndexDataProvider provides statistical data from the underlying Lucene
 * index.
 * <p/>
 * Calculated values may be cached. So any call to those functions may not
 * trigger a recalculation of the values. If this is not desired, then needed
 * update functions must be provided by the implementing class.
 * <p/>
 * Also, any restriction to a subset of index fields must be applied by the
 * implementing class as they are no enforced.
 */
public interface IndexDataProvider {

  /**
   * Get the frequency of all terms in the index.
   *
   * @return The frequency of all terms in the index
   */
  long getTermFrequency();

  /**
   * Instructs the data provider to pre-fill caches, etc.
   *
   * @throws Exception Thrown, if any warm-up method fails
   */
  void warmUp()
      throws Exception;

  /**
   * Get the term frequency of a single term in the index.
   *
   * @param term Term to lookup
   * @return The frequency of the term in the index, or <tt>null</tt> if none
   * was stored
   */
  Long getTermFrequency(final ByteArray term);

  /**
   * Get the document frequency of a single term in the index.
   *
   * @param term Term to lookup
   * @return The frequency of the term in the index
   */
  int getDocumentFrequency(final ByteArray term);

  /**
   * Get the relative term frequency for a term in the index.
   *
   * @param term Term to lookup
   * @return Relative term frequency for the given term
   */
  double getRelativeTermFrequency(final ByteArray term);

  /**
   * Close this instance. This is meant for handling cleanups after using this
   * instance. The behavior of functions called after this is undefined.
   */
  void dispose();

  /**
   * Get an {@link Iterator} over a unique set of all terms from the index.
   *
   * @return Unique terms iterator
   */
  Iterator<ByteArray> getTermsIterator();

  /**
   * Get a {@link Source} providing all known terms.
   *
   * @return {@link Source} providing all known terms
   */
  Source<ByteArray> getTermsSource();

  /**
   * Get an iterator over all known document-ids.
   *
   * @return Iterator over document-ids
   */
  Iterator<Integer> getDocumentIdIterator();

  /**
   * Get a {@link Source} providing all known document ids.
   *
   * @return {@link Source} providing all known document ids
   */
  Source<Integer> getDocumentIdSource();

  /**
   * Get the number of unique terms in the index.
   *
   * @return Number of unique terms in the index
   */
  long getUniqueTermsCount();

  /**
   * Get a {@link DocumentModel} instance for the document with the given id.
   *
   * @param docId Lucene document-id
   * @return Document-model associated with the given Lucene document-id
   */
  DocumentModel getDocumentModel(final int docId);

  /**
   * Test if a document (model) for the specific document-id is known.
   *
   * @param docId Document-id to lookup
   * @return True if a model is known, false otherwise
   */
  boolean hasDocument(final Integer docId);

  /**
   * Get a unique set of terms for all documents identified by their id.
   *
   * @param docIds List of document ids to extract terms from
   * @return List of terms from all documents
   * @throws IOException Thrown on low-level I/O errors
   */
  Collection<ByteArray> getDocumentsTermSet(final Collection<Integer> docIds)
      throws IOException;

  /**
   * Get the number of all Documents (models) known to this instance.
   *
   * @return Number of Documents known
   */
  long getDocumentCount();

  /**
   * Check if a document contains the given term.
   *
   * @param documentId Id of the document to check
   * @param term Term to lookup
   * @return True, if it contains the term, false otherwise
   */
  boolean documentContains(final int documentId, final ByteArray term);

  /**
   * Get the last commit generation id of the Lucene index.
   *
   * @return Commit generation id
   */
  long getLastIndexCommitGeneration();

  /**
   * Get the list of currently visible document fields.
   *
   * @return List of document field names
   */
  Set<String> getDocumentFields();

  /**
   * Get the list of stopwords currently in use.
   *
   * @return List of words to exclude
   */
  Set<String> getStopwords();

//  /**
//   * Get the {@link IndexReader} for accessing the Lucene index.
//   *
//   * @return Index reader pointing at the Lucene index
//   */
//  IndexReader getIndexReader()
//      throws IOException;

  /**
   * Flag indicating, if this instance is closed.
   *
   * @return True, if instance was disposed
   */
  boolean isDisposed();
}
