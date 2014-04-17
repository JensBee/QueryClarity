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

import de.unihildesheim.ByteArray;
import de.unihildesheim.SupportsPersistence;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.util.concurrent.processing.Source;
import java.util.Collection;
import java.util.Iterator;

/**
 * IndexDataProvider provides statistical data from the underlying Lucene
 * index.
 *
 * Calculated values may be cached. So any call to those functions may not
 * trigger a recalculation of the values. If this is not desired, then needed
 * update functions must be provided by the implementing class.
 *
 * Also, any restriction to a subset of index fields must be applied by the
 * implementing class as they are no enforced.
 *
 *
 */
public interface IndexDataProvider extends SupportsPersistence {

  /**
   * Get the frequency of all terms in the index.
   *
   * @return The frequency of all terms in the index
   */
  long getTermFrequency();

  /**
   * Instructs the data provider to pre-fill caches, etc. Should be called,
   * after the {@link Environment} is set.
   */
  void warmUp();

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
   * Get a {@link ProcessPipe.Source} providing all known terms.
   *
   * @return {@link ProcessPipe.Source} providing all known terms
   */
  Source<ByteArray> getTermsSource();

  /**
   * Get an iterator over all known document-ids.
   *
   * @return Iterator over document-ids
   */
  Iterator<Integer> getDocumentIdIterator();

  /**
   * Get a {@link ProcessPipe.Source} providing all known document ids.
   *
   * @return {@link ProcessPipe.Source} providing all known document ids
   */
  Source<Integer> getDocumentIdSource();

  /**
   * Get the number of unique terms in the index.
   *
   * @return Number of unique terms in the index
   */
  long getUniqueTermsCount();

//  /**
//   * Tell the data provider, we want to access custom data specified by the
//   * given prefix.
//   * <p>
//   * A prefix must be registered before any call to
//   * <tt>setTermData()</tt> or <tt>getTermData()</tt> can be made.
//   *
//   * @param prefix Prefix name to register
//   */
//  void registerPrefix(final String prefix);
//
//  /**
//   * Store enhanced data for a document & term combination with a custom
//   * prefix.
//   * <tt>Null</tt> is not allowed for any parameter value.
//   *
//   * @param prefix Custom prefix, to identify where the data belongs to. Must
//   * not start with an underscore.
//   * @param documentId Document identifier
//   * @param term Term to which the data is associated.
//   * @param key Storage key.
//   * @param value Storage value
//   * @return Any previously set value or null, if there was none
//   */
//  Object setTermData(final String prefix, final int documentId,
//          final ByteArray term, final String key, final Object value);
//
//  /**
//   * Get enhanced data stored with a prefix (to distinguish data types) for a
//   * document-id and a term. The data is accessed via a key.
//   *
//   * @param prefix Prefix to stored data
//   * @param documentId Document-id the data is attached to
//   * @param term Term the data is attached to
//   * @param key Key to identify the data
//   * @return Data stored at the given location or <tt>null</tt> if there was
//   * none
//   */
//  Object getTermData(final String prefix, final int documentId,
//          final ByteArray term, final String key);
//
//  /**
//   * Get all term-data stored under a given prefix, document-id and key.
//   *
//   * @param prefix Prefix to stored data
//   * @param documentId Document-id the data is attached to
//   * @param key Key to identify the data
//   * @return Mapping of all stored data
//   */
//  Map<ByteArray, Object> getTermData(final String prefix,
//          final int documentId, final String key);
//
//  /**
//   * Removes all stored term-data.
//   */
//  void clearTermData();
  /**
   * Get a {@link DocumentModel} instance for the document with the given id.
   *
   * @param docId Lucene document-id
   * @return Document model associated with the given Lucene document-id
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
   */
  Collection<ByteArray> getDocumentsTermSet(final Collection<Integer> docIds);

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
   * Test accessor: get stopwords.
   *
   * @return List of stopwords currently set
   */
  Collection<String> testGetStopwords();

  /**
   * Test accessor: get fields.
   *
   * @return List of fields currently set
   */
  Collection<String> testGetFieldNames();
}
