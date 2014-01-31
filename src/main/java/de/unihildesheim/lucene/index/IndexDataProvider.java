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

import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesWrap;
import java.util.Iterator;

/**
 * IndexDataProvider provides statistical data from the underlying Lucene index.
 *
 * Calculated values may be cached. So any call to those functions may not
 * trigger a recalculation of the values. If this is not desired, then needed
 * update functions must be provided by the implementing class.
 *
 * Also, any restriction to a subset of index fields must be applied by the
 * implementing class as they are no enforced.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public interface IndexDataProvider {

  /**
   * Get the frequency of all terms in the index.
   *
   * @return The frequency of all terms in the index
   */
  long getTermFrequency();

  /**
   * Get the term frequency of a single term in the index.
   *
   * @param term Term to lookup
   * @return The frequency of the term in the index
   */
  long getTermFrequency(final BytesWrap term);

  /**
   * Get the relative term frequency for a term in the index.
   *
   * @param term Term to lookup
   * @return Relative term frequency for the given term
   */
  double getRelativeTermFrequency(final BytesWrap term);

  /**
   * Close this instance. This is meant for handling cleanups after using this
   * instance. The behavior of functions called after this is undefined.
   */
  void dispose();

  /**
   * Get the index-fields this DataProvider operates on.
   *
   * @return Index field names
   */
  String[] getTargetFields();

  /**
   * Get an {@link Iterator} over a unique set of all terms from the index.
   *
   * @return Unique terms iterator
   */
  Iterator<BytesWrap> getTermsIterator();

  /**
   * Get the number of unique terms in the index.
   *
   * @return Number of unique terms in the index
   */
  long getTermsCount();

  /**
   * Get an {@link Iterator} over all known {@link DocumentModel} instances.
   *
   * @return Iterator over all {@link DocumentModel} instances
   */
  Iterator<DocumentModel> getDocModelIterator();

  /**
   * Get a {@link DocumentModel} instance for the document with the given id.
   * The returned {@link DocumentModel} should be immutable and for reading
   * only. It's advised to use a {@link ImmutableDocumentModel} for returning.
   *
   * To actually modify a document model you should use the
   * {@link IndexDataProvider#removeDocumentModel(int)} and
   * {@link IndexDataProvider#addDocumentModel(DocumentModel)} methods.
   *
   * @param docId Lucene document-id
   * @return Document model associated with the given Lucene document-id
   */
  DocumentModel getDocumentModel(final int docId);

  /**
   * Updates an already stored {@link DocumentModel}. Use this to update any
   * model, that have been changed externally.
   *
   * @param docModel Document model to update. It must already have been in the
   * collection of known models.
   */
  void updateDocumentModel(final DocumentModel docModel);

  /**
   * Get the number of all {@link DocumentModel}s known to this instance.
   *
   * @return Number of {@link DocumentModel}s known
   */
  long getDocModelCount();

  /**
   * Stores a property value to the {@link IndexDataProvider}. Depending on the
   * implementation this property may be persistent.
   *
   * @param prefix Prefix to identify the property store
   * @param key Key to assign a property to
   * @param value Property value
   */
  void setProperty(final String prefix, final String key, final String value);

  /**
   * Retrieve a previously stored property from the {@link IndexDataProvider}.
   * Depending on the implementation stored property values may be persistent
   * between instantiations.
   *
   * @param prefix Prefix to identify the property store
   * @param key Key under which the property was stored
   * @return The stored property vale or null, if none was found
   */
  String getProperty(final String prefix, final String key);

  /**
   * Same as {@link IndexDataProvider#getProperty(String, String)}, but allows
   * to specify a default value.
   *
   * @param prefix Prefix to identify the property store
   * @param key Key under which the property was stored
   * @param defaultValue Default value to return, if the specified key was not
   * found
   * @return The stored property vale or <tt>defaultValue</tt>, if none was
   * found
   */
  String getProperty(final String prefix, final String key,
          final String defaultValue);
}
