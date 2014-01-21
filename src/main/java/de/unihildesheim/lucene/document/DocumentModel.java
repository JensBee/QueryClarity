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
package de.unihildesheim.lucene.document;

import java.io.Serializable;

/**
 * General document model storing basic values needed for calculation. All
 * classes implementing this interface should be immutable, since the document
 * models should be easy cacheable.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public interface DocumentModel extends Serializable {

  /**
   * Serialization class version id.
   */
  long serialVersionUID = 0L;

  /**
   * Set the id of the associated Lucene document.
   *
   * @param documentId Id of the associated Lucene document.
   * @return New {@link DocumentModel} with all properties of the current object
   * and the given id set.
   */
  DocumentModel setDocId(final int documentId);

  /**
   * Get the id of the associated Lucene document.
   *
   * @return Id of the associated Lucene document
   */
  int getDocId();

  /**
   * Get the overall frequency of all terms in the document.
   *
   * @return Summed frequency of all terms in the document
   */
  long getTermFrequency();

  /**
   * Check if the document contains the given term.
   *
   * @param term Term to lookup
   * @return True if term is contained in document, false otherwise
   */
  boolean containsTerm(final String term);

  /**
   * Get the frequency of the given term in the document.
   *
   * @param term Non <tt>null</tt> term to lookup
   * @return Frequency of the given term in the document
   */
  long getTermFrequency(final String term);

  /**
   * Set the term frequency value for a specific term.
   *
   * @param term Non <tt>null</tt> term
   * @param frequency Document frequency for the specific term
   * @return New {@link DocumentModel} with all properties of the current object
   * and the given frequency value set.
   */
  DocumentModel addTermFrequency(final String term, final long frequency);

  /**
   * Get a specific value stored for a term by a given key.
   *
   * @param term Term whose value should be retrieved
   * @param key Key under which the data is stored
   * @return Stored value, or <tt>null</tt> if no value was stored under the
   * specified key.
   */
  Object getTermData(final String term, final String key);

  /**
   * Store a value for a term in this document. This will silently overwrite any
   * previously stored value.
   *
   * @param term Non <tt>null</tt> term to store a value for
   * @param key Non <tt>null</tt> key to identify the value
   * @param value Value to store
   * @return New {@link DocumentModel} with all properties of the current object
   * and the given key-value data set.
   */
  DocumentModel addTermData(final String term, final String key,
          final Object value);
}
