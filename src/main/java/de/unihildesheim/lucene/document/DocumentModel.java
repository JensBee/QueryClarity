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

import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.Lockable;
import java.io.Serializable;

/**
 * General document model storing basic values needed for calculation. All
 * classes implementing this interface should take care of mutability, since the
 * document models should be easy cacheable.
 *
 * To allow fine grained control over when the instance may be altered, the
 * {@link DefaultDocumentModel#lock()} and {@link DefaultDocumentModel#unlock()}
 * methods should be implemented as needed.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public interface DocumentModel extends Serializable, Lockable {

  /**
   * Serialization class version id.
   */
  long serialVersionUID = 0L;

  /**
   * Set the id of the associated Lucene document.
   *
   * @param documentId Id of the associated Lucene document
   */
  void setDocId(final int documentId);

  /**
   * Create a new plain {@link DocumentModel} with a specific document-id and
   * the expected number of terms to which data should be stored. Meant to
   * initialize plain instances created by the default empty constructor only.
   *
   * @param documentId Id of the associated Lucene document
   * @param termsCount Number of terms to expect for this document. This value
   * will be used to initialize the data store to the appropriate size.
   * @return New empty {@link DocumentModel} instance
   */
  void create(final int documentId, final int termsCount);

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
  boolean containsTerm(final BytesWrap term);

  /**
   * Get the frequency of the given term in the document.
   *
   * @param term Non <tt>null</tt> term to lookup
   * @return Frequency of the given term in the document
   */
  long getTermFrequency(final BytesWrap term);

  /**
   * Set the frequency value for a specific term.
   *
   * @param term Term whose frequency value should be set
   * @param frequency Frequency value
   */
  void setTermFrequency(final BytesWrap term, final long frequency);

  /**
   * Get a specific value stored for a term by a given key.
   *
   * @param term Term whose value should be retrieved
   * @param key Key under which the data is stored
   * @return Stored {@link Number} value, or <tt>null</tt> if no value was
   * stored under the specified key.
   */
  Number getTermData(final BytesWrap term, final String key);

  /**
   * Store a value for a term in this document. This will silently overwrite any
   * previously stored value.
   *
   * @param term Non <tt>null</tt> term to store a value for
   * @param key Non <tt>null</tt> key to identify the value
   * @param value {@link Number} value to store
   */
  void setTermData(final BytesWrap term, final String key,
          final Number value);

  /**
   * Lock the document model, making all it's data immutable.
   */
  void lock();

  /**
   * Un-lock the document model to make it mutable again.
   */
  void unlock();

  /**
   * Indicates whether this model has changed data.
   * @return True, if data was changed
   */
  boolean hasChanged();

  /**
   * Set the changed flag.
   * @param state True, if changed, false otherwise
   */
  void setChanged(final boolean state);
}
