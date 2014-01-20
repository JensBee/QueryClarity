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
import java.util.Collection;
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
  long getTermFrequency(final String term);

  /**
   * Get the relative term frequency for a term in the index.
   *
   * @param term Term to lookup
   * @return Relative term frequency for the given term
   */
  double getRelativeTermFrequency(final String term);

  /**
   * Close this instance. This is meant for handling cleanups after using this
   * instance. The behaviour of functions called after this is undefined.
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
  Iterator<String> getTermsIterator();

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
   * Get (remove) a document model from the list of known models. This is a safe
   * way to get a {@link DocumentModel} for doing modifications to the model.
   *
   * @param docId Id of the document model to remove
   * @return The document model associated with the given id, or <tt>null</tt>
   * if none was stored under the given id.
   */
  DocumentModel removeDocumentModel(final int docId);

  /**
   * Adds a {@link DocumentModel} to the list of known models. This should only
   * be used to re-add models that have previously been removed from the list to
   * apply any modifications.
   *
   * @param documentModel Document model to add to the list of known models
   */
  void addDocumentModel(final DocumentModel documentModel);

  Collection<DocumentModel> getDocModels();
}
