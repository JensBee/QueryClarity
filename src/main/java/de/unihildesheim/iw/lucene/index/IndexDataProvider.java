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

import de.unihildesheim.iw.lucene.document.DocumentModel;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;

import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * IndexDataProvider provides statistical data from the underlying Lucene index.
 * <br> Calculated values may be cached. So any call to those functions may not
 * trigger a recalculation of the values. If this is not desired, then needed
 * update functions must be provided by the implementing class. <br> Also, any
 * restriction to a subset of index fields must be applied by the implementing
 * class as they are no enforced.
 */
public interface IndexDataProvider
    extends AutoCloseable {

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
  long getTermFrequency(final BytesRef term);

  /**
   * Get the relative term frequency of a single term in the index.
   *
   * @param term Term to lookup
   * @return The relative frequency of the term in the index
   */
  double getRelativeTermFrequency(final BytesRef term);

  /**
   * Get the document frequency of a single term in the index.
   *
   * @param term Term to lookup
   * @return The frequency of the term in the index
   */
  int getDocumentFrequency(final BytesRef term);

  /**
   * Get the relative document frequency of a single term in the index.
   *
   * @param term Term to lookup
   * @return The relative frequency of the term in the index
   */
  double getRelativeDocumentFrequency(final BytesRef term);

  /**
   * Close this instance. This is meant for handling cleanups after using this
   * instance. The behavior of functions called after this is undefined.
   * Default implementation does nothing.
   */
  @SuppressFBWarnings("ACEM_ABSTRACT_CLASS_EMPTY_METHODS")
  @Override
  default void close() {
    // NOP
  }

  /**
   * Get an all known document-ids.
   *
   * @return Stream of document-ids
   */
  @NotNull
  IntStream getDocumentIds();

  /**
   * Get a {@link DocumentModel} instance for the document with the given id.
   *
   * @param docId Lucene document-id
   * @return Document-model associated with the given Lucene document-id
   */
  @NotNull
  DocumentModel getDocumentModel(final int docId);

  /**
   * Test if a document (model) for the specific document-id is known.
   *
   * @param docId Document-id to lookup
   * @return True if a model is known, false otherwise
   */
  boolean hasDocument(final int docId);

  /**
   * Get terms for a document and a specific number of named fields.
   * @param docId Document id
   * @param field Fields to extract terms from
   * @return Terms from the requested document fields
   */
  Stream<BytesRef> getDocumentTerms(final int docId, String... field);

  /**
   * Get terms for all documents identified by their id.
   *
   * @param docIds List of document ids to extract terms from
   * @return Terms from all documents
   */
  @NotNull
  Stream<BytesRef> getDocumentsTerms(final DocIdSet docIds);

  /**
   * Get the number of all Documents (models) known to this instance.
   *
   * @return Number of Documents known
   */
  long getDocumentCount();

  /**
   * Get the list of currently visible document fields. The returned array
   * should not be modified by caller.
   *
   * @return List of document field names
   */
  @NotNull
  String[] getDocumentFields();

  /**
   * Get a collection metrics instance providing derived index information.
   *
   * @return {@link CollectionMetrics} instance
   */
  CollectionMetrics metrics();
}
