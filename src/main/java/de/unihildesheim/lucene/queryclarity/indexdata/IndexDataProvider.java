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
package de.unihildesheim.lucene.queryclarity.indexdata;

import de.unihildesheim.lucene.queryclarity.documentmodel.DocumentModel;
import java.util.Set;

/**
 * IndexDataProvider provides statistical data from the underlying lucene index.
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
   * Get the frequency of all terms in a specific document.
   *
   * @param documentId Target document-id
   * @return Frequency of all terms in the specified document
   */
  long getTermFrequency(final int documentId);

  /**
   * Get the frequency of a single term in a specific document.
   *
   * @param documentId Target document-id
   * @param term Term to lookup
   * @return Frequency of the given term in the specified document
   */
  long getTermFrequency(final int documentId, final String term);

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
   * Get the index-fields this dataprovider operates on.
   */
  String[] getTargetFields();

  /**
   * Get a unique set of all terms in the index.
   *
   * @return Set of all terms in the index
   */
  Set<String> getTerms();

  /**
   * Get a {@link DocumentModel} instance for the document with the given id.
   *
   * @param docId Lucene document-id
   * @return Document model associated with the given lucene document-id
   */
  DocumentModel getDocumentModel(final int docId);

  /**
   * Retrieve the probability value of term t modelling the document with the
   * given id.
   *
   * @param documentId Target document identified by lucene's document-id
   * @param term The term to lookup
   * @return Pre-calculated probability value of term t modelling the document
   * with the given id
   */
  double getDocumentTermProbability(final int documentId, final String term);
}
