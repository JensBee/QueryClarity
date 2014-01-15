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

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public interface DocumentModel {

  /**
   * Get the id of the associated lucene document.
   *
   * @return Id of the associated lucene document
   */
  int id();

  /**
   * Get the overall frequency of all terms in the document.
   *
   * @return Summed frequency of all terms in the document
   */
  long termFrequency();

  /**
   * Get the frequency of the given term in the document.
   *
   * @param term Term to lookup
   * @return Frequency of the given term in the document
   */
  long termFrequency(final String term);

  /**
   * Get the calculated probability value for the given term. If the returned
   * value is <code>0</code> this means the term was not found in the document
   * and further calculations should be done to get a meaningful value.
   *
   * @param term Term to lookup
   * @return Calculated probability value for the given term. The result is
   * <code>0</code> if the term was not found in the document.
   */
  double termProbability(final String term);
}
