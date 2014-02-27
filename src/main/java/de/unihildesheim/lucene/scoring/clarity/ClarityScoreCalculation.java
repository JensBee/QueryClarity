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
package de.unihildesheim.lucene.scoring.clarity;

import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.scoring.clarity.impl.ClarityScoreResult;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public interface ClarityScoreCalculation {

  /**
   * Calculate the clarity score based on the given query terms.
   *
   * @param query Query used for term extraction
   * @return Calculated clarity score for the given terms, or <tt>null</tt>
   * on errors.
   */
  ClarityScoreResult calculateClarity(final Query query);

//  /**
//   * Get the {@link IndexDataProvider} for statistical index related
//   * informations used by this instance.
//   *
//   * @return Data provider used by this instance
//   */
//  IndexDataProvider getIndexDataProvider();
//
//  /**
//   * Get the {@link IndexReader} used by this instance.
//   *
//   * @return IndexReader used by this instance
//   */
//  IndexReader getReader();
}
