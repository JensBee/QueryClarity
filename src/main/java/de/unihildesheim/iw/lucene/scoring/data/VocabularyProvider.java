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

package de.unihildesheim.iw.lucene.scoring.data;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Jens Bertram
 */
public interface VocabularyProvider {
  /**
   * Set the data provider that may be sed to get feedback vocabulary.
   *
   * @param indexDataProvider Data provider instance
   * @return Self reference
   */
  VocabularyProvider indexDataProvider(final IndexDataProvider
      indexDataProvider);

  /**
   * Set the document ids that may be used to gather feedback vocabulary.
   *
   * @param documentIds Set of document ids in the Lucene index
   * @return Self reference
   */
  VocabularyProvider documentIds(final Set<Integer> documentIds);

  /**
   * Get the vocabulary.
   *
   * @return Vocabulary
   */
  Iterator<ByteArray> get()
      throws DataProviderException;
}
