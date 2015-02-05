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

import de.unihildesheim.iw.lucene.index.IndexDataProvider;

import java.util.Objects;
import java.util.Set;

/**
 * Base (no-operation) class for more specific {@link VocabularyProvider}
 * implementations. Overriding implementations should replace methods as
 * needed.
 *
 * @author Jens Bertram
 */
public abstract class AbstractVocabularyProvider<T extends VocabularyProvider>
    implements VocabularyProvider {
  /**
   * Data provider for index data.
   */
  IndexDataProvider dataProv;
  /**
   * Document id's whose terms should be used as vocabulary.
   */
  Set<Integer> docIds;

  @Override
  public T indexDataProvider(final IndexDataProvider indexDataProvider) {
    this.dataProv = Objects.requireNonNull(indexDataProvider);
    return getThis();
  }

  /**
   * Get a self reference of the overriding class.
   *
   * @return Self reference
   */
  protected abstract T getThis();

  @Override
  public T documentIds(final Set<Integer> documentIds) {
    this.docIds = Objects.requireNonNull(documentIds);
    return getThis();
  }
}
