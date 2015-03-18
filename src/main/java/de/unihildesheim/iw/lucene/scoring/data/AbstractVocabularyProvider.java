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
import org.apache.lucene.search.DocIdSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Base (no-operation) class for more specific {@link VocabularyProvider}
 * implementations. Overriding implementations should replace methods as
 * needed.
 *
 * @author Jens Bertram
 */
public abstract class AbstractVocabularyProvider
    implements VocabularyProvider {
  /**
   * Data provider for index data.
   */
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  @Nullable
  IndexDataProvider dataProv;
  /**
   * Document id's whose terms should be used as vocabulary.
   */
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  @Nullable
  DocIdSet docIds;

  @Override
  public AbstractVocabularyProvider indexDataProvider(
      @NotNull final IndexDataProvider indexDataProvider) {
    this.dataProv = indexDataProvider;
    return getThis();
  }

  /**
   * Get a self reference of the overriding class.
   *
   * @return Self reference
   */
  protected abstract AbstractVocabularyProvider getThis();

  @Override
  public AbstractVocabularyProvider documentIds(
      @NotNull final DocIdSet documentIds) {
    this.docIds = documentIds;
    return getThis();
  }
}
