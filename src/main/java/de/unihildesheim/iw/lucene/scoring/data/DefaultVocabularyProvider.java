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
import java.util.Objects;
import java.util.Set;

/**
 * Default {@link VocabularyProvider} implementation that uses an {@link
 * IndexDataProvider} and a set of document-ids to create a vocabulary. The
 * vocabulary is equal to all unique terms from all documents described by their
 * id.
 *
 * @author Jens Bertram
 */
public class DefaultVocabularyProvider
    implements VocabularyProvider {
  private IndexDataProvider dataProv;
  private Set<Integer> docIds;

  @Override
  public VocabularyProvider indexDataProvider(
      final IndexDataProvider indexDataProvider) {
    this.dataProv = Objects.requireNonNull(indexDataProvider);
    return this;
  }

  @Override
  public VocabularyProvider documentIds(final Set<Integer> documentIds) {
    this.docIds = Objects.requireNonNull(documentIds);
    return this;
  }

  @Override
  public Iterator<ByteArray> get()
      throws DataProviderException {
    return Objects.requireNonNull(this.dataProv, "Data provider not set.")
        .getDocumentsTermsSet(
            Objects.requireNonNull(this.docIds, "Document ids not set."));
  }
}