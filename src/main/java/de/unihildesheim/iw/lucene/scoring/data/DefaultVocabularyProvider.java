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
import org.apache.lucene.util.BytesRef;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.stream.Stream;

/**
 * Default {@link VocabularyProvider} implementation that uses an {@link
 * IndexDataProvider} and a set of document-ids to create a vocabulary. The
 * vocabulary is equal to all unique terms from all documents described by their
 * id. This simply passes on the original iterator.
 *
 * @author Jens Bertram
 */
public final class DefaultVocabularyProvider
    extends AbstractVocabularyProvider<DefaultVocabularyProvider> {

  /**
   * Logger instance for this class.
   */
  static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      DefaultVocabularyProvider.class);

  @Override
  public DefaultVocabularyProvider getThis() {
    return this;
  }

  @Override
  public Stream<BytesRef> get() {
    return
        Objects.requireNonNull(this.dataProv,
            "Data provider not set.")
            .getDocumentsTerms(Objects.requireNonNull(
                this.docIds, "Document ids not set."));
  }
}
