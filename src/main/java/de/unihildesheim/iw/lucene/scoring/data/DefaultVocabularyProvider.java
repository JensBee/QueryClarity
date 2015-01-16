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
import de.unihildesheim.iw.mapdb.DBMakerUtils;
import org.mapdb.DB;
import org.slf4j.LoggerFactory;

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
  public Iterator<ByteArray> get()
      throws DataProviderException {
    LOG.debug("Generating vocabulary");
    final Iterator<ByteArray> termsIt = Objects.requireNonNull(
        this.dataProv, "Data provider not set.").getDocumentsTermsSet(
        Objects.requireNonNull(this.docIds, "Document ids not set."));

    if (this.filter == null) {
      // forward the plain iterator
      return termsIt;
    }

    LOG.debug("Using filter");
    final DB termsCache = DBMakerUtils.newCompressedTempFileDB().make();
    final Set<ByteArray> terms = termsCache.createTreeSet("termsCache")
        .serializer(ByteArray.SERIALIZER_BTREE)
        .make();
    while (termsIt.hasNext()) {
      final ByteArray term = this.filter.filter(termsIt.next());
      if (term != null) {
        terms.add(term);
      }
    }

    return new Iterator<ByteArray>() {
      private final Iterator<ByteArray> termsIt = terms.iterator();

      @Override
      public boolean hasNext() {
        final boolean state = this.termsIt.hasNext();
        if (!state) {
          termsCache.close();
        }
        return state;
      }

      @Override
      public ByteArray next() {
        return this.termsIt.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
