/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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
import de.unihildesheim.iw.lucene.CommonTermsDefaults;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.mapdb.DBMakerUtils;
import org.mapdb.DB;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * Vocabulary provider avoiding terms common in the index.
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class CommonTermsVocabularyProvider
    extends AbstractVocabularyProvider<CommonTermsVocabularyProvider> {
  /**
   * Logger instance for this class.
   */
  static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      CommonTermsVocabularyProvider.class);

  @Override
  public CommonTermsVocabularyProvider getThis() {
    return this;
  }

  @Override
  public CommonTermsVocabularyProvider indexDataProvider(
      final IndexDataProvider indexDataProvider) {
    super.indexDataProvider(indexDataProvider);
    return this;
  }

  @Override
  public Iterator<ByteArray> get()
      throws DataProviderException {
    LOG.debug("Generating vocabulary");
    final Iterator<ByteArray> termsIt = Objects.requireNonNull(
        this.dataProv, "Data provider not set.").getDocumentsTermsSet(
        Objects.requireNonNull(this.docIds, "Document ids not set."));

    if (LOG.isDebugEnabled() && this.filter != null) {
      LOG.debug("Using filter");
    }

    final DB termsCache = DBMakerUtils.newCompressedTempFileDB().make();
    final Set<ByteArray> terms = termsCache.createTreeSet("termsCache")
        .serializer(ByteArray.SERIALIZER_BTREE)
        .make();

    while (termsIt.hasNext()) {
      ByteArray term = termsIt.next();
      // skip high frequent terms

      // terms exceeding this threshold (relative document frequency) are skipped.
      final float threshold = CommonTermsDefaults.MTF_DEFAULT;

      if (this.dataProv.metrics().relDf(term).floatValue() <= threshold) {
        if (this.filter != null) {
          // filter term
          term = this.filter.filter(term);
          if (term != null) {
            terms.add(term);
          }
        } else {
          terms.add(term);
        }
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
