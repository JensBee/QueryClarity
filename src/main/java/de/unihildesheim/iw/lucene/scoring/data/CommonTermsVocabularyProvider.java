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
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.stream.Stream;

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
  public Stream<ByteArray> get()
      throws DataProviderException {
    final float threshold = CommonTermsDefaults.MTF_DEFAULT;
    return Objects.requireNonNull(this.dataProv,
        "Data provider not set.")
        .getDocumentsTerms(Objects.requireNonNull(
            this.docIds, "Document ids not set."))
        .filter(t ->
            this.dataProv.metrics().relDf(t).floatValue() <= threshold);
  }
}
