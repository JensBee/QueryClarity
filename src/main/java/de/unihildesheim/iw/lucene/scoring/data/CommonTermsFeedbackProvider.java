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

import de.unihildesheim.iw.util.Buildable.BuildableException;
import de.unihildesheim.iw.lucene.document.FeedbackQuery;
import de.unihildesheim.iw.lucene.index.IndexUtils;
import de.unihildesheim.iw.lucene.query.RelaxableQuery;
import de.unihildesheim.iw.lucene.query.RxCommonTermsQuery;
import de.unihildesheim.iw.lucene.query.RxCommonTermsQuery.Builder;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link FeedbackProvider} using the {@link RxCommonTermsQuery} for
 * retrieval.
 *
 * @author Jens Bertram
 */
public final class CommonTermsFeedbackProvider
    extends AbstractFeedbackProvider<CommonTermsFeedbackProvider> {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      CommonTermsFeedbackProvider.class);
  /**
   * Reusable {@link IndexSearcher} instance.
   */
  @Nullable
  private IndexSearcher searcher;

  @Override
  public CommonTermsFeedbackProvider indexReader(
      @NotNull final IndexReader indexReader) {
    super.indexReader(indexReader);
    this.searcher = IndexUtils.getSearcher(indexReader);
    return this;
  }

  @Override
  public CommonTermsFeedbackProvider getThis() {
    return this;
  }

  @Override
  public DocIdSet get()
      throws IOException, BuildableException {
    final RelaxableQuery qObj = new Builder()
        .analyzer(Objects.requireNonNull(this.analyzer,
            "Analyzer not set."))
        .fields(getDocumentFields())
        .reader(Objects.requireNonNull(this.reader,
            "IndexReader not set."))
        .query(getUniqueQueryTerms())
        .build();

    if (this.useFixedAmount) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using Fixed feedback amount={}", this.fixedAmount);
      }
      return FeedbackQuery.getFixed(
          Objects.requireNonNull(this.searcher,
              "IndexReader (Searcher) not set."),
          Objects.requireNonNull(this.dataProv, "IndexDataProvider not set."),
          qObj, this.fixedAmount, getDocumentFields());
    } else if (this.useUnboundAmount) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using unbound feedback amount");
      }
      return FeedbackQuery.getMax(
          Objects.requireNonNull(this.searcher,
              "IndexReader (Searcher) not set."),
          qObj.getQueryObj(), Integer.MAX_VALUE);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("using MinMax feedback min={} max={}",
          this.minAmount, this.maxAmount);
    }
    return FeedbackQuery.getMinMax(
        Objects.requireNonNull(this.searcher,
            "IndexReader (Searcher) not set."),
        qObj, this.minAmount, this.maxAmount);
  }
}
