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

import de.unihildesheim.iw.lucene.document.FeedbackQuery;
import de.unihildesheim.iw.lucene.index.IndexUtils;
import de.unihildesheim.iw.lucene.query.RelaxableQuery;
import de.unihildesheim.iw.lucene.query.RxTryExactTermsQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Default implementation of a {@link FeedbackProvider} using the {@link
 * RxTryExactTermsQuery} for retrieval.
 *
 * @author Jens Bertram
 */
public final class DefaultFeedbackProvider
    extends AbstractFeedbackProvider<DefaultFeedbackProvider> {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultFeedbackProvider.class);
  /**
   * Reusable {@link IndexSearcher} instance.
   */
  @Nullable
  private IndexSearcher searcher;

  @Override
  public DefaultFeedbackProvider indexReader(
      @NotNull final IndexReader indexReader) {
    super.indexReader(indexReader);
    this.searcher = IndexUtils.getSearcher(indexReader);
    return this;
  }

  @Override
  public DefaultFeedbackProvider getThis() {
    return this;
  }

  @Override
  public DefaultFeedbackProvider queryParser(@NotNull final RelaxableQuery rtq) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DocIdSet get()
      throws IOException, ParseException {
    final RelaxableQuery qObj = getQueryParserInstance();
    if (this.useFixedAmount) {
      return FeedbackQuery.getFixed(
          Objects.requireNonNull(this.searcher,
              "IndexReader (Searcher) not set."),
          Objects.requireNonNull(this.dataProv, "IndexDataProvider not set."),
          qObj, this.fixedAmount, getDocumentFields());
    } else if (this.useUnboundAmount) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using unbound feedback amount");
      }
      final Query queryObj = qObj.getQueryObj();
      // single term query or boolean type - depends on number of terms
      if (BooleanQuery.class.isInstance(queryObj)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reset number of required query terms that must match to " +
              "1.");
        }
        ((BooleanQuery) queryObj).setMinimumNumberShouldMatch(1);
      }
      return FeedbackQuery.getMax(
          Objects.requireNonNull(this.searcher,
              "IndexReader (Searcher) not set."),
          queryObj, Integer.MAX_VALUE);
    }
    return FeedbackQuery.getMinMax(
        Objects.requireNonNull(this.searcher,
            "IndexReader (Searcher) not set."),
        qObj, this.minAmount, this.maxAmount);
  }
}
