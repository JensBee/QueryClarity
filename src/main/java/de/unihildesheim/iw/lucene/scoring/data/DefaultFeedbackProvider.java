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
import de.unihildesheim.iw.lucene.query.TryExactTermsQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

/**
 * Default implementation of a {@link FeedbackProvider} using the {@link
 * TryExactTermsQuery} for retrieval.
 *
 * @author Jens Bertram
 */
public class DefaultFeedbackProvider
    extends AbstractFeedbackProvider<DefaultFeedbackProvider> {
  @Nullable
  private IndexSearcher searcher;

  public DefaultFeedbackProvider indexReader(final IndexReader indexReader) {
    super.indexReader(indexReader);
    this.searcher = IndexUtils.getSearcher(indexReader);
    return this;
  }

  @Override
  public DefaultFeedbackProvider getThis() {
    return this;
  }

  @Override
  public DocIdSet get()
      throws ParseException, IOException, InvocationTargetException,
             NoSuchMethodException, InstantiationException,
             IllegalAccessException {
    final RelaxableQuery qObj = getQueryParserInstance();
    if (this.useFixedAmount) {
      return FeedbackQuery.getFixed(
          Objects.requireNonNull(this.searcher,
              "IndexReader (Searcher) not set."),
          Objects.requireNonNull(this.dataProv, "IndexDataProvider not set."),
          qObj, this.fixedAmount);
    }
    return FeedbackQuery.getMinMax(
        Objects.requireNonNull(this.searcher,
            "IndexReader (Searcher) not set."),
        qObj, this.minAmount, this.maxAmount);
  }
}
