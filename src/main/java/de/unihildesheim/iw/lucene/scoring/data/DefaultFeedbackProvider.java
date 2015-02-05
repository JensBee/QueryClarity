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
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.query.RelaxableQuery;
import de.unihildesheim.iw.lucene.query.TryExactTermsQuery;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParserBase;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * Default implementation of a {@link FeedbackProvider} using the {@link
 * TryExactTermsQuery} for retrieval.
 *
 * @author Jens Bertram
 */
public final class DefaultFeedbackProvider
    extends AbstractFeedbackProvider<DefaultFeedbackProvider> {

  @Override
  public DefaultFeedbackProvider getThis() {
    return this;
  }

  @Override
  public Set<Integer> get()
      throws ParseException, IOException, DataProviderException {
    final RelaxableQuery qObj =
        new TryExactTermsQuery(
            Objects.requireNonNull(this.qAnalyzer, "Analyzer not set."),
            QueryParserBase.escape(Objects.requireNonNull(this.queryStr,
                "Query string not set.")),
            Objects.requireNonNull(this.docFields, "Document fields not set."));
    if (this.useFixedAmount) {
      return FeedbackQuery.getFixed(
          Objects.requireNonNull(this.idxReader, "IndexReader not set."),
          Objects.requireNonNull(this.dataProv, "IndexDataProvider not set."),
          qObj, this.fixedAmount);
    }
    return FeedbackQuery.getMinMax(
        Objects.requireNonNull(this.idxReader, "IndexReader not set."),
        qObj, this.minAmount, this.maxAmount);
  }
}
