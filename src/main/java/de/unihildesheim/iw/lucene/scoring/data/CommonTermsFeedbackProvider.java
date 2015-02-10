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

import de.unihildesheim.iw.lucene.query.RelaxableCommonTermsQuery;
import de.unihildesheim.iw.lucene.query.RelaxableQuery;

/**
 * {@link FeedbackProvider} using a {@link RelaxableCommonTermsQuery} to
 * retrieve feedback documents. Thus trying to avoid common terms found in a
 * lot of documents in the index.
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class CommonTermsFeedbackProvider
  extends DefaultFeedbackProvider {

  public CommonTermsFeedbackProvider() {
    super.queryParser(RelaxableCommonTermsQuery.class);
  }

  @Override
  public CommonTermsFeedbackProvider getThis() {
    return this;
  }

  @Override
  public CommonTermsFeedbackProvider queryParser(
      final Class<? extends RelaxableQuery> rtq) {
    throw new UnsupportedOperationException("Query parser cannot be changed.");
  }
}
