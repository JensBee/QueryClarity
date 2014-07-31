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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

import java.util.Set;

/**
 * Base (no-operation) class for more specific {@link FeedbackProvider}
 * implementations. Overriding implementations should replace methods as
 * needed.
 *
 * @author Jens Bertram
 */
public abstract class AbstractFeedbackProvider<T extends FeedbackProvider>
    implements FeedbackProvider {

  @Override
  public T query(final String query) {
    return getThis();
  }

  @Override
  public T query(final Query query) {
    return getThis();
  }

  @Override
  public T amount(final int min, final int max) {
    return getThis();
  }

  @Override
  public T amount(final int fixed) {
    return getThis();
  }

  @Override
  public T indexReader(final IndexReader indexReader) {
    return getThis();
  }

  @Override
  public T analyzer(final Analyzer analyzer) {
    return getThis();
  }

  @Override
  public T fields(final Set<String> fields) {
    return getThis();
  }

  /**
   * Get a self reference of the overriding class.
   *
   * @return Self reference
   */
  public abstract T getThis();
}
