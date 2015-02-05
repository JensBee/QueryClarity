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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;

import java.util.Objects;
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

  /**
   * Number of documents to get, if a fixed amount is requested.
   */
  int fixedAmount;
  /**
   * Minimum number of documents to get.
   */
  int minAmount;
  /**
   * Maximum number of documents to get.
   */
  int maxAmount;
  /**
   * True, if a fixed amount of documents should be tried to retrieve.
   */
  boolean useFixedAmount;

  /**
   * Reader to access the index.
   */
  IndexReader idxReader;
  /**
   * Reader to access the index.
   */
  IndexDataProvider dataProv;
  /**
   * Query analyzer.
   */
  Analyzer qAnalyzer;
  /**
   * Query string.
   */
  String queryStr;
  /**
   * Document fields to query.
   */
  Set<String> docFields;

  @Override
  public T query(final String query) {
    this.queryStr = Objects.requireNonNull(query);
    return getThis();
  }

  @Override
  public T amount(final int min, final int max) {
    this.minAmount = min;
    this.maxAmount = max;
    this.useFixedAmount = false;
    return getThis();
  }

  @Override
  public T amount(final int fixed) {
    this.fixedAmount = fixed;
    this.useFixedAmount = true;
    return getThis();
  }

  @Override
  public T indexReader(final IndexReader indexReader) {
    this.idxReader = Objects.requireNonNull(indexReader);
    return getThis();
  }

  @Override
  public T analyzer(final Analyzer analyzer) {
    this.qAnalyzer = Objects.requireNonNull(analyzer);
    return getThis();
  }

  @Override
  public T dataProvider(final IndexDataProvider dp) {
    this.dataProv = Objects.requireNonNull(dp);
    return getThis();
  }

  @Override
  public T fields(final Set<String> fields) {
    this.docFields = Objects.requireNonNull(fields);
    return getThis();
  }

  /**
   * Get a self reference of the overriding class.
   *
   * @return Self reference
   */
  protected abstract T getThis();
}
