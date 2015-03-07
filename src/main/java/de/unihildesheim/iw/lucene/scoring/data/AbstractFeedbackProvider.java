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
import de.unihildesheim.iw.lucene.query.RelaxableQuery;
import de.unihildesheim.iw.lucene.query.TryExactTermsQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

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
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  int fixedAmount;
  /**
   * Minimum number of documents to get.
   */
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  int minAmount;
  /**
   * Maximum number of documents to get.
   */
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  int maxAmount;
  /**
   * True, if a fixed amount of documents should be tried to retrieve.
   */
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  boolean useFixedAmount;

  /**
   * Reader to access the index.
   */
  @Nullable
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  IndexReader idxReader;
  /**
   * Reader to access the index.
   */
  IndexDataProvider dataProv;
  /**
   * Query analyzer.
   */
  @Nullable
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  Analyzer qAnalyzer;
  /**
   * Query string.
   */
  @Nullable
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  String queryStr;
  /**
   * Document fields to query.
   */
  @Nullable
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  String[] docFields;
  /**
   * Query parser to use. Defaults to {@link TryExactTermsQuery}.
   */
  @Nullable
  @SuppressWarnings("InstanceVariableMayNotBeInitialized")
  Class<? extends RelaxableQuery> queryParser;

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
  public T fields(final String[] fields) {
    this.docFields = Objects.requireNonNull(fields);
    return getThis();
  }

  @Override
  public T queryParser(final Class<? extends RelaxableQuery> rtq) {
    this.queryParser = Objects.requireNonNull(rtq);
    return getThis();
  }

  /**
   * Creates a new instance of the currently set {@link RelaxableQuery} class
   * using reflection.
   * @return New instance
   * @throws NoSuchMethodException Thrown, if the required constructor is not
   * defined.
   * @throws IllegalAccessException Thrown, if creating the instance failed
   * @throws InvocationTargetException Thrown, if creating the instance failed
   * @throws InstantiationException Thrown, if creating the instance failed
   */
  protected RelaxableQuery getQueryParserInstance()
      throws NoSuchMethodException, IllegalAccessException,
             InvocationTargetException, InstantiationException {
    final Constructor cTor = getQueryParser().getDeclaredConstructor(
        Analyzer.class, String.class, String[].class);
    return (RelaxableQuery)cTor.newInstance(
        Objects.requireNonNull(this.qAnalyzer, "Analyzer not set."),
        Objects.requireNonNull(this.queryStr, "Query string not set."),
        Objects.requireNonNull(this.docFields, "Document fields not set."));
  }

  /**
   * Get the defined {@link RelaxableQuery} instance that will be used.
   * @return RelaxableQuery class
   */
  private Class<? extends RelaxableQuery> getQueryParser() {
    return this.queryParser == null ? TryExactTermsQuery.class :
        this.queryParser;
  }

  /**
   * Get a self reference of the overriding class.
   *
   * @return Self reference
   */
  protected abstract T getThis();
}
