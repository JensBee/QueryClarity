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
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * Base (no-operation) class for more specific {@link FeedbackProvider}
 * implementations. Overriding implementations should replace methods as
 * needed.
 *
 * @author Jens Bertram
 */
public abstract class AbstractFeedbackProvider
    <I extends AbstractFeedbackProvider<I>>
    implements FeedbackProvider {

  /**
   * Number of documents to get, if a fixed amount is requested.
   */
  protected int fixedAmount;
  /**
   * Minimum number of documents to get.
   */
  protected int minAmount;
  /**
   * Maximum number of documents to get.
   */
  protected int maxAmount;
  /**
   * True, if a fixed amount of documents should be tried to retrieve.
   */
  protected boolean useFixedAmount;

  /**
   * Reader to access the index.
   */
  @Nullable
  private IndexReader idxReader;
  /**
   * Reader to access the index.
   */
  protected IndexDataProvider dataProv;
  /**
   * Query analyzer.
   */
  @Nullable
  private Analyzer qAnalyzer;
  /**
   * Query string.
   */
  @Nullable
  private String queryStr;
  /**
   * Document fields to query.
   */
  @Nullable
  private String[] docFields;
  /**
   * Query parser to use. Defaults to {@link TryExactTermsQuery}.
   */
  @Nullable
  Class<? extends RelaxableQuery> queryParser;

  @Override
  public I query(@NotNull final String query) {
    this.queryStr = query;
    return getThis();
  }

  @Override
  public I amount(final int min, final int max) {
    this.minAmount = min;
    this.maxAmount = max;
    this.useFixedAmount = false;
    return getThis();
  }

  @Override
  public I amount(final int fixed) {
    this.fixedAmount = fixed;
    this.useFixedAmount = true;
    return getThis();
  }

  @Override
  public I indexReader(
      @NotNull final IndexReader indexReader) {
    this.idxReader = indexReader;
    return getThis();
  }

  @Override
  public I analyzer(@NotNull final Analyzer analyzer) {
    this.qAnalyzer = analyzer;
    return getThis();
  }

  @Override
  public I dataProvider(
      @NotNull final IndexDataProvider dp) {
    this.dataProv = dp;
    return getThis();
  }

  @Override
  public I fields(@NotNull final String... fields) {
    this.docFields = fields.clone();
    return getThis();
  }

  @Override
  public I queryParser(
      @NotNull final Class<? extends RelaxableQuery> rtq) {
    this.queryParser = rtq;
    return getThis();
  }

  /**
   * Get a list of document fields to query. If document fields are already set
   * by using {@link #fields(String[])} these will be returned. Otherwise all
   * fields available to the {@link #idxReader IndexReader} will be returned. If
   * there are no postings available to the reader an {@link
   * IllegalStateException} will be thrown.
   *
   * @return Fields or {@code null} if no fields are set
   * @throws IOException Thrown on low-level i/o-errors
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  protected final String[] getDocumentFields()
      throws IOException {
    if (this.docFields == null) {
      final Fields fields = MultiFields.getFields(
          Objects.requireNonNull(this.idxReader, "IndexReader not set"));
      if (fields == null) {
        throw new IllegalStateException("Reader has no postings.");
      }
      return StreamSupport.stream(fields.spliterator(), false)
          .toArray(String[]::new);
    }
    return this.docFields;
  }

  /**
   * Creates a new instance of the currently set {@link RelaxableQuery} class
   * using reflection.
   *
   * @return New instance
   * @throws NoSuchMethodException Thrown, if the required constructor is not
   * defined.
   * @throws IllegalAccessException Thrown, if creating the instance failed
   * @throws InvocationTargetException Thrown, if creating the instance failed
   * @throws InstantiationException Thrown, if creating the instance failed
   * @throws IOException Thrown on low-level i/o-errors
   */
  protected RelaxableQuery getQueryParserInstance()
      throws NoSuchMethodException, IllegalAccessException,
             InvocationTargetException, InstantiationException, IOException {
    final Constructor cTor = getQueryParser().getDeclaredConstructor(
        Analyzer.class, String.class, String[].class);
    return (RelaxableQuery) cTor.newInstance(
        Objects.requireNonNull(this.qAnalyzer, "Analyzer not set."),
        Objects.requireNonNull(this.queryStr, "Query string not set."),
        getDocumentFields());
  }

  /**
   * Get the defined {@link RelaxableQuery} instance that will be used.
   *
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
  protected abstract I getThis();
}
