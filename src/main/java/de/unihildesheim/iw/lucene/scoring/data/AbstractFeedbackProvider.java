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
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.query.RelaxableQuery;
import de.unihildesheim.iw.lucene.query.RxTryExactTermsQuery;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.BytesRefArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
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
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractFeedbackProvider.class);
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
   * True, if all matching documents should be retrieved.
   */
  boolean useUnboundAmount;

  /**
   * Reader to access the index.
   */
  @Nullable
  IndexReader reader;
  /**
   * Reader to access the index.
   */
  @Nullable
  IndexDataProvider dataProv;
  /**
   * Query analyzer.
   */
  @Nullable
  Analyzer analyzer;
  /**
   * Query string.
   */
  @Nullable
  String queryStr;
  /**
   * Query terms.
   */
  @Nullable
  Collection<String> queryTerms;
  /**
   * Query terms (BytesRef array).
   */
  @Nullable
  BytesRefArray queryTermsArr;
  /**
   * Document fields to query.
   */
  @Nullable
  private String[] docFields;
  /**
   * Query parser to use. Defaults to {@link RxTryExactTermsQuery}.
   */
  @Nullable
  private
  RelaxableQuery queryParser;

  @Override
  public I query(final String q) {
    if (q != null && !q.trim().isEmpty()) {
      this.queryStr = q;
    }
    return getThis();
  }

  @Override
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  public I query(final Collection<String> q) {
    if (q != null && !q.isEmpty()) {
      this.queryTerms = q;
    }
    return getThis();
  }

  @Override
  public I query(final BytesRefArray q) {
    if (q != null && q.size() > 0) {
      this.queryTermsArr = q;
    }
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
  public I unboundAmount() {
    this.minAmount = 1;
    this.useUnboundAmount = true;
    return getThis();
  }

  @Override
  public I unboundAmount(final int min) {
    this.minAmount = min;
    this.useUnboundAmount = true;
    return getThis();
  }

  @Override
  public I indexReader(
      @NotNull final IndexReader indexReader) {
    this.reader = indexReader;
    return getThis();
  }

  @Override
  public I analyzer(@NotNull final Analyzer analyzer) {
    this.analyzer = analyzer;
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
  public I queryParser(@NotNull final RelaxableQuery rtq) {
    this.queryParser = rtq;
    return getThis();
  }

  /**
   * Get a list of document fields to query. If document fields are already set
   * by using {@link #fields(String[])} these will be returned. Otherwise all
   * fields available to the {@link #reader IndexReader} will be returned. If
   * there are no postings available to the reader an {@link
   * IllegalStateException} will be thrown.
   *
   * @return Fields (instance field, no clone) or {@code null} if no fields are
   * set
   * @throws IOException Thrown on low-level i/o-errors
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  final synchronized String[] getDocumentFields()
      throws IOException {
    if (this.docFields == null) {
      final Fields fields = MultiFields.getFields(
          Objects.requireNonNull(this.reader, "IndexReader not set"));
      if (fields == null) {
        throw new IllegalStateException("Reader has no postings.");
      }
      this.docFields = StreamSupport.stream(fields.spliterator(), false)
          .toArray(String[]::new);
    }
    return this.docFields;
  }

  /**
   * Creates a new instance of the currently set {@link RelaxableQuery} class
   * using reflection.
   *
   * @return New instance
   * @throws IOException Thrown on low-level i/o-errors
   * @throws ParseException Thrown on errors parsing the query
   */
  final RelaxableQuery getQueryParserInstance()
      throws ParseException, IOException {
    if (this.queryParser == null) {
      Objects.requireNonNull(this.analyzer, "Analyzer not set.");
      final Collection<String> queryTerms = getUniqueQueryTerms();
      if (queryTerms.isEmpty()) {
        throw new IllegalArgumentException("Query is empty.");
      }
      this.queryParser = new RxTryExactTermsQuery(
          this.analyzer, queryTerms, getDocumentFields());
    }
    return this.queryParser;
  }

  /**
   * Get a self reference of the overriding class.
   *
   * @return Self reference
   */
  protected abstract I getThis();

  /**
   * Get alist of unique query terms.
   * @return List of unique query terms
   * @throws IOException Thrown on low-level i/o-errors
   */
  protected Collection<String> getUniqueQueryTerms()
      throws IOException {
    final Set<String> queryTerms;
    assert this.analyzer != null;
    if (this.queryTerms != null && !this.queryTerms.isEmpty()) {
      queryTerms = new HashSet<>(this.queryTerms);
    } else if (this.queryTermsArr != null && this.queryTermsArr.size() >0) {
      queryTerms = new HashSet<>(
          BytesRefUtils.arrayToCollection(this.queryTermsArr));
    } else if (this.queryStr != null && !this.queryStr.trim().isEmpty()) {
      queryTerms = new HashSet<>(
          QueryUtils.tokenizeQueryString(this.queryStr, this.analyzer,
              this.dataProv)
      );
    } else {
      queryTerms = Collections.emptySet();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unique query terms: {}", queryTerms.size());
    }
    return queryTerms;
  }
}
