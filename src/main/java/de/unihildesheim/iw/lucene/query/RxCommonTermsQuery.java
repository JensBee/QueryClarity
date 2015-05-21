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

package de.unihildesheim.iw.lucene.query;

import de.unihildesheim.iw.util.Buildable;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRefArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * {@link RelaxableQuery Relaxable} implementation of a {@link
 * CommonTermsQuery}.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class RxCommonTermsQuery
    implements TermsProvidingQuery, RelaxableQuery {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      RxCommonTermsQuery.class);
  /**
   * Final query object.
   */
  private final Query query;
  /**
   * List of terms contained in the query (stopped, analyzed). These are all
   * terms of the query regardless of high/low frequency occurrence.
   */
  private final Collection<String> queryTerms;

  /**
   * New instance using settings from the supplied {@link Builder} instance.
   *
   * @param builder {@link Builder} Instance builder
   * @throws IOException Thrown on low-level i/o-errors
   */
  RxCommonTermsQuery(@NotNull final Builder builder)
      throws IOException {
    assert builder.analyzer != null;
    assert builder.fields != null;

    if (builder.fields.length == 0) {
      throw new IllegalArgumentException("Empty fields list.");
    }

    // choose query source
    if (builder.queryTerms != null && !builder.queryTerms.isEmpty()) {
      this.queryTerms = new ArrayList<>(builder.queryTerms.size());
      this.queryTerms.addAll(builder.queryTerms);
    } else if (builder.queryTermsArr != null &&
        builder.queryTermsArr.size() > 0) {
      this.queryTerms = new ArrayList<>(builder.queryTermsArr.size());
      this.queryTerms.addAll(
          BytesRefUtils.arrayToCollection(builder.queryTermsArr));
    } else if (builder.queryStr != null && !builder.queryStr.trim().isEmpty()) {
      this.queryTerms = QueryUtils.tokenizeQueryString(
          builder.queryStr, builder.analyzer);
    } else {
      throw new IllegalArgumentException("Empty query.");
    }

    // list of unique terms contained in the query (stopped, analyzed)
    final String[] uniqueQueryTerms = this.queryTerms.stream()
        .distinct().toArray(String[]::new);

    final CommonTermsQuery ctQuery = new CommonTermsQuery(
        builder.highFreqOccur, builder.lowFreqOccur, builder.maxTermFrequency);

    assert builder.fields != null;
    for (final String field : builder.fields) {
      for (final String uniqueQueryTerm : uniqueQueryTerms) {
        ctQuery.add(new Term(field, uniqueQueryTerm));
      }
    }

    // at least one of the low-frequent terms must match
    ctQuery.setLowFreqMinimumNumberShouldMatch(1.0F);

    assert builder.reader != null;
    this.query = ctQuery.rewrite(builder.reader);

    if (LOG.isDebugEnabled()) {
      LOG.debug("RCTQ {} uQt={}", this.query, uniqueQueryTerms);
    }
  }

  @NotNull
  @Override
  public Query getQueryObj() {
    return this.query;
  }

  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
  @NotNull
  @Override
  public Collection<String> getQueryTerms() {
    return Collections.unmodifiableCollection(this.queryTerms);
  }

  /**
   * Builder to create a new {@link RxCommonTermsQuery} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      implements Buildable<RxCommonTermsQuery> {
    /**
     * IndexReader to access the Lucene index.
     */
    @Nullable
    IndexReader reader;
    /**
     * Analyzer to use for query parsing.
     */
    @Nullable
    Analyzer analyzer;
    /**
     * {@link Occur} used for high frequency terms.
     */
    @NotNull
    Occur highFreqOccur = Occur.SHOULD;
    /**
     * {@link Occur} used for low frequency terms.
     */
    @NotNull
    Occur lowFreqOccur = Occur.SHOULD;
    /**
     * a value in [0..1) (or absolute number &gt;=1) representing the maximum
     * threshold of a terms document frequency to be considered a low frequency
     * term.
     */
    float maxTermFrequency = 0.01f;
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
     * List of fields to query.
     */
    @Nullable
    String[] fields;

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @NotNull
    @Override
    public RxCommonTermsQuery build()
        throws BuildableException {
      validate();
      try {
        return new RxCommonTermsQuery(this);
      } catch (final IOException e) {
        throw new BuildException(e);
      }
    }

    @Override
    public void validate()
        throws ConfigurationException {
      if (this.analyzer == null) {
        throw new ConfigurationException("Analyzer not set.");
      }
      if (this.reader == null) {
        throw new ConfigurationException("IndexReader not set.");
      }
      if (this.fields == null || this.fields.length == 0) {
        throw new ConfigurationException("No query fields supplied.");
      }
      if ((this.queryTerms == null || this.queryTerms.isEmpty()) &&
          (this.queryTermsArr == null || this.queryTermsArr.size() == 0) &&
          (this.queryStr == null || this.queryStr.trim().isEmpty())) {
        throw new ConfigurationException("Empty query.");
      }
    }

    /**
     * Set the maximum threshold of a terms document frequency to be considered
     * a low frequency term. Defaults to {@link #maxTermFrequency}.
     *
     * @param mtf a value in [0..1) (or absolute number &gt;=1) representing the
     * maximum threshold of a terms document frequency to be considered a low
     * frequency term.
     * @return Self reference
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public Builder maxTermFrequency(final float mtf) {
      this.maxTermFrequency = mtf;
      return this;
    }

    /**
     * Set the {@link Occur} used for low frequency terms. Defaults to {@link
     * #lowFreqOccur}.
     *
     * @param lfo {@link Occur} used for high frequency terms
     * @return Self reference
     */
    public Builder lowFreqOccur(@NotNull final Occur lfo) {
      this.lowFreqOccur = lfo;
      return this;
    }

    /**
     * Set the {@link Occur} used for high frequency terms. Defaults to {@link
     * #highFreqOccur}.
     *
     * @param hfo {@link Occur} used for high frequency terms
     * @return Self reference
     */
    public Builder highFreqOccur(@NotNull final Occur hfo) {
      this.highFreqOccur = hfo;
      return this;
    }

    /**
     * Set the fields to query.
     *
     * @param f Non empty list of fields to query
     * @return Self reference
     */
    public Builder fields(@NotNull final String... f) {
      this.fields = f.clone();
      return this;
    }


    /**
     * Set the query string.
     *
     * @param q Non empty query string
     * @return Self reference
     */
    public Builder query(final String q) {
      if (q != null && !q.trim().isEmpty()) {
        this.queryStr = q;
      }
      return this;
    }

    /**
     * Set the query string.
     *
     * @param q Non empty query string
     * @return Self reference
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    public Builder query(final Collection<String> q) {
      if (q != null && !q.isEmpty()) {
        this.queryTerms = q;
      }
      return this;
    }

    /**
     * Set the query string.
     *
     * @param q Non empty query string
     * @return Self reference
     */
    public Builder query(final BytesRefArray q) {
      if (q!=null && q.size() > 0) {
        this.queryTermsArr = q;
      }
      return this;
    }

    /**
     * Set the {@link IndexReader} used for parsing query terms.
     *
     * @param r IndexReader
     * @return Self reference
     */
    public Builder reader(@NotNull final IndexReader r) {
      this.reader = r;
      return this;
    }

    /**
     * Set the {@link Analyzer} used for parsing the query.
     *
     * @param a Analyzer
     * @return Self reference
     */
    public Builder analyzer(@NotNull final Analyzer a) {
      this.analyzer = a;
      return this;
    }
  }
}
