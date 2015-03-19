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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.lucene.CommonTermsDefaults;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link RelaxableQuery Relaxable} implementation of a {@link
 * CommonTermsQuery}.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class RelaxableCommonTermsQuery
    implements TermsProvidingQuery, RelaxableQuery {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      RelaxableCommonTermsQuery.class);
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
   * List of unique terms contained in the query (stopped, analyzed).
   */
  private final Set<String> uniqueQueryTerms;
  /**
   * Number of fields that gets queried. Used for choosing the relax method.
   */
  private final int numberOfFields;

  /**
   * New instance using settings from the supplied {@link Builder} instance.
   *
   * @param builder {@link Builder} Instance builder
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  RelaxableCommonTermsQuery(@NotNull final Builder builder) {
    // get all query terms
    assert builder.queryStr != null;
    assert builder.analyzer != null;
    this.queryTerms = QueryUtils.tokenizeQueryString(
        builder.queryStr, builder.analyzer);

    // list of unique terms contained in the query (stopped, analyzed)
    this.uniqueQueryTerms = new HashSet<>(this.queryTerms);

    assert builder.fields != null;
    this.numberOfFields = builder.fields.length;

    if (this.numberOfFields == 1) {
      // query spans only one field so use simply a single CommonTermsQuery.

      this.query = new CommonTermsQuery(
          Occur.SHOULD,
          CommonTermsDefaults.LFOP_DEFAULT,
          CommonTermsDefaults.MTF_DEFAULT
      );

      // add terms to query
      final String fld = builder.fields[0];
      for (final String term : this.uniqueQueryTerms) {
        ((CommonTermsQuery) this.query).add(new Term(fld, term));
      }
      // Try to match all terms. Since we don't know the number of high/low
      // frequent terms we try to match all terms first. This may fail if there
      // are not enough boolean clauses so we have to relax a few times.
      ((CommonTermsQuery) this.query).setLowFreqMinimumNumberShouldMatch(
          (float) this.uniqueQueryTerms.size());
    } else {
      // generate a CommonTermsQuery for each field
      final Collection<Query> subQueries = new ArrayList<>(this.numberOfFields);
      Arrays.stream(builder.fields).forEach(f -> {
        final CommonTermsQuery ctQ = new CommonTermsQuery(
            Occur.SHOULD,
            CommonTermsDefaults.LFOP_DEFAULT,
            CommonTermsDefaults.MTF_DEFAULT
        );
        for (final String term : this.uniqueQueryTerms) {
          ctQ.add(new Term(f, term));
        }
        subQueries.add(ctQ);
      });

      // create a DisjunctionMaxQuery for all sub queries
      this.query = new DisjunctionMaxQuery(subQueries, 0.1f);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("RCTQ {} uQt={}", this.query, this.uniqueQueryTerms);
    }
  }

  /**
   * Reduce the number of terms that must match by one.
   *
   * @return True, if query was relaxed, false, if no terms left to relax the
   * query
   */
  @Override
  public boolean relax() {
    if (this.numberOfFields == 1) {
      // query is single CommonTermsQuery instance
      final float matchCount = ((CommonTermsQuery) this.query)
          .getLowFreqMinimumNumberShouldMatch();
      if (matchCount > 1f) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Relax to {}/{}", matchCount - 1f,
              this.uniqueQueryTerms.size());
        }
        ((CommonTermsQuery) this.query).setLowFreqMinimumNumberShouldMatch(
            matchCount - 1f);
        return true;
      }
    } else {
      ((DisjunctionMaxQuery) this.query).getDisjuncts().forEach(dj -> {
        final CommonTermsQuery q = (CommonTermsQuery) dj;
        if (LOG.isDebugEnabled()) {
          LOG.debug("OLD: hf={} lf={}",
              q.getHighFreqMinimumNumberShouldMatch(),
              q.getLowFreqMinimumNumberShouldMatch());
        }
        float mmf;
        // reset high freq matches
        mmf = q.getHighFreqMinimumNumberShouldMatch() - 1f;
        if (mmf <= 0f) {
          mmf = 1f;
        }
        q.setHighFreqMinimumNumberShouldMatch(mmf);
        // reset low freq matches
        mmf = q.getLowFreqMinimumNumberShouldMatch() - 1f;
        if (mmf <= 0f) {
          mmf = 1f;
        }
        q.setLowFreqMinimumNumberShouldMatch(mmf);
        if (LOG.isDebugEnabled()) {
          LOG.debug("NEW: hf={} lf={}",
              q.getHighFreqMinimumNumberShouldMatch(),
              q.getLowFreqMinimumNumberShouldMatch());
        }
      });
    }
    return false;
  }

  @Override
  public Query getQueryObj() {
    return this.query;
  }

  @Override
  public Collection<String> getQueryTerms() {
    return Collections.unmodifiableCollection(this.queryTerms);
  }

  /**
   * Builder to create a new {@link RelaxableCommonTermsQuery} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      implements Buildable<RelaxableCommonTermsQuery> {
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
    Occur lowFreqOccur = CommonTermsDefaults.LFOP_DEFAULT;
    /**
     * a value in [0..1) (or absolute number &gt;=1) representing the maximum
     * threshold of a terms document frequency to be considered a low frequency
     * term.
     */
    float maxTermFrequency = CommonTermsDefaults.MTF_DEFAULT;
    /**
     * Query string.
     */
    @Nullable
    String queryStr;
    /**
     * List of fields to query.
     */
    @Nullable
    String[] fields;

    @Override
    public RelaxableCommonTermsQuery build()
        throws BuildableException {
      validate();
      assert this.analyzer != null;
      return new RelaxableCommonTermsQuery(this);
    }

    @Override
    public void validate()
        throws ConfigurationException {
      if (this.analyzer == null) {
        throw new ConfigurationException("Analyzer not set.");
      }
      if (this.fields == null || this.fields.length == 0) {
        throw new ConfigurationException("No query fields supplied.");
      }
      if (StringUtils.isStrippedEmpty(this.queryStr)) {
        throw new ConfigurationException("Empty query string.");
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
    public Builder query(@NotNull final String... f) {
      this.fields = f.clone();
      return this;
    }


    /**
     * Set the query string.
     *
     * @param q Non empty query string
     * @return Self reference
     */
    public Builder query(@NotNull final String q) {
      this.queryStr = q;
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
