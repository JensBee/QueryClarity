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
import de.unihildesheim.iw.util.StringUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
   * New instance using settings from the supplied {@link Builder} instance.
   *
   * @param builder {@link Builder} Instance builder
   * @throws IOException Thrown on low-level i/o-errors
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  RelaxableCommonTermsQuery(@NotNull final Builder builder)
      throws IOException {
    // get all query terms
    assert builder.queryStr != null;
    assert builder.analyzer != null;
    this.queryTerms = QueryUtils.tokenizeQueryString(
        builder.queryStr, builder.analyzer);

    // list of unique terms contained in the query (stopped, analyzed)
    final String[] uniqueQueryTerms = this.queryTerms.stream()
        .distinct().toArray(String[]::new);
    final int uniqueTermsCount = uniqueQueryTerms.length;

    // heavily based on code from org.apache.lucene.queries.CommonTermsQuery
    assert builder.reader != null;
    final List<LeafReaderContext> leaves = builder.reader.leaves();
    final int maxDoc = builder.reader.maxDoc();
    TermsEnum termsEnum = null;
    final List<Query> subQueries = new ArrayList<>(10);

    assert builder.fields != null;
    for (final String field : builder.fields) {
      final TermContext[] tcArray = new TermContext[uniqueTermsCount];
      final BooleanQuery lowFreq = new BooleanQuery();
      final BooleanQuery highFreq = new BooleanQuery();

      // collect term statistics
      for (int i = 0; i < uniqueTermsCount; i++) {
        final Term term = new Term(field, uniqueQueryTerms[i]);
        for (final LeafReaderContext context : leaves) {
          final TermContext termContext = tcArray[i];
          final Fields fields = context.reader().fields();
          final Terms terms = fields.terms(field);
          if (terms != null) {
            // only, if field exists
            termsEnum = terms.iterator(termsEnum);
            if (termsEnum != TermsEnum.EMPTY) {
              if (termsEnum.seekExact(term.bytes())) {
                if (termContext == null) {
                  tcArray[i] = new TermContext(
                      builder.reader.getContext(),
                      termsEnum.termState(),
                      context.ord,
                      termsEnum.docFreq(),
                      termsEnum.totalTermFreq());
                } else {
                  termContext.register(
                      termsEnum.termState(),
                      context.ord,
                      termsEnum.docFreq(),
                      termsEnum.totalTermFreq());
                }
              }
            }
          }
        }

        // build query
        if (tcArray[i] == null) {
          lowFreq.add(new TermQuery(term), builder.lowFreqOccur);
        } else {
          if ((builder.maxTermFrequency >= 1f &&
              (float) tcArray[i].docFreq() > builder.maxTermFrequency) ||
              (tcArray[i].docFreq() > (int) Math.ceil(
                  (double) (builder.maxTermFrequency * (float) maxDoc)))) {
            highFreq.add(
                new TermQuery(term, tcArray[i]), builder.highFreqOccur);
          } else {
            lowFreq.add(
                new TermQuery(term, tcArray[i]), builder.lowFreqOccur);
          }
        }

        final int numLowFreqClauses = lowFreq.clauses().size();
        final int numHighFreqClauses = highFreq.clauses().size();
        if (builder.lowFreqOccur == Occur.SHOULD && numLowFreqClauses > 0) {
          lowFreq.setMinimumNumberShouldMatch(numLowFreqClauses);
        }
        if (builder.highFreqOccur == Occur.SHOULD && numHighFreqClauses > 0) {
          highFreq.setMinimumNumberShouldMatch(numHighFreqClauses);
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("qLF={}", lowFreq);
        LOG.debug("qHF={}", highFreq);
      }

      if (lowFreq.clauses().isEmpty()) {
        subQueries.add(highFreq);
      } else if (highFreq.clauses().isEmpty()) {
        subQueries.add(lowFreq);
      } else {
        final BooleanQuery query = new BooleanQuery(true); // final query
        query.add(highFreq, Occur.SHOULD);
        query.add(lowFreq, Occur.MUST);
        subQueries.add(query);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("qList={}", subQueries);
    }

    this.query = subQueries.size() == 1 ? subQueries.get(0) :
        new DisjunctionMaxQuery(subQueries, 0.1f);

    if (LOG.isDebugEnabled()) {
      LOG.debug("RCTQ {} uQt={}", this.query, uniqueQueryTerms);
    }
  }

  /**
   * Reduce the number of terms that must match by one.
   *
   * @return True, if query was relaxed, false, if no terms left to relax the
   * query
   */
  @SuppressWarnings("ReuseOfLocalVariable")
  @Override
  public boolean relax() {
    boolean relaxed = false;
    if (BooleanQuery.class.isInstance(this.query)) {
      // simple bool query
      final BooleanQuery q = (BooleanQuery) this.query;
      int mm = q.getMinimumNumberShouldMatch();
      if (mm >= 1) {
        if (mm == 1) {
          mm = 0;
        } else {
          mm--;
        }
        q.setMinimumNumberShouldMatch(mm);
        relaxed = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Relaxed single clause query to {}", mm);
        }
      }
    } else {
      // disMax
      final List<Query> dj = ((DisjunctionMaxQuery) this.query).getDisjuncts();
      for (final Query q : dj) {
        int mm = ((BooleanQuery) q).getMinimumNumberShouldMatch();
        if (mm >= 1) {
          if (mm == 1) {
            mm = 0;
          } else {
            mm--;
          }
          ((BooleanQuery) q).setMinimumNumberShouldMatch(mm);
          relaxed = true;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Relaxed a clause to {}", mm);
          }
        }
      }
    }
    return relaxed;
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
   * Builder to create a new {@link RelaxableCommonTermsQuery} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      implements Buildable<RelaxableCommonTermsQuery> {
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
     * List of fields to query.
     */
    @Nullable
    String[] fields;

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @NotNull
    @Override
    public RelaxableCommonTermsQuery build()
        throws BuildableException {
      validate();
      try {
        return new RelaxableCommonTermsQuery(this);
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
    public Builder query(@NotNull final String q) {
      this.queryStr = q;
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
