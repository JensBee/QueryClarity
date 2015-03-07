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

import de.unihildesheim.iw.lucene.CommonTermsDefaults;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Relaxable implementation of a {@link RelaxableCommonTermsQuery}.
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class RelaxableCommonTermsQuery
    extends RelaxableQuery implements TermsProvidingQuery  {

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
   * New instance using the supplied query.
   *
   * @param analyzer Query analyzer
   * @param queryStr Query string
   * @param fields Fields to query
   */
  public RelaxableCommonTermsQuery(final Analyzer analyzer,
      final String queryStr,
      final String[] fields) {
    super(analyzer, queryStr, fields);
    Objects.requireNonNull(analyzer, "Analyzer was null.");
    if (Objects.requireNonNull(fields, "Fields were null.").length == 0) {
      throw new IllegalArgumentException("Empty fields list.");
    }
    if (StringUtils.isStrippedEmpty(Objects.requireNonNull(queryStr,
        "Query was null."))) {
      throw new IllegalArgumentException("Empty query.");
    }

    // get all query terms
    this.queryTerms = QueryUtils.tokenizeQueryString(queryStr, analyzer);
    // list of unique terms contained in the query (stopped, analyzed)
    this.uniqueQueryTerms = new HashSet<>(this.queryTerms);

    this.numberOfFields = fields.length;
    if (this.numberOfFields == 1) {
      // query spans only one field so use simply a single CommonTermsQuery.

      this.query = new CommonTermsQuery(
          Occur.SHOULD,
          CommonTermsDefaults.LFOP_DEFAULT,
          CommonTermsDefaults.MTF_DEFAULT
      );

      // add terms to query
      final String fld = fields[0];
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
      Arrays.stream(fields).forEach(f -> {
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
    LOG.debug("RCTQ {} uQt={}", this.query, this.uniqueQueryTerms);
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
        LOG.debug("Relax to {}/{}", matchCount - 1f,
            this.uniqueQueryTerms.size());
        ((CommonTermsQuery) this.query).setLowFreqMinimumNumberShouldMatch(
            matchCount - 1f);
        return true;
      }
    } else {
      LOG.warn("Relaxion for MultiField queries currently not implemented!");
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
}
