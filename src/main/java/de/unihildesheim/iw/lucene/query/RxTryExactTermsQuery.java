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

package de.unihildesheim.iw.lucene.query;

import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.BytesRefArray;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Query builder that tries to match all terms from a given query. A {@link
 * #relax()} method is provided to stepwise reduce the number of terms that must
 * match in a document. <br> Example: If a query "Lucene in action" is provided
 * at first all three terms must match. If {@link #relax()} was called only two
 * terms of the original query must match and so on.
 *
 * @author Jens Bertram
 */
public final class RxTryExactTermsQuery
    implements TermsProvidingQuery, RelaxableQuery {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      RxTryExactTermsQuery.class);

  /**
   * Final query object.
   */
  private final BooleanQuery query;

  /**
   * List of terms contained in the query (stopped, analyzed).
   */
  private final Collection<String> queryTerms;
  /**
   * List of unique terms contained in the query (stopped, analyzed).
   */
  private final Set<String> uniqueQueryTerms;

  /**
   * New instance using the supplied query terms.
   *
   * @param analyzer Query analyzer
   * @param queryTerms Query terms
   * @param fields Fields to query
   * @throws ParseException Thrown, if the query could not be parsed
   */
  public RxTryExactTermsQuery(
      @NotNull final Analyzer analyzer,
      @NotNull final Collection<String> queryTerms,
      @NotNull final String... fields)
      throws ParseException {
    if (fields.length == 0) {
      throw new IllegalArgumentException("Empty fields list.");
    }

    if (queryTerms.isEmpty()) {
      throw new IllegalArgumentException("Empty query.");
    }

    this.queryTerms = new ArrayList<>(queryTerms.size());
    this.queryTerms.addAll(queryTerms);

    final QueryParser qParser = new MultiFieldQueryParser(fields, analyzer);

    this.query = new BooleanQuery();
    this.uniqueQueryTerms = new HashSet<>(this.queryTerms);
    for (final String term : this.uniqueQueryTerms) {
      @SuppressWarnings("ObjectAllocationInLoop")
      final BooleanClause bc = new BooleanClause(
          qParser.parse(QueryParserBase.escape(term)),
          Occur.SHOULD);
      this.query.add(bc);
    }
    this.query.setMinimumNumberShouldMatch(this.uniqueQueryTerms.size());
    if (LOG.isDebugEnabled()) {
      LOG.debug("TEQ {} uQt={}", this.query, this.uniqueQueryTerms);
    }
  }

  /**
   * New instance using the supplied query terms.
   *
   * @param analyzer Query analyzer
   * @param queryTerms Query terms
   * @param fields Fields to query
   * @throws ParseException Thrown, if the query could not be parsed
   * @throws IOException Thrown on low-level i/o errors
   */
  public RxTryExactTermsQuery(
      @NotNull final Analyzer analyzer,
      @NotNull final BytesRefArray queryTerms,
      @NotNull final String... fields)
      throws IOException, ParseException {
    this(analyzer, BytesRefUtils.arrayToCollection(queryTerms), fields);
  }

  /**
   * New instance using the supplied query.
   *
   * @param analyzer Query analyzer
   * @param queryStr Query string
   * @param fields Fields to query
   * @throws ParseException Thrown, if the query could not be parsed
   */
  public RxTryExactTermsQuery(
      @NotNull final Analyzer analyzer,
      @NotNull final String queryStr,
      @NotNull final String... fields)
      throws ParseException {
    this(analyzer, QueryUtils.tokenizeQueryString(queryStr, analyzer), fields);
  }

  /**
   * Reduce the number of terms that must match by one.
   *
   * @return True, if query was relaxed, false, if no terms left to relax the
   * query
   */
  @Override
  public boolean relax() {
    final int matchCount = this.query.getMinimumNumberShouldMatch();
    if (matchCount > 1) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Relax to {}/{}", matchCount - 1,
            this.uniqueQueryTerms.size());
      }
      this.query.setMinimumNumberShouldMatch(matchCount - 1);
      return true;
    }
    return false;
  }

  /**
   * Get the query object. This object is directly changed on each call to
   * {@link #relax()}, so there's no need to re-get it every time after relaxing
   * the query.
   *
   * @return Current query object
   */
  @NotNull
  @Override
  public BooleanQuery getQueryObj() {
    return this.query;
  }

  /**
   * Get the list of terms from the original query. Stop-words are removed.
   *
   * @return List of query terms with stop-words removed
   */
  @NotNull
  @Override
  public Collection<String> getQueryTerms() {
    return Collections.unmodifiableCollection(this.queryTerms);
  }
}