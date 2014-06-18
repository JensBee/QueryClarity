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

import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
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
public final class TryExactTermsQuery
    implements TermsProvidingQuery, RelaxableQuery {

  /**
   * Logger instance for this class.
   */
  static final Logger LOG = LoggerFactory.getLogger(
      TryExactTermsQuery.class);

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
   * New instance using the supplied query.
   *
   * @param analyzer Query analyzer
   * @param queryStr Query string
   * @param fields Fields to query
   * @throws ParseException Thrown, if the query could not be parsed
   */
  public TryExactTermsQuery(final Analyzer analyzer, final String queryStr,
      final Set<String> fields)
      throws ParseException {
    Objects.requireNonNull(analyzer, "Analyzer was null.");
    if (Objects.requireNonNull(fields, "Fields were null.").isEmpty()) {
      throw new IllegalArgumentException("Empty fields list.");
    }
    if (StringUtils.isStrippedEmpty(Objects.requireNonNull(queryStr,
        "Query was null."))) {
      throw new IllegalArgumentException("Empty query.");
    }

    this.queryTerms = QueryUtils.tokenizeQueryString(queryStr, analyzer);

    final QueryParser qParser = new MultiFieldQueryParser(
        LuceneDefaults.VERSION, fields.toArray(new String[fields.size()]),
        analyzer);

    this.query = new BooleanQuery();
    this.uniqueQueryTerms = new HashSet<>(this.queryTerms);
    for (final String term : this.uniqueQueryTerms) {
      @SuppressWarnings("ObjectAllocationInLoop")
      final BooleanClause bc = new BooleanClause(qParser.parse(term),
          BooleanClause.Occur.SHOULD);
      this.query.add(bc);
    }
    this.query.setMinimumNumberShouldMatch(this.uniqueQueryTerms.size());
    LOG.debug("TEQ {}", this.query);
  }

  /**
   * Get the query object.
   *
   * @return Query object
   */
  public BooleanQuery get() {
    return this.query;
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
      //noinspection HardcodedFileSeparator
      LOG.debug("Relax to {}/{}", matchCount - 1, this.uniqueQueryTerms.size());
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
  @Override
  public Query getQueryObj() {
    return this.query;
  }

  /**
   * Get the list of terms from the original query. Stop-words are removed.
   *
   * @return List of query terms with stop-words removed
   */
  @Override
  public Collection<String> getQueryTerms() {
    return Collections.unmodifiableCollection(this.queryTerms);
  }
}
