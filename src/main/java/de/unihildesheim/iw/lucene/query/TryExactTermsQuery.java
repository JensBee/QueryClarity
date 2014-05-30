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
import java.util.Set;

/**
 * @author Jens Bertram
 */
public class TryExactTermsQuery
    implements TermsProvidingQuery {

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

  public TryExactTermsQuery(Analyzer analyzer, final String query,
      final Set<String> fields)
      throws ParseException {

    this.queryTerms = new SimpleTermsQuery(analyzer, query,
        SimpleTermsQuery.DEFAULT_OPERATOR, fields).getQueryTerms();

    final QueryParser qParser = new MultiFieldQueryParser(
        LuceneDefaults.VERSION, fields.toArray(new String[fields.size()]),
        analyzer);

    this.query = new BooleanQuery();
    this.uniqueQueryTerms = new HashSet<>(this.queryTerms);
    for (final String term : this.uniqueQueryTerms) {
      this.query.add(new BooleanClause(qParser.parse(term),
          BooleanClause.Occur.SHOULD));
    }
    this.query.setMinimumNumberShouldMatch(this.uniqueQueryTerms.size());
    LOG.debug("TEQ {}", this.query.toString());
  }

  public BooleanQuery get() {
    return this.query;
  }

  public boolean relax() {
    final int matchCount = this.query.getMinimumNumberShouldMatch();
    if (matchCount > 1) {
      LOG.debug("Relax to {}/{}", matchCount - 1, this.uniqueQueryTerms.size());
      this.query.setMinimumNumberShouldMatch(matchCount - 1);
      return true;
    }
    return false;
  }

  @Override
  public Collection<String> getQueryTerms() {
    return Collections.unmodifiableCollection(this.queryTerms);
  }

  @Override
  public Query getQueryObj() {
    return this.query;
  }
}
