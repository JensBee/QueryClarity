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
package de.unihildesheim.lucene.query;

import de.unihildesheim.lucene.util.BytesWrap;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import org.apache.lucene.queryparser.classic.ParseException;

/**
 * Utilities to handle queries. Currently only simple term queries are
 * supported.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class QueryUtils {

  /**
   * Private constructor for utility class.
   */
  private QueryUtils() {
    // empty private constructor for utility class
  }

  /**
   * Break down a query to it's single terms.
   *
   * @param query Query to extract terms from
   * @return Term extracted from the query
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term
   * to UTF-8 fails
   * @throws org.apache.lucene.queryparser.classic.ParseException Thrown, if
   * query string could not be parsed
   */
  private static Collection<BytesWrap> extractTerms(final String query) throws
          UnsupportedEncodingException, ParseException {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("Query string was empty.");
    }
    final SimpleTermsQuery queryObj = TermsQueryBuilder.buildFromEnvironment(
            query);
    final Collection<String> qTerms = queryObj.getQueryTerms();

    if (qTerms.isEmpty()) {
      throw new IllegalStateException("Query string returned no terms.");
    }
    final Collection<BytesWrap> bwTerms = new ArrayList<>(qTerms.size());
    for (String qTerm : qTerms) {
      bwTerms.add(new BytesWrap(qTerm.getBytes("UTF-8")));
    }
    return bwTerms;
  }

  /**
   * Extract all unique terms from the query.
   *
   * @param query Query to extract terms from
   * @return Collection of terms from the query string
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term
   * to UTF-8 fails
   * @throws org.apache.lucene.queryparser.classic.ParseException Thrown, if
   * query string could not be parsed
   */
  public static Collection<BytesWrap> getUniqueQueryTerms(final String query)
          throws UnsupportedEncodingException, ParseException {
    return new HashSet<>(extractTerms(query));
  }

  /**
   * Extract all terms from the query.
   *
   * @param query Query to extract terms from
   * @return Collection of terms from the query string
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term
   * to UTF-8 fails
   * @throws org.apache.lucene.queryparser.classic.ParseException Thrown, if
   * query string could not be parsed
   */
  public static Collection<BytesWrap> getAllQueryTerms(final String query)
          throws UnsupportedEncodingException, ParseException {
    return extractTerms(query);
  }
}
