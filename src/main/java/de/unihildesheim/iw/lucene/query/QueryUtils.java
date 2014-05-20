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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.ByteArray;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Utilities to handle queries. Currently only simple term queries are
 * supported.
 *
 * @author Jens Bertram
 */
public final class QueryUtils {


  private final Set<String> fields;
  private final IndexReader reader;

  public QueryUtils(final IndexReader newReader, final Set<String> newFields) {
    Objects.requireNonNull(newReader, "IndexReader was null.");
    if (Objects.requireNonNull(newFields, "Fields were null.").isEmpty()) {
      throw new IllegalArgumentException("Fields list was empty.");
    }
    this.fields = newFields;
    this.reader = newReader;
  }

  /**
   * Break down a query to it's single terms.
   *
   * @param query Query to extract terms from
   * @return Term extracted from the query
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term to
   * UTF-8 fails
   * @throws org.apache.lucene.queryparser.classic.ParseException Thrown, if
   * query string could not be parsed
   */
  private List<ByteArray> extractTerms(final String query)
      throws
      UnsupportedEncodingException, ParseException,
      Buildable.ConfigurationException, Buildable.BuildException {
    assert query != null && !query.isEmpty();

    final Collection<String> qTerms =
        new TermsQueryBuilder(this.reader, this.fields).query(query).build()
            .getQueryTerms();

    if (qTerms.isEmpty()) {
      throw new IllegalStateException("Query string returned no terms.");
    }
    final List<ByteArray> bwTerms = new ArrayList<>(qTerms.size());
    for (final String qTerm : qTerms) {
      final ByteArray termBa = new ByteArray(qTerm.getBytes("UTF-8"));
      bwTerms.add(termBa);
    }
    return bwTerms;
  }

  /**
   * Extract all unique terms from the query.
   *
   * @param query Query to extract terms from
   * @return Collection of terms from the query string
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term to
   * UTF-8 fails
   * @throws org.apache.lucene.queryparser.classic.ParseException Thrown, if
   * query string could not be parsed
   */
  public Set<ByteArray> getUniqueQueryTerms(final String query)
      throws UnsupportedEncodingException, ParseException,
             Buildable.ConfigurationException, Buildable.BuildException {
    if (Objects.requireNonNull(query, "Query was null.").trim().isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }
    return new HashSet<>(extractTerms(query));
  }

  /**
   * Extract all terms from the query.
   *
   * @param query Query to extract terms from
   * @return Collection of terms from the query string
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term to
   * UTF-8 fails
   * @throws org.apache.lucene.queryparser.classic.ParseException Thrown, if
   * query string could not be parsed
   */
  public List<ByteArray> getAllQueryTerms(final String query)
      throws UnsupportedEncodingException, ParseException,
             Buildable.ConfigurationException, Buildable.BuildException {
    if (Objects.requireNonNull(query, "Query was null.").trim().isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }
    return extractTerms(query);
  }
}
