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

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.index.IndexUtils;
import java.util.Collection;
import java.util.HashSet;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;

/**
 * Builder building a simple terms query. This relies on the
 * {@link Environment} to validate some data. Further the {@link Environment}
 * may be used to get default values for some query parameters.
 *
 * @author Jens Bertram
 */
public final class TermsQueryBuilder {

  /**
   * Collection of stop-words to use.
   */
  private Collection<String> stopWords = null;
  /**
   * List of fields to query.
   */
  private String[] fields = null;
  /**
   * Boolean operator to use for joining query terms.
   */
  private QueryParser.Operator operator;

  /**
   * Set the list of stop-words to exclude from the final query object.
   *
   * @param newStopWords List of stop-words
   * @return Self reference
   */
  public TermsQueryBuilder setStopWords(final Collection<String> newStopWords) {
    this.stopWords = new HashSet<>(newStopWords.size());
    this.stopWords.addAll(newStopWords);
    return this;
  }

  /**
   * Set the document fields that get queried.
   *
   * @param newFields List of fields to query
   * @return Self reference
   * @throws de.unihildesheim.lucene.Environment.NoIndexException Thrown, if
   * no index is provided in the {@link Environment}
   */
  public TermsQueryBuilder setFields(final String[] newFields) throws
          Environment.NoIndexException {
    IndexUtils.checkFields(newFields);
    this.fields = newFields.clone();
    return this;
  }

  /**
   * Set the boolean operator to combine single terms.
   *
   * @param newOperator Boolean operator
   * @return Self reference
   */
  public TermsQueryBuilder setBoolOperator(
          final QueryParser.Operator newOperator) {
    this.operator = newOperator;
    return this;
  }

  /**
   * Simple validation of the provided query string. Throws an exception if
   * the query is invalid.
   *
   * @param query Query string
   */
  private static void checkQueryString(final String query) {
    if (query == null || query.trim().isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }
  }

  /**
   * Build the query using all parameters from the {@link Environment}.
   *
   * @param query Query string
   * @return Query build using default options from {@link SimpleTermsQuery}
   * and parameters from the {@link Environment}
   * @throws ParseException Thrown, if the query could not be parsed
   */
  public static SimpleTermsQuery buildFromEnvironment(final String query)
          throws
          ParseException {
    checkQueryString(query);
    return new SimpleTermsQuery(query, SimpleTermsQuery.DEFAULT_OPERATOR,
            Environment.getFields(), Environment.getStopwords());
  }

  /**
   * Build the query using the configured parameters and filling missing ones
   * from the {@link Environment}.
   *
   * @param query Query string
   * @return Query build using configured parameters and default options from
   * {@link SimpleTermsQuery} and parameters from the {@link Environment} if
   * they are missing
   * @throws ParseException Thrown, if the query could not be parsed
   */
  public SimpleTermsQuery buildUsingEnvironment(final String query) throws
          ParseException {
    checkQueryString(query);
    Collection<String> finalStopWords = this.stopWords;
    if (finalStopWords == null) {
      finalStopWords = Environment.getStopwords();
    }

    String[] finalFields = this.fields;
    if (finalFields == null || finalFields.length == 0) {
      finalFields = Environment.getFields();
    }

    QueryParser.Operator finalOperator = this.operator;
    if (finalOperator == null) {
      finalOperator = SimpleTermsQuery.DEFAULT_OPERATOR;
    }
    return new SimpleTermsQuery(query, finalOperator, finalFields,
            finalStopWords);
  }

  /**
   * Build the query using all configured parameters. If a required parameter
   * is left unconfigured an Exception will be thrown.
   *
   * @param query Query string
   * @return Query build using configured parameters and default options from
   * {@link SimpleTermsQuery} if they are missing and defaults are provided
   * @throws ParseException Thrown, if the query could not be parsed
   */
  public SimpleTermsQuery build(final String query) throws ParseException {
    checkQueryString(query);
    if (this.stopWords == null) {
      throw new IllegalStateException("No stopwords set.");
    }
    if (this.fields == null) {
      throw new IllegalStateException("No fields set.");
    }

    final QueryParser.Operator finalOperator;
    if (this.operator == null) {
      finalOperator = SimpleTermsQuery.DEFAULT_OPERATOR;
    } else {
      finalOperator = this.operator;
    }
    return new SimpleTermsQuery(query, finalOperator, this.fields,
            this.stopWords);
  }
}
