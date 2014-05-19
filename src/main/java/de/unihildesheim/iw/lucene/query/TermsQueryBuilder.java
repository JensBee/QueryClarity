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
import de.unihildesheim.iw.lucene.index.IndexUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Query builder building a simple terms query.
 *
 * @author Jens Bertram
 */
public final class TermsQueryBuilder
    implements Buildable<SimpleTermsQuery> {

  /**
   * Collection of stop-words to use.
   */
  private Set<String> stopwords = Collections.<String>emptySet();

  /**
   * List of fields to query.
   */
  private Set<String> fields;

  /**
   * Boolean operator to use for joining query terms.
   */
  private QueryParser.Operator operator = SimpleTermsQuery.DEFAULT_OPERATOR;

  /**
   * Reader to access Lucene index.
   */
  private final IndexReader idxReader;

  /**
   * Query string.
   */
  private String query;

  public TermsQueryBuilder(final IndexReader reader,
      final Set<String> newFields) {
    Objects.requireNonNull(reader);
    if (Objects.requireNonNull(newFields).isEmpty()) {
      throw new IllegalArgumentException("Empty fields.");
    }
    this.idxReader = reader;
    this.fields = newFields;
  }

  /**
   * Set the list of stop-words to exclude from the final query object.
   *
   * @param newStopwords List of stop-words
   * @return Self reference
   */
  public TermsQueryBuilder setStopwords(final Set<String> newStopwords) {
    this.stopwords = new HashSet<>(Objects.requireNonNull(newStopwords));
    return this;
  }

  /**
   * Set the document fields that get queried.
   *
   * @param newFields List of fields to query
   * @return Self reference
   */
  public TermsQueryBuilder setFields(final Set<String> newFields) {
    if (Objects.requireNonNull(newFields).isEmpty()) {
      throw new IllegalArgumentException("Empty fields.");
    }
    IndexUtils.checkFields(this.idxReader, newFields);
    this.fields = new HashSet<>(newFields);
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
    this.operator = Objects.requireNonNull(newOperator);
    return this;
  }

  /**
   * Set the query string.
   *
   * @param queryStr Query string
   * @return Self reference
   */
  public TermsQueryBuilder query(final String queryStr) {
    if (Objects.requireNonNull(queryStr).trim().isEmpty()) {
      throw new IllegalArgumentException("Empty query string.");
    }
    this.query = queryStr;
    return this;
  }

  /**
   * Builds the instance.
   *
   * @return Query build using configured parameters and default options from
   * {@link SimpleTermsQuery} if they are missing and defaults are provided
   * @throws ParseException Thrown, if the query could not be parsed
   * @throws ConfigurationException Thrown, if any mandatory setting is left
   * unconfigured
   */
  @Override
  public SimpleTermsQuery build()
      throws ConfigurationException, BuildException {
    validate();
    try {
      return new SimpleTermsQuery(this.query, this.operator, this.fields,
          this.stopwords);
    } catch (ParseException e) {
      throw new BuildException(e);
    }
  }

  @Override
  public void validate()
      throws ConfigurationException {
    if (this.query == null || this.query.trim().isEmpty()) {
      throw new ConfigurationException("Query was empty.");
    }
    if (this.stopwords == null) {
      throw new ConfigurationException("No stopwords set.");
    }
    if (this.fields == null) {
      throw new ConfigurationException("No fields set.");
    }
  }
}
