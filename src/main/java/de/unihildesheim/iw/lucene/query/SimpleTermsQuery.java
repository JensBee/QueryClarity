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
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexUtils;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Simple term query using a {@link org.apache.lucene.queryparser.classic
 * .MultiFieldQueryParser} under the hood.
 *
 * @author Jens Bertram
 */
public final class SimpleTermsQuery
    implements TermsProvidingQuery {

  /**
   * Default boolean operator to use for concatenating terms.
   */
  public static final Operator DEFAULT_OPERATOR
      = Operator.OR;

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      SimpleTermsQuery.class);

  /**
   * Final query generated by {@link MultiFieldQueryParser}.
   */
  private final Query queryObj;

  /**
   * Collection of all terms from the query (stop-words removed).
   */
  private final List<String> queryTerms;

  /**
   * Create a new simple term query using a builder.
   *
   * @param builder Builder
   * @throws ParseException Thrown if there were errors parsing the query
   * string
   * @see #SimpleTermsQuery(Analyzer, String, Operator, Set)
   */
  SimpleTermsQuery(final Builder builder)
      throws ParseException, DataProviderException {
    this(
        Objects.requireNonNull(builder, "Builder was null").analyzer,
        builder.query, builder.getOperator(), builder.fields
    );
  }

  /**
   * Create a new simple term query.
   *
   * @param analyzer Analyzer for parsing the query
   * @param query Query string
   * @param operator Default boolean operator to use
   * @param fields Document fields to include
   * @throws ParseException Thrown if there were errors parsing the query
   * string
   */
  public SimpleTermsQuery(final Analyzer analyzer, final String query,
      final Operator operator, final Set<String> fields)
      throws ParseException, DataProviderException {
    Objects.requireNonNull(analyzer, "Analyzer was null.");
    Objects.requireNonNull(operator, "Operator was null.");
    if (Objects.requireNonNull(fields, "Fields were null.").isEmpty()) {
      throw new IllegalArgumentException("Empty fields list.");
    }
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Empty query.");
    }

    // generate a local list of query terms using the provided Analyzer
    this.queryTerms = QueryUtils.tokenizeQueryString(query, analyzer);
    // TODO: need test-case
    if (this.queryTerms.isEmpty()) {
      throw new ParseException("Stopped query is empty.");
    }

    final QueryParser qParser = new MultiFieldQueryParser(
        fields.toArray(new String[fields.size()]), analyzer);

    LOG.debug("STQ userQuery={}", query);
    LOG.debug("STQ analyzer={} tokens={}",
        analyzer.getClass().getSimpleName(), this.queryTerms);
    LOG.debug("STQ tokens={} operator={} fields={}",
        this.queryTerms.size(), operator, fields);

    qParser.setDefaultOperator(operator);
    this.queryObj = qParser.parse(QueryParserUtil.escape(query));
    LOG.debug("STQ Q={}", this.queryObj);
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

  /**
   * Get the query object. (stopped)
   *
   * @return Query object
   */
  public Query getQueryObj() {
    return this.queryObj;
  }

  /**
   * Query builder building a simple terms query.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      implements Buildable<SimpleTermsQuery> {
    /**
     * Reader to access Lucene index.
     */
    private final IndexReader idxReader;

    /**
     * Analyzer to use for parsing queries.
     */
    @SuppressWarnings("PackageVisibleField")
    Analyzer analyzer;

    /**
     * List of fields to query.
     */
    @SuppressWarnings("PackageVisibleField")
    Set<String> fields;

    /**
     * Query string.
     */
    @SuppressWarnings("PackageVisibleField")
    String query;

    /**
     * Initializes the Builder.
     *
     * @param reader IndexReader to access the Lucene index
     * @param newFields List of document fields to query
     */
    public Builder(final IndexReader reader,
        final Set<String> newFields) {
      Objects.requireNonNull(reader, "IndexReader was null.");
      if (Objects.requireNonNull(newFields, "Fields were null.").isEmpty()) {
        throw new IllegalArgumentException("Empty fields.");
      }
      this.idxReader = reader;
      this.fields = new HashSet<>(newFields);
    }

    /**
     * Set the analyzer to use for parsing queries.
     *
     * @param newAnalyzer Analyzer
     * @return Self reference
     */
    public Builder analyzer(final Analyzer newAnalyzer) {
      this.analyzer = newAnalyzer;
      return this;
    }

    /**
     * Set the document fields that get queried.
     *
     * @param newFields List of fields to query
     * @return Self reference
     */
    public Builder fields(final Set<String> newFields) {
      if (Objects.requireNonNull(newFields, "Fields were null.").isEmpty()) {
        throw new IllegalArgumentException("Empty fields.");
      }
      IndexUtils.checkFields(this.idxReader, newFields);
      this.fields = new HashSet<>(newFields);
      return this;
    }

    /**
     * Boolean operator to use for joining query terms.
     */
    private Operator operator = DEFAULT_OPERATOR;

    /**
     * Set the boolean operator to combine single terms.
     *
     * @param newOperator Boolean operator
     * @return Self reference
     */
    public Builder boolOperator(
        final Operator newOperator) {
      this.operator = Objects.requireNonNull(newOperator, "Operator was null.");
      return this;
    }

    /**
     * Set the query string.
     *
     * @param queryStr Query string
     * @return Self reference
     */
    public Builder query(final String queryStr) {
      if (Objects.requireNonNull(queryStr, "Query string was null.").trim()
          .isEmpty()) {
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
     * @throws ConfigurationException Thrown, if any mandatory setting is left
     * unconfigured
     */
    @Override
    public SimpleTermsQuery build()
        throws ConfigurationException, BuildException {
      validate();
      try {
        return new SimpleTermsQuery(this);
      } catch (final ParseException | DataProviderException e) {
        throw new BuildException(e);
      }
    }

    @Override
    public void validate()
        throws ConfigurationException {
      if (this.query == null || StringUtils.isStrippedEmpty(this.query)) {
        throw new ConfigurationException("Query was empty.");
      }
      if (this.fields == null) {
        throw new ConfigurationException("No fields set.");
      }
      if (this.analyzer == null) {
        throw new ConfigurationException("No query analyzer set.");
      }
    }

    /**
     * Get the boolean operator to use for joining query terms.
     *
     * @return Boolean operator
     */
    public Operator getOperator() {
      return this.operator;
    }


  }
}
