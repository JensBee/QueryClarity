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
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
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

  /**
   * Document fields to use.
   */
  private final Set<String> fields;
  /**
   * Reader to access Lucene index.
   */
  private final IndexReader reader;
  /**
   * Query analyzer.
   */
  private final Analyzer analyzer;

  /**
   * New instance with defined analyzer, reader and fields to query.
   *
   * @param newAnalyzer Query analyzer
   * @param newReader IndexReader
   * @param newFields Document fields to query
   */
  public QueryUtils(final Analyzer newAnalyzer,
      final IndexReader newReader,
      final Set<String> newFields) {
    Objects.requireNonNull(newReader, "IndexReader was null.");
    if (Objects.requireNonNull(newFields, "Fields were null.").isEmpty()) {
      throw new IllegalArgumentException("Fields list was empty.");
    }
    this.fields = new HashSet<>(newFields);
    this.reader = newReader;
    this.analyzer = newAnalyzer;
  }

  /**
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string.
   *
   * @param query Query string to tokenize
   * @param newAnalyzer Analyzer to use
   * @return Tokenized query string with stop-words removed
   */
  public static List<String> tokenizeQueryString(final String query,
      final Analyzer newAnalyzer) {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final List<String> result = new ArrayList<>();
    try (TokenStream stream = newAnalyzer.tokenStream(null, query)) {
      stream.reset();
      while (stream.incrementToken()) {
        result.add(stream.getAttribute(CharTermAttribute.class).toString());
      }
    } catch (final IOException e) {
      // not thrown b/c we're using a string reader
    }
    return result;
  }

  /**
   * Extract all unique terms from the query. Stopwords are not removed.
   *
   * @param query Query to parse
   * @return List of unique query terms extracted from the given query
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term to
   * UTF-8 fails
   */
  public static Set<ByteArray> getUniqueQueryTerms(
      final TermsProvidingQuery query)
      throws UnsupportedEncodingException {
    return new HashSet<>(extractTerms(query));
  }

  /**
   * Break down a query to it's single terms. Stopwords are not removed.
   *
   * @param query Query to extract terms from
   * @return Collection of all terms from the query string
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term to
   * UTF-8 fails
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  private static List<ByteArray> extractTerms(final TermsProvidingQuery query)
      throws UnsupportedEncodingException {
    assert query != null;

    final Collection<String> qTerms = query.getQueryTerms();

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
   * Extract all terms from the query. Stopwords are not removed.
   *
   * @param query Query to parse
   * @return Collection of all terms from the query string
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term to
   * UTF-8 fails
   */
  public static List<ByteArray> getAllQueryTerms(
      final TermsProvidingQuery query)
      throws UnsupportedEncodingException {
    return extractTerms(query);
  }

  /**
   * Extract all unique terms from the query. Stopwords are not removed.
   *
   * @param query Query to extract terms from
   * @return Collection of terms from the query string
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term to
   * UTF-8 fails
   * @throws Buildable.BuildException Thrown, if building the query object
   * failed
   * @throws Buildable.ConfigurationException Thrown, if building the query
   * object failed
   */
  public Set<ByteArray> getUniqueQueryTerms(final String query)
      throws UnsupportedEncodingException,
             Buildable.ConfigurationException, Buildable.BuildException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }
    return new HashSet<>(extractTerms(
        new SimpleTermsQuery.Builder(this.reader, this.fields)
            .analyzer(this.analyzer).query(query).build()
    ));
  }

  /**
   * Extract all terms from the query. Stopwords are not removed.
   *
   * @param query Query to extract terms from
   * @return Collection of all terms from the query string
   * @throws java.io.UnsupportedEncodingException Thrown, if encoding a term to
   * UTF-8 fails
   * @throws Buildable.BuildException Thrown, if building the query object
   * failed
   * @throws Buildable.ConfigurationException Thrown, if building the query
   * object failed
   */
  @SuppressWarnings("TypeMayBeWeakened")
  public List<ByteArray> getAllQueryTerms(final String query)
      throws UnsupportedEncodingException,
             Buildable.ConfigurationException, Buildable.BuildException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }
    return extractTerms(
        new SimpleTermsQuery.Builder(this.reader, this.fields)
            .analyzer(this.analyzer).query(query).build()
    );
  }
}
