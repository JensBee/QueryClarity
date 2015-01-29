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

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.index.CollectionMetrics;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.util.ByteArrayUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      QueryUtils.class);
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
   * stopwords from the query string. The {@link CollectionMetrics}
   * instance is used to skip terms no found in the collection.
   *
   * @param query Query string to tokenize
   * @param qAnalyzer Analyzer to use
   * @param cMetrics Collection metrics to skip terms not in the collection. If
   * null all terms wll be included.
   * @return List of tokens from original query with stop-words removed
   * @throws DataProviderException Thrown, if accessing {@link
   * CollectionMetrics} fails
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public static List<ByteArray> tokenizeQuery(final String query,
      final Analyzer qAnalyzer, @Nullable final CollectionMetrics cMetrics)
      throws DataProviderException {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final List<ByteArray> result = new ArrayList<>();
    try (TokenStream stream = qAnalyzer.tokenStream(null, query)) {
      stream.reset();
      while (stream.incrementToken()) {
        result.add(new ByteArray(stream.getAttribute(CharTermAttribute.class)
            .toString().getBytes(StandardCharsets.UTF_8)));
      }
    } catch (final IOException e) {
      // not thrown b/c we're using a string reader
    }
    if (cMetrics != null) {
      removeUnknownTerms(cMetrics, result);
    }
    return result;
  }

  /**
   * Remove terms from the given collection, if they are not found in the
   * collection.
   *
   * @param cMetrics Metrics to access term frequency values
   * @param terms Collection of terms to check against the collection
   * @throws DataProviderException Thrown, if accessing {@link
   * CollectionMetrics} fails
   */
  private static void removeUnknownTerms(final CollectionMetrics
      cMetrics, final Iterable<ByteArray> terms)
      throws DataProviderException {
    final Iterator<ByteArray> termsIt = terms.iterator();
    final StringBuilder sb = new StringBuilder(
        "Skipped terms (stopword or not in collection): [");
    boolean removed = false;
    while (termsIt.hasNext()) {
      final ByteArray term = termsIt.next();
      if (cMetrics.tf(term) != null && cMetrics.tf(term) <= 0L) {
        sb.append(ByteArrayUtils.utf8ToString(term)).append(' ');
        termsIt.remove();
        removed = true;
      }
    }
    if (removed) {
      LOG.warn(sb.toString().trim() + "].");
    }
  }

  /**
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string.
   *
   * @param query Query string to tokenize
   * @param qAnalyzer Analyzer to use
   * @return Tokenized query string with stop-words removed
   * @throws DataProviderException Thrown, if accessing {@link
   * CollectionMetrics} fails
   */
  public static List<String> tokenizeQueryString(final String query,
      final Analyzer qAnalyzer)
      throws DataProviderException {
    return tokenizeQueryString(query, qAnalyzer, null);
  }

  /**
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string. The {@link CollectionMetrics}
   * instance is used to skip terms no found in the collection.
   *
   * @param query Query string to tokenize
   * @param qAnalyzer Analyzer to use
   * @param cMetrics Collection metrics to skip terms not in the collection. If
   * null all terms wll be included.
   * @return List of tokens from original query with stop-words removed
   * @see #tokenizeQuery(String, Analyzer, CollectionMetrics)
   * @throws DataProviderException Thrown, if accessing {@link
   * CollectionMetrics} fails
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public static List<String> tokenizeQueryString(final String query,
      final Analyzer qAnalyzer, @Nullable final CollectionMetrics cMetrics)
      throws DataProviderException {
    final List<ByteArray> tokenizedQuery = tokenizeQuery(query, qAnalyzer,
        cMetrics);
    final List<String> tokenizedQueryStr = new ArrayList<>(tokenizedQuery.size());
    for (final ByteArray ba : tokenizedQuery) {
      tokenizedQueryStr.add(ByteArrayUtils.utf8ToString(ba));
    }
    return tokenizedQueryStr;
  }

  /**
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string. Returns a mapping of query-term to
   * in-query-frequency.
   *
   * @param query Query string
   * @param qAnalyzer Analyzer used to parse the query String
   * @return mapping of query-term to in-query-frequency
   * @see #tokenizeAndMapQuery(String, Analyzer, CollectionMetrics)
   * @throws DataProviderException Thrown, if accessing {@link
   * CollectionMetrics} fails
   */
  public static Map<ByteArray, Integer> tokenizeAndMapQuery(final
  String query, final Analyzer qAnalyzer)
      throws DataProviderException {
    return tokenizeAndMapQuery(query, qAnalyzer, null);
  }

  /**
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string. Returns a mapping of query-term to
   * in-query-frequency. The {@link CollectionMetrics} instance is used
   * to skip terms no found in the collection.
   *
   * @param query Query String
   * @param qAnalyzer Analyzer used to parse the query String
   * @param cMetrics Collection metrics to skip terms not in the collection. If
   * null all terms wll be included.
   * @return mapping of query-term to in-query-frequency with optonally terms
   * not in the collection skipped
   * @throws DataProviderException Thrown, if accessing {@link
   * CollectionMetrics} fails
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public static Map<ByteArray, Integer> tokenizeAndMapQuery(final
  String query, final Analyzer qAnalyzer,
      @Nullable final CollectionMetrics cMetrics)
      throws DataProviderException {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<ByteArray, Integer> result = new HashMap<>();
    try (TokenStream stream = qAnalyzer.tokenStream(null, query)) {
      stream.reset();
      while (stream.incrementToken()) {
        final ByteArray term = new ByteArray(stream.getAttribute
            (CharTermAttribute.class).toString()
            .getBytes(StandardCharsets.UTF_8));
        if (result.containsKey(term)) {
          result.put(term, result.get(term) + 1);
        } else {
          result.put(term, 1);
        }
      }
    } catch (final IOException e) {
      // not thrown b/c we're using a string reader
    }
    if (cMetrics != null) {
      removeUnknownTerms(cMetrics, result.keySet());
    }
    return result;
  }

  /**
   * Break down a query to it's single terms. Stopwords are not removed.
   *
   * @param query Query to extract terms from
   * @return Collection of all terms from the query string
   * @deprecated
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  private static List<ByteArray> extractTerms(final TermsProvidingQuery query) {
    assert query != null;

    final Collection<String> qTerms = query.getQueryTerms();

    if (qTerms.isEmpty()) {
      throw new IllegalStateException("Query string returned no terms.");
    }
    final List<ByteArray> bwTerms = new ArrayList<>(qTerms.size());
    for (final String qTerm : qTerms) {
      final ByteArray termBa =
          new ByteArray(qTerm.getBytes(StandardCharsets.UTF_8));
      bwTerms.add(termBa);
    }
    return bwTerms;
  }
}
