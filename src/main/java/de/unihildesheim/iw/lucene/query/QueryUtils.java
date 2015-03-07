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

import de.unihildesheim.iw.lucene.index.CollectionMetrics;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string. The {@link CollectionMetrics} instance is
   * used to skip terms no found in the collection.
   *
   * @param query Query string to tokenize
   * @param qAnalyzer Analyzer to use
   * @param cMetrics Collection metrics to skip terms not in the collection. If
   * null all terms wll be included.
   * @return List of tokens from original query with stop-words removed
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public static BytesRefArray tokenizeQuery(final String query,
      final Analyzer qAnalyzer, @Nullable final CollectionMetrics cMetrics) {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    BytesRefArray result = new BytesRefArray(Counter.newCounter(false));

    try (TokenStream stream = qAnalyzer.tokenStream(null, query)) {
      stream.reset();
      while (stream.incrementToken()) {
        final BytesRef term = new BytesRef(
            stream.getAttribute(CharTermAttribute.class));
        if (term.length > 0) {
          result.append(term);
          //result.add(new ByteArray(term.getBytes(StandardCharsets.UTF_8)));
        }
      }
    } catch (final IOException e) {
      // not thrown b/c we're using a string reader
    }
    if (cMetrics != null) {
      result = removeUnknownTerms(cMetrics, result);
    }
    return result;
  }

  /**
   * Remove terms from the given collection, if they are not found in the
   * collection.
   *
   * @param cMetrics Metrics to access term frequency values
   * @param terms Collection of terms to check against the collection
   * @return Passed in terms with non-collection terms removed
   */
  private static BytesRefArray removeUnknownTerms(final CollectionMetrics
      cMetrics, final BytesRefArray terms) {
    final StringBuilder sb = new StringBuilder(
        "Skipped terms (stopword or not in collection): [");
    final FixedBitSet bits = new FixedBitSet(terms.size());
    final BytesRefBuilder spare = new BytesRefBuilder();
    BytesRef term;

    if (terms.size() == 0) {
      return terms;
    } else {
      for (int i = terms.size() - 1; i >= 0; i--) {
        term = terms.get(spare, i);
        if (cMetrics.tf(term) <= 0L) {
          sb.append(term.utf8ToString()).append(' ');
          bits.set(i);
        }
      }

      if (bits.cardinality() > 0) {
        LOG.warn(sb.toString().trim() + "].");
        final BytesRefArray cleanTerms = new BytesRefArray(
            Counter.newCounter(false));
        for (int i = terms.size() - 1; i >= 0; i--) {
          if (!bits.get(i)) {
            term = terms.get(spare, i);
            cleanTerms.append(term); // copies bytes
          }
        }
        return cleanTerms;
      }
      return terms;
    }
  }

  /**
   * Remove terms from the given collection, if they are not found in the
   * collection.
   *
   * @param cMetrics Metrics to access term frequency values
   * @param terms Collection of terms to check against the collection
   * @return Passed in terms with non-collection terms removed
   */
  private static Collection<BytesRef> removeUnknownTerms(final CollectionMetrics
      cMetrics, final Collection<BytesRef> terms) {
    return terms.stream()
        .filter(t -> (cMetrics.tf(t) <= 0L))
        .collect(Collectors.toList());
  }

  /**
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string.
   *
   * @param query Query string to tokenize
   * @param qAnalyzer Analyzer to use
   * @return Tokenized query string with stop-words removed CollectionMetrics}
   * fails
   */
  public static List<String> tokenizeQueryString(final String query,
      final Analyzer qAnalyzer) {
    return tokenizeQueryString(query, qAnalyzer, null);
  }

  /**
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string. The {@link CollectionMetrics} instance is
   * used to skip terms no found in the collection.
   *
   * @param query Query string to tokenize
   * @param qAnalyzer Analyzer to use
   * @param cMetrics Collection metrics to skip terms not in the collection. If
   * null all terms wll be included.
   * @return List of tokens from original query with stop-words removed
   * @see #tokenizeQuery(String, Analyzer, CollectionMetrics)
   */
  public static List<String> tokenizeQueryString(final String query,
      final Analyzer qAnalyzer, @Nullable final CollectionMetrics cMetrics) {
    final BytesRefArray tokenizedQuery = tokenizeQuery(query, qAnalyzer,
        cMetrics);
    final List<String> tokenizedQueryStr =
        new ArrayList<>(tokenizedQuery.size());
    tokenizedQueryStr.addAll(
        StreamUtils.stream(tokenizedQuery)
            .map(BytesRef::utf8ToString)
            .collect(Collectors.toList()));
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
   */
  public static Map<BytesRef, Integer> tokenizeAndMapQuery(final
  String query, final Analyzer qAnalyzer) {
    return tokenizeAndMapQuery(query, qAnalyzer, null);
  }

  /**
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string. Returns a mapping of query-term to
   * in-query-frequency. The {@link CollectionMetrics} instance is used to skip
   * terms no found in the collection.
   *
   * @param query Query String
   * @param qAnalyzer Analyzer used to parse the query String
   * @param cMetrics Collection metrics to skip terms not in the collection. If
   * null all terms wll be included.
   * @return mapping of query-term to in-query-frequency with optonally terms
   * not in the collection skipped
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public static Map<BytesRef, Integer> tokenizeAndMapQuery(final
  String query, final Analyzer qAnalyzer,
      @Nullable final CollectionMetrics cMetrics) {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<BytesRef, Integer> result = new HashMap<>();
    try (TokenStream stream = qAnalyzer.tokenStream(null, query)) {
      stream.reset();
      while (stream.incrementToken()) {
        final BytesRef term = new BytesRef(stream.getAttribute
            (CharTermAttribute.class));
        if (result.containsKey(term)) {
          result.put(BytesRef.deepCopyOf(term), result.get(term) + 1);
        } else {
          result.put(BytesRef.deepCopyOf(term), 1);
        }
      }
    } catch (final IOException e) {
      // not thrown b/c we're using a string reader
    }
    if (cMetrics != null) {
      removeUnknownTerms(cMetrics, result.keySet()).stream()
          .forEach(result::remove);
    }
    return result;
  }
}
