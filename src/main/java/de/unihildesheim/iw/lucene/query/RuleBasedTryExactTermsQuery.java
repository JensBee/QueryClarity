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
import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.Metrics;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil;
import org.apache.lucene.search.Query;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Query builder that tries to match all terms from a given query. A {@link
 * #relax()} method is provided to stepwise reduce the number of terms that must
 * match in a document. Relaxing the query is done by a specific {@link
 * RelaxRule}, which decides which term will be removed from the query. <br>
 * Example: If a query "Lucene in action" is provided at first all three terms
 * must match. If {@link #relax()} was called one term is removed and only two
 * remaining terms of the original query must match and so on.
 *
 * @author Jens Bertram
 */
public final class RuleBasedTryExactTermsQuery
    implements TermsProvidingQuery, RelaxableQuery {
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      RuleBasedTryExactTermsQuery.class);
  /**
   * List of unique terms contained in the query (stopped, analyzed).
   */
  private final List<String> uniqueQueryTerms;
  /**
   * Query parser used to (re-)parse query strings.
   */
  private final QueryParser qParser;
  /**
   * Cache of term to document- or term-frequency value.
   */
  private final Map<ByteArray, Long> termFreqCache;
  /**
   * Query relaxing rule to use.
   */
  private final RelaxRule relaxRule;
  /**
   * Final query generated by {@link MultiFieldQueryParser}.
   */
  private Query queryObj;

  /**
   * @param dataProv DataProvider for accessing statistical data
   * @param analyzer Query analyzer to use
   * @param query Query string
   * @param operator Boolean operator used to chain query terms
   * @param newFields Fields to query
   * @param rule Query simplifying rule
   * @throws ParseException Thrown, if the query could not be parsed
   */
  public RuleBasedTryExactTermsQuery(final IndexDataProvider dataProv,
      final Analyzer analyzer, final String query,
      final QueryParser.Operator operator, final Set<String> newFields,
      final RelaxRule rule)
      throws ParseException, DataProviderException {
    Objects.requireNonNull(dataProv, "DataProvider was null.");
    Objects.requireNonNull(analyzer, "Analyzer was null.");
    Objects.requireNonNull(operator, "Operator was null.");
    Objects.requireNonNull(rule, "Relax-rule was null.");
    if (Objects.requireNonNull(newFields, "Fields were null.").isEmpty()) {
      throw new IllegalArgumentException("Empty fields list.");
    }
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Empty query.");
    }

    // fields to query.
    final Set<String> fields = new HashSet<>(newFields);

    // generate a local list of query terms using the provided Analyzer
    final List<String> queryTerms = QueryUtils.tokenizeQueryString(query,
        analyzer);
    // TODO: need test-case
    if (queryTerms.isEmpty()) {
      throw new ParseException("Stopped query is empty.");
    }

    // list, not set, because order matters
    this.uniqueQueryTerms = new ArrayList<>(queryTerms.size());
    for (final String qTerm : queryTerms) {
      if (!this.uniqueQueryTerms.contains(qTerm)) {
        this.uniqueQueryTerms.add(qTerm);
      }
    }

    this.qParser = new MultiFieldQueryParser(
        LuceneDefaults.VERSION, newFields.toArray(new String[fields.size
        ()]), analyzer);

    this.qParser.setDefaultOperator(operator);
    this.queryObj = this.qParser.parse(QueryParserUtil.escape(query));

    // create a frequency cache, if needed
    if (RelaxRule.HIGHEST_TERMFREQ == rule ||
        RelaxRule.HIGHEST_DOCFREQ == rule) {
      this.termFreqCache = new HashMap<>(this.uniqueQueryTerms.size());
      final Metrics metrics = new Metrics(dataProv);

      for (final String term : this.uniqueQueryTerms) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final ByteArray termBa =
            new ByteArray(term.getBytes(StandardCharsets.UTF_8));
        if (RelaxRule.HIGHEST_TERMFREQ == rule) {
          // TODO may be null!
          this.termFreqCache.put(termBa, dataProv.metrics().tf(termBa));
        } else {
          this.termFreqCache
              .put(termBa, dataProv.metrics().df(termBa).longValue());
        }
      }
    } else {
      this.termFreqCache = Collections.emptyMap();
    }

    this.relaxRule = rule;
  }

  /**
   * Get the terms of the last query that was generated. Stopwords are removed
   * and terms are unique.
   *
   * @return Query terms
   */
  @Override
  public Collection<String> getQueryTerms() {
    return Collections.unmodifiableCollection(this.uniqueQueryTerms);
  }

  /**
   * Reduce the number of terms that must match by one.
   *
   * @return True, if query was relaxed, false, if no terms left to relax the
   * query
   * @throws ParseException Thrown on error parsing the newly generated query
   * string
   */
  @Override
  public boolean relax()
      throws ParseException {
    // we cannot relax anymore
    if (this.uniqueQueryTerms.size() == 1) {
      return false;
    }
    LOG.debug("Relax before={} q={}", this.uniqueQueryTerms, this.queryObj);
    switch (this.relaxRule) {
      case FIRST:
        this.uniqueQueryTerms.remove(0);
        break;
      case HIGHEST_DOCFREQ:
      case HIGHEST_TERMFREQ:
        long docFreq = 0L;
        ByteArray termToRemove = null;
        for (final Map.Entry<ByteArray, Long> tfcEntry : this.termFreqCache
            .entrySet()) {
          if (tfcEntry.getValue() > docFreq) {
            termToRemove = tfcEntry.getKey();
            docFreq = tfcEntry.getValue();
          } else if (tfcEntry.getValue() == docFreq &&
              RandomValue.getBoolean()) {
            termToRemove = tfcEntry.getKey();
          }
        }
        LOG.debug("t={}, f={}", ByteArrayUtils.utf8ToString(termToRemove),
            docFreq);
        // if term is null then there's something wrong with the index. Maybe
        // the analyzer is not correct.
        // graceful recover, remove a random term and throw a warning
        if (termToRemove == null) {
          final int idx = RandomValue.getInteger(0,
              this.termFreqCache.size() - 1);
          int counter = 0;
          final Iterator<ByteArray> termIt = this.termFreqCache.keySet()
              .iterator();
          while (termIt.hasNext()) {
            if (counter == idx) {
              termToRemove = termIt.next();
              break;
            }
            termIt.next();
            counter++;
          }
          LOG.warn("Encountered <null> for a term. Looks like there's " +
              "something wrong with the index or analyzer. Removed a random " +
              "term from the query.");
        }
        this.uniqueQueryTerms.remove(ByteArrayUtils.utf8ToString(termToRemove));
        this.termFreqCache.remove(termToRemove);
        break;
      case LAST:
        this.uniqueQueryTerms.remove(this.uniqueQueryTerms.size() - 1);
        break;
      case RANDOM:
        this.uniqueQueryTerms.remove(RandomValue.getInteger(0,
            this.uniqueQueryTerms.size() - 1));
        break;
    }

    this.queryObj = this.qParser.parse(QueryParserUtil.escape(StringUtils
        .join(this.uniqueQueryTerms, " ")));
    LOG.debug("Relax after={} q={} rule={}", this.uniqueQueryTerms,
        this.queryObj, this.relaxRule);
    return true;
  }

  /**
   * Get the current query object. This must be called after each call to {@link
   * #relax()}, because the object is re-created each time the query gets
   * simplified.
   *
   * @return Current query object
   */
  @Override
  public Query getQueryObj() {
    LOG.debug("return new Query {}", this.queryObj);
    return this.queryObj;
  }

  /**
   * Rules used to simplify a query, e.g. if no document matches all terms in
   * the initial query. <br> If multiple terms match the same criteria a random
   * one out of those will be chosen.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum RelaxRule {
    /**
     * Removes the first term.
     */
    FIRST,
    /**
     * Removes the term with the highest document-frequency.
     */
    HIGHEST_DOCFREQ,
    /**
     * Removes the term with the highest index-frequency.
     */
    HIGHEST_TERMFREQ,
    /**
     * Removes the last term.
     */
    LAST,
    /**
     * Removes a randomly chosen term.
     */
    RANDOM
  }
}
