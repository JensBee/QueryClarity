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
package de.unihildesheim.lucene.queryclarity;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class TestUtility {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          CalculationTest.class);

  /**
   * Character source for random query term generation.
   */
  private static final char[] RAND_TERM_LETTERS = new char[]{'a', 'b', 'c', 'd',
    'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4', '5', '6', '7', '8',
    '9', '0'};

  /**
   * Calculate facorial value for a number.
   *
   * @param number Value to get the facorial for
   * @return Facorial of given value
   */
  protected final long calcFactorial(final long number) {
    long factorial = 1;
    for (long i = 1; i <= number; ++i) {
      factorial *= i;
    }
    return factorial;
  }

  /**
   * Join strings contained in a set. From https://stackoverflow.com/a/15921919
   *
   * @param set Set with strings to join
   * @param sep Seperator value
   * @return All strings of the given set joined by the given seperator
   */
  public static final String join(final Set<String> set, final String sep) {
    String result = null;
    if (set != null) {
      final StringBuilder strBuilder = new StringBuilder();
      final Iterator<String> setIt = set.iterator();
      if (setIt.hasNext()) {
        strBuilder.append(setIt.next());
      }
      while (setIt.hasNext()) {
        strBuilder.append(sep).append(setIt.next());
      }
      result = strBuilder.toString();
    }
    return result;
  }

  /**
   *
   * @param log Logger instance to use
   * @param title the value of title
   */
  protected static final void logHeader(final Logger log, final String title) {
    log.info(">>\n##################################################\n### {}\n"
            + "##################################################", title);
  }

  /**
   * Test configration to use.
   */
  private final TestConfiguration conf;

  /**
   * Create a new utility instance with the given configuration
   *
   * @param config Configuration to use
   */
  protected TestUtility(final TestConfiguration config) {
    this.conf = config;
  }

  /**
   * Calculate the maximum possible query length based on the given input.
   *
   * @param desiredMaxLength Desired maximum query length in terms
   * @param desiredMinLength Desired minimum query length in terms
   * @return The possible maximum query length. Same as the input, if it's valid
   */
  protected final int getMaxQueryLength(final Integer desiredMaxLength) {
    int maxLength;

    if (desiredMaxLength == null || desiredMaxLength > conf.getMaxQueryLength()) {
      maxLength = conf.getMaxQueryLength();
    } else {
      maxLength = desiredMaxLength;
    }
    if (maxLength > conf.getIndex().getTermCount()) {
      maxLength = conf.getIndex().getTermCount();
    }

    return maxLength;
  }

  /**
   * Calculate the minimum possible query length based on the given input.
   *
   * @param desiredMinLength Desired minimum query length
   * @return The possible minimum query length. Same as the input, if it's valid
   */
  protected final int getMinQueryLength(final Integer desiredMinLength) {
    int minLength = 1;

    if (desiredMinLength != null && desiredMinLength > 0) {
      minLength = desiredMinLength;
    }
    if (minLength > getMaxQueryLength(null)) {
      minLength = getMaxQueryLength(null);
    }

    return minLength;
  }

  /**
   * Validate the values for min/max query length an output a warning, if the
   * values have changed.
   *
   * @param minLength Desired minimum query length in terms
   * @param maxLength Desired maximum query length in terms
   * @return Integer array with {min, max} length for a query
   */
  protected final Integer[] validateQueryLengths(final Integer minLength,
          final Integer maxLength) {
    final int minQueryLength = this.getMinQueryLength(minLength);
    final int maxQueryLength = this.getMaxQueryLength(maxLength);

    if (minLength != null && minLength != minQueryLength) {
      LOG.warn("Index restriction: setting minimum query length to {}",
              minQueryLength);
    }
    if (maxLength != null && maxLength != maxQueryLength) {
      LOG.warn("Index restriction: setting maximum query length to {}",
              maxQueryLength);
    }

    return new Integer[]{minQueryLength, maxQueryLength};
  }

  /**
   * Dump all queries contained in a set.
   *
   * @param queries Queries to dump
   */
  protected static final void dumpQueries(final Set<Set<String>> queries) {
    final TreeSet<String> queryList = new TreeSet();

    for (final Set<String> query : queries) {
      queryList.add(TestUtility.join(query, " "));
    }

    LOG.info("DumpQueries --start--");
    for (String q : queryList) {
      LOG.info("DumpQueries: {}", q);
    }
    LOG.info("DumpQueries --end--");
  }

  /**
   * Generate a random query based on the terms available in the lucene index.
   *
   * @param minLength Minimum length of the query in terms, or 1 if null.
   * @param maxLength Maximim length of the query in terms, or
   * {@link #MAX_QUERY_LENGTH}, if null
   * @return Random choosen terms for a query
   */
  protected final Set<String> generateRandomQuery(final Integer minLength,
          final Integer maxLength) {
    final Set<String> indexTermSet = conf.getIndex().getTerms();
    final String[] indexTerms = indexTermSet.toArray(new String[indexTermSet.
            size()]);

    int randTermIdx; // random number to choose a term from index
    int randQueryLength; // random length for a generated query

    // check desired values for query lengths
    final Integer[] queryLengths = this.validateQueryLengths(minLength,
            maxLength);
    final int minQueryLength = queryLengths[0];
    final int maxQueryLength = queryLengths[1];

    randQueryLength = minQueryLength + (int) (Math.random() * (maxQueryLength
            - minQueryLength) + 1);

    // HashSet ensures distinct values
    final Set<String> queryTerms = new HashSet(randQueryLength);

    while (queryTerms.size() < randQueryLength) {
      LOG.trace("queryGen now={} size={} desired={} min={} max={}",
              queryTerms, queryTerms.size(), randQueryLength, minQueryLength,
              maxQueryLength);
      randTermIdx = (int) (Math.random() * (indexTerms.length));
      LOG.trace("randTermIdx={}", randTermIdx);
      queryTerms.add(indexTerms[randTermIdx]);
    }

    // return a alphabetically sorted set of query terms
    return new TreeSet(queryTerms);
  }

  /**
   * Generate a random query with random length, based on the terms available in
   * the lucene index. See {@link #generateRandomQuery(java.lang.Integer, java.lang.Integer)
   *
   * @return Random choosen terms for a query
   */
  protected final Set<String> generateRandomQuery() {
    return this.generateRandomQuery(null, null);
  }

  /**
   * Create a query object from the given string. The given string will be
   * tokenized at whitespace and duplicate terms will be eliminated.
   *
   * @param query Query string
   * @return Unique set of query terms contained in the original query
   */
  protected final Set<String> generateQuery(final String query) {
    final Set<String> queryTerms = new HashSet();
    for (String term : Arrays.asList(query.toLowerCase().split("\\s+"))) {
      queryTerms.add(term);
    }
    return queryTerms;
  }

  /**
   * Generate a number of random distinct queries based on the terms in the
   * lucene index.
   *
   * @param count Number of distinct queries to generate
   * @param minLength Minimum length of a query in terms
   * @param maxLength Maximum length of a query in terms
   * @return Generated queries
   */
  protected final Set<Set<String>> generateRandomQueries(final Integer count,
          Integer minLength, Integer maxLength) {
    // check desired values for query lengths
    final Integer[] queryLengths = this.validateQueryLengths(minLength,
            maxLength);
    final int minQueryLength = queryLengths[0];
    final int maxQueryLength = queryLengths[1];
    int queryCount = count; // number of queries to run

    // check number of possible combinations
    long possibleCombinations = 0L;
    final int termsInIndex = conf.getIndex().getTermCount();
    final long facAllTerms = this.calcFactorial(termsInIndex);
    for (int i = minQueryLength; i <= maxQueryLength; i++) {
      possibleCombinations += facAllTerms / (this.calcFactorial(i) * this.
              calcFactorial(termsInIndex - i));
    }

    if (count > possibleCombinations) {
      LOG.warn("The number of desired queries ({}) "
              + "exceedes the number of possible combinations ({}). "
              + "Reducing the number of queries to {}",
              count, possibleCombinations, possibleCombinations);
      queryCount = (int) possibleCombinations;
    }

    LOG.info("Generating {} random queries minLength={} maxLength={}",
            queryCount,
            minQueryLength, maxQueryLength);

    final Set<Set<String>> queries = new HashSet(queryCount);

    while (queries.size() < queryCount) {
      queries.add(this.generateRandomQuery(minQueryLength, maxQueryLength));
    }

    return queries;
  }

  /**
   * Generate a number of random distinct queries with random length based on
   * the terms in the lucene index. See {@link #generateRandomQueries(java.lang.Integer, java.lang.Integer, java.lang.Integer)
   * }
   *
   * @param count Number of queries to generate
   * @return Random queries
   */
  protected final Set<Set<String>> generateRandomQueries(final Integer count) {
    return this.generateRandomQueries(count, null, null);
  }

  /**
   * Get a random char from the list of chars {@link #RAND_TERM_LETTERS} allowed
   * for random term generation.
   *
   * @return a random choosen char
   */
  protected final char getRandomChar() {
    final int idx = (int) (Math.random() * RAND_TERM_LETTERS.length);
    return RAND_TERM_LETTERS[idx];
  }

  /**
   * Generate a random term based on the list of chars {@link #RAND_TERM_LETTERS}
   * allowed for random term generation.
   *
   * @param minLength Minimum length of the term
   * @param maxLength Maximum length of the term
   * @return A random generated term
   *
   */
  protected final String generateRandomTerm(final int minLength,
          final int maxLength) {
    final int randTermLength = minLength + (int) (Math.random() * (maxLength
            - minLength) + 1);
    final StringBuilder termSb = new StringBuilder(randTermLength);

    for (int i = 0; i < randTermLength; i++) {
      termSb.append(getRandomChar());
    }
    return termSb.toString();
  }

  protected final Set<String> generateRandomTermsQuery() {
    final Integer[] queryLengths = validateQueryLengths(null, null);
    final int minQueryLength = queryLengths[0];
    final int maxQueryLength = queryLengths[1];
    final int randQueryLength = minQueryLength + (int) (Math.random()
            * (maxQueryLength - minQueryLength) + 1);

    final Set<String> query = new HashSet(randQueryLength);

    for (int i = 0; i < randQueryLength; i++) {
      query.add(generateRandomTerm(3, 10));
    }

    return query;
  }

  protected final Set<Set<String>> generateRandomTermsQueries(
          final int queryCount) {
    final Set<Set<String>> queries = new HashSet(queryCount);

    while (queries.size() < queryCount) {
      queries.add(generateRandomTermsQuery());
    }

    return queries;
  }
}
