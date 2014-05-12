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

import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Simple term query using a {@link org.apache.lucene.queryparser.classic
 * .MultiFieldQueryParser} under the hood.
 *
 * @author Jens Bertram
 */
public final class SimpleTermsQuery
    extends Query {

  /**
   * Default boolean operator to use for concatenating terms.
   */
  public static final QueryParser.Operator DEFAULT_OPERATOR
      = QueryParser.Operator.OR;
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
   * Create a new simple term query.
   *
   * @param query Query string
   * @param operator Default boolean operator to use
   * @param fields Document fields to include
   * @param stopWords List of stop-words to exclude from the query (case is
   * ignored)
   * @throws ParseException Thrown if there were errors parsing the query
   * string
   */
  public SimpleTermsQuery(final String query,
      final QueryParser.Operator operator, final Set<String> fields,
      final Set<String> stopWords)
      throws ParseException {
    LOG.debug("STQ q({})={} op={} f={} s({})={}", query.split(" ").length,
        query, operator, fields, stopWords.size(), stopWords);
    if (query == null || query.trim().isEmpty()) {
      throw new IllegalArgumentException("Empty query.");
    }
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
        new CharArraySet(LuceneDefaults.VERSION, stopWords, true));
    final QueryParser qParser = new MultiFieldQueryParser(
        LuceneDefaults.VERSION, fields.toArray(new String[fields.size()]),
        analyzer);
    this.queryTerms = tokenizeQueryString(query, analyzer);
    final String stoppedQuery = StringUtils.join(this.queryTerms, " ");

    if (stoppedQuery.trim().isEmpty()) {
      throw new ParseException("Stopped query is empty.");
    }
    LOG.debug("Queries: orig={} stopped={}", query, stoppedQuery);

    qParser.setDefaultOperator(operator);
    this.queryObj = qParser.parse(stoppedQuery);
  }

  /**
   * Tokenizes a query string using Lucenes analyzer. This also removes
   * stopwords from the query string.
   *
   * @param query Query string to tokenize
   * @param analyzer Analyzer to use
   * @return Tokenized query string with stop-words removed
   */
  private List<String> tokenizeQueryString(final String query,
      final Analyzer analyzer) {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    List<String> result = new ArrayList<>();
    try (TokenStream stream = analyzer.tokenStream(null,
        new StringReader(query))) {
      stream.reset();
      while (stream.incrementToken()) {
        result.add(stream.getAttribute(CharTermAttribute.class)
            .toString());
      }
    } catch (IOException e) {
      // not thrown b/c we're using a string reader...
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Get the Query object.
   *
   * @return Query object
   */
  protected Query getQueryObj() {
    return this.queryObj;
  }

  /**
   * Get the list of terms from the original query. Stop-words are removed.
   *
   * @return List of query terms with stop-words removed
   */
  public List<String> getQueryTerms() {
    return new ArrayList<>(this.queryTerms);
  }

  @Override
  public String toString(final String field) {
    return "SimpleTermQuery: " + queryObj.toString(field);
  }

  @Override
  public String toString() {
    return "SimpleTermQuery: " + queryObj.toString();
  }

  @Override
  public Weight createWeight(final IndexSearcher searcher)
      throws IOException {
    return new SimpleTermQueryWeight(searcher);
  }

  /**
   * Provides weighting information for query re-use.
   */
  @SuppressWarnings("PublicInnerClass")
  public final class SimpleTermQueryWeight
      extends Weight {

    /**
     * Weight object for the simple query.
     */
    private final Weight stqWeight;

    /**
     * Create a new weight object.
     *
     * @param searcher Searcher to use
     * @throws IOException Thrown on low-level I/O errors
     */
    public SimpleTermQueryWeight(final IndexSearcher searcher)
        throws
        IOException {
      super();
      stqWeight = getQueryObj().createWeight(searcher);
    }

    @Override
    public Explanation explain(final AtomicReaderContext context,
        final int doc)
        throws IOException {
      return null;
    }

    @Override
    public Query getQuery() {
      return SimpleTermsQuery.this;
    }

    @Override
    public float getValueForNormalization()
        throws IOException {
      return stqWeight.getValueForNormalization();
    }

    @Override
    public void normalize(final float norm, final float topLevelBoost) {
      stqWeight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(final AtomicReaderContext context,
        final boolean scoreDocsInOrder,
        final boolean topScorer, final Bits acceptDocs)
        throws IOException {
      return stqWeight.
          scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
    }
  }
}
