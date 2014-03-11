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

import de.unihildesheim.lucene.LuceneDefaults;
import de.unihildesheim.lucene.util.BytesWrap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.QueryTermExtractor;
import org.apache.lucene.search.highlight.WeightedTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class QueryUtils {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          QueryUtils.class);

  private static final int MAX_EXPECTED_QUERY_LENGTH = 15;

  /**
   * Private constructor for utility class.
   */
  private QueryUtils() {
    // empty private constructor for utility class
  }

  /**
   * Extract all terms of the given {@link Query}. The returned list is
   * unique.
   *
   * @param reader Reader to use
   * @param query Query object to parse
   * @return Unique list of terms of the rewritten {@link Query}
   * @throws IOException Thrown on low-level I/O errors
   */
  public static Collection<BytesWrap> getUniqueQueryTerms(
          final IndexReader reader, final Query query) throws IOException {
    // get a list of all query terms and wrap them in an unique set
    return new HashSet<>(getAllQueryTerms(reader, query, null));
  }

  /**
   * Extract all terms of the given {@link Query}. Beware: If the query is a
   * MultiField query then it's assumed that the query string is the same for
   * all fields.
   *
   * @param reader Reader to use
   * @param query Query object to parse
   * @param fields Document fields to extract terms from MultiField queries
   * (may be null)
   * @return List of all terms of the rewritten {@link Query}
   * @throws IOException Thrown on low-level I/O errors
   */
  public static Collection<BytesWrap> getAllQueryTerms(
          final IndexReader reader, final Query query, final String[] fields)
          throws IOException {
    final Query rwQuery = query.rewrite(reader);

    final WeightedTerm[] wqTerms;
    // get all terms from the query
    if (fields == null) {
      wqTerms = QueryTermExtractor.getTerms(rwQuery, true);
    } else {
      wqTerms = QueryTermExtractor.getTerms(rwQuery, true, fields[0]);
    }

    final Collection<BytesWrap> queryTerms = new ArrayList(wqTerms.length);
    // store all plain query terms
    for (WeightedTerm wTerm : wqTerms) {
      queryTerms.add(new BytesWrap(wTerm.getTerm().getBytes("UTF-8")));
    }

    return queryTerms;
  }

  /**
   * Creates a query for the given list of fields. No stop-words are removed.
   *
   * @param fields Fields to include in query
   * @param query Query string
   * @return Query build from the given string searching the given fields
   * @throws org.apache.lucene.queryparser.classic.ParseException Thrown if
   * query could not be parsed
   */
  public static Query buildQuery(final String[] fields, final String query) throws
          ParseException {
    // create a default query
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
            CharArraySet.EMPTY_SET);
    final QueryParser qParser = new MultiFieldQueryParser(
            LuceneDefaults.VERSION, fields, analyzer);
    return qParser.parse(query);
  }
}
