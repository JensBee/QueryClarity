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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.QueryTermExtractor;
import org.apache.lucene.search.highlight.WeightedTerm;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class QueryUtils {

  /**
   * Private constructor for utility class.
   */
  private QueryUtils() {
    // empty private constructor for utility class
  }

  /**
   * Extract all terms of the given {@link Query}.
   * @param reader Reader to use
   * @param query Query object to parse
   * @return Terms of the rewritten {@link Query}
   * @throws IOException Thrown on low-level I/O errors
   */
  public static String[] getQueryTerms(final IndexReader reader,
          final Query query) throws IOException {
    final Query rwQuery = query.rewrite(reader);

    // get all terms from the query
    final WeightedTerm[] wqTerms = QueryTermExtractor.getTerms(
            rwQuery, true);

    // stores all plain terms from the weighted query terms
    final Set<String> queryTerms = new HashSet(wqTerms.length);

    // store all plain query terms
    for (WeightedTerm wTerm : wqTerms) {
      queryTerms.add(wTerm.getTerm());
    }

    return queryTerms.toArray(new String[queryTerms.size()]);
  }
}
