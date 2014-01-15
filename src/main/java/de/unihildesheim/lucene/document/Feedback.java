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
package de.unihildesheim.lucene.document;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class Feedback {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          Feedback.class);

  /**
   * Get a number of feedback documents matching a query.
   *
   * @param reader Reader to access lucene's index
   * @param query Query to get matching documents
   * @param maxDocCount Maximum number of documents to get. The number of
   * results may be lower, if there were less matching documents.
   * @return Documents matching the query
   * @throws IOException Thrown on low-level I/O errors
   */
  public static TopDocs get(final IndexReader reader, final Query query,
          final int maxDocCount) throws IOException {
    LOG.debug("Getting feedback documents...");

    final IndexSearcher searcher = new IndexSearcher(reader);

    TopDocs results;
    int fbDocCnt;
    if (maxDocCount == -1) {
      LOG.debug("Feedback doc count is unlimited. "
              + "Running pre query to get total hits.");
      results = searcher.search(query, 1);
      LOG.debug("Running post query expecting {} results.", results.totalHits);
      final int expResults = results.totalHits;
      results = searcher.search(query, expResults);
      fbDocCnt = results.totalHits;
      LOG.debug("Post query returned {} feedback documents.", fbDocCnt);
    } else {
      results = searcher.search(query, maxDocCount);
      fbDocCnt = Math.min(results.totalHits, maxDocCount);
      LOG.debug("Query returned {} feedback documents "
              + "from {} total matching documents.",
              fbDocCnt, results.totalHits);
    }

    return results;
  }
}
