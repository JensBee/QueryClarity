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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.Bits;
import org.slf4j.LoggerFactory;

/**
 * Utility class to get feedback documents needed for calculations.
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
   * results may be lower, if there were less matching documents. If <tt>-1</tt>
   * is given, then the maximum number of feedback documents is unlimited. This
   * means, all matching documents will be returned.
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
      final TotalHitCountCollector coll = new TotalHitCountCollector();
      searcher.search(query, coll);
      final int expResults = coll.getTotalHits();
      LOG.debug("Running post query expecting {} results.", expResults);
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

  /**
   * Same as {@link Feedback#get(IndexReader, Query, int)}, except that, if the
   * maximum number of feedback documents matching the query is not reached,
   * then random documents will be picked from the index to reach this value.
   *
   * @param reader Reader to access lucene's index
   * @param query Query to get matching documents
   * @param docCount Number of documents to return.
   * @return List of lucene document ids
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  public static Integer[] getFixed(final IndexReader reader,
          final Query query,
          final int docCount) throws IOException {
    LOG.debug("Getting {} feedback documents...", docCount);
    // number of documents in index
    final int maxIdxDocs = reader.maxDoc();
    int maxRetDocs; // maximum number of documents that can be returned
    boolean allDocs = false; // true, if all documents should be retirieved

    if (docCount > maxIdxDocs) {
      maxRetDocs = Math.min(maxIdxDocs, docCount);
      LOG.warn("Requested number of feedback documents ({}) "
              + "is larger than the amount of documents in the index ({}). "
              + "Returning only {} feedback documents.",
              docCount, maxIdxDocs, maxRetDocs);
      allDocs = true;
    } else {
      maxRetDocs = docCount;
    }

    Collection<Integer> docIds;
    if (allDocs) {
      // get all documents from collection
      docIds = new ArrayList(maxRetDocs);
      final Bits liveDocs = MultiFields.getLiveDocs(reader); // NOPMD

      for (int i = 0; i < reader.maxDoc(); i++) {
        // check if document is deleted
        if (liveDocs == null) {
          docIds.add(i);
        } else if (liveDocs.get(i)) {
          docIds.add(i);
        }
      }
    } else {
      // get a set of random documents
      final TopDocs initialDocs = get(reader, query, maxRetDocs);

      docIds = new HashSet(initialDocs.scoreDocs.length);
      // add the matching documents to the list
      for (ScoreDoc scoreDoc : initialDocs.scoreDocs) {
        docIds.add(scoreDoc.doc);
      }

      // get the amount of random docs to get
      int randDocs = maxRetDocs - initialDocs.scoreDocs.length;
      LOG.debug("Got {} matching feedback documents. "
              + "Getting additional {} random feedback documents...",
              initialDocs.scoreDocs.length, randDocs);

      if (randDocs > 0) {
        final Bits liveDocs = MultiFields.getLiveDocs(reader); // NOPMD

        while (randDocs > 0) {
          final int docId = (int) (Math.random() * maxIdxDocs);

          // check if document is not already collected..
          if (!docIds.contains(docId)) {
            // ..and not deleted
            if (liveDocs == null) {
              docIds.add(docId);
              randDocs--;
            } else if (liveDocs.get(docId)) {
              docIds.add(docId);
              randDocs--;
            }
          }
        }
      }
    }

    return docIds.toArray(new Integer[docIds.size()]);
  }
}
