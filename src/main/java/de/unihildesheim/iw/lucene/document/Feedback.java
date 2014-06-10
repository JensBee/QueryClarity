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
package de.unihildesheim.iw.lucene.document;

import de.unihildesheim.iw.lucene.query.RelaxableQuery;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.Bits;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Utility class to get feedback documents based on queries. Feedback documents
 * are provided as list of document ids matching the given query.
 *
 * @author Jens Bertram
 */
public final class Feedback {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      Feedback.class);

  /**
   * Private constructor for utility class.
   */
  private Feedback() {
    // empty private constructor for utility class
  }

  /**
   * Same as {@link #get(IndexReader, Query, int)}, except, if the maximum
   * number of feedback documents matching the query is not reached, random
   * documents will be picked from the index to reach this value. <br> NOTE:
   * This is currently broken, as field contents are not checked. Random chosen
   * documents may have no content in the targeted fields.
   *
   * @param reader Reader to access Lucene index
   * @param query Query to get matching documents
   * @param docCount Number of documents to return
   * @return List of Lucene document ids
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  // TODO: only return documents with terms in requested field(s). Also the
  // order of documents is random instead of relevance.
  public static Set<Integer> getFixed(final IndexReader reader,
      final Query query, final int docCount)
      throws IOException {
    Objects.requireNonNull(reader, "IndexReader was null.");
    Objects.requireNonNull(query, "Query was null.");

    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final int maxRetDocs = getMaxDocs(reader, docCount);
    final Set<Integer> docIds = createDocSet(getDocs(new IndexSearcher(reader),
        query, docCount));

    // get the amount of random docs to get
    int randDocs = maxRetDocs - docIds.size();

    if (randDocs > 0) {
      LOG.debug("Got {} matching feedback documents. "
              + "Getting additional {} random feedback documents...",
          docIds.size(), randDocs
      );
      final Bits liveDocs = MultiFields.getLiveDocs(reader);

      while (randDocs > 0) {
        final int docId = RandomValue.getInteger(0, maxRetDocs - 1);

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

    timeMeasure.stop();
    LOG.debug("Getting {} feedback documents took {}.", maxRetDocs,
        timeMeasure.getTimeString());
    return docIds;
  }

  /**
   * Get the maximum number of documents that can be retrieved.
   *
   * @param reader Reader to access the index
   * @param docCount Number of documents that should be retrieved
   * @return Actual number of documents possible to retrieve
   */
  private static int getMaxDocs(final IndexReader reader, final int docCount) {
    final int maxRetDocs; // maximum number of documents that can be returned
    final int maxIdxDocs = reader.maxDoc();
    if (docCount > maxIdxDocs) {
      maxRetDocs = Math.min(maxIdxDocs, docCount);
      LOG.warn("Requested number of feedback documents ({}) "
              + "is larger than the amount of documents in the index ({}). "
              + "Returning only {} feedback documents at maximum.",
          docCount, maxIdxDocs, maxRetDocs
      );
    } else {
      maxRetDocs = docCount;
    }
    return maxRetDocs;
  }

  /**
   * Create a set of document ids from the provided {@link TopDocs} instance.
   * The result order is preserved.
   *
   * @param topDocs {@link TopDocs} instance
   * @return New Set of document ids
   */
  private static Set<Integer> createDocSet(final TopDocs topDocs) {
    // LinkedHashSet keeps the order of elements (important to keep the
    // scoring).
    final Set<Integer> docIds = new LinkedHashSet<>(topDocs.scoreDocs
        .length);
    mergeDocSet(docIds, topDocs);
    return docIds;
  }

  /**
   * Get a limited number of feedback documents matching a query.
   *
   * @param searcher Searcher for issuing the query
   * @param query Query to get matching documents
   * @param maxDocCount Maximum number of documents to get. The number of
   * results may be lower, if there were less matching documents. If {@code -1}
   * is provided as value, then the maximum number of feedback documents is
   * unlimited. This means, all matching documents will be returned.
   * @return Documents matching the query
   * @throws IOException Thrown on low-level I/O errors
   */
  private static TopDocs getDocs(final IndexSearcher searcher,
      final Query query, final int maxDocCount)
      throws IOException {
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final TopDocs results;
    final int fbDocCnt;
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
    }

    timeMeasure.stop();

    if (LOG.isDebugEnabled()) {
      if (maxDocCount == -1) {
        //noinspection HardcodedFileSeparator
        LOG.debug("Getting {}/unlimited feedback documents "
            + "took {}.", fbDocCnt, timeMeasure.getTimeString());
      } else {
        //noinspection HardcodedFileSeparator
        LOG.debug("Getting {}/{} feedback documents ({} requested) "
                + "took {}.", fbDocCnt, results.totalHits, maxDocCount,
            timeMeasure.getTimeString()
        );
      }
    }
    return results;
  }

  /**
   * Merges the document ids from the provided {@link TopDocs} instance into the
   * provided Set. The results are appended to the list. The original order from
   * TopDocs is preserved while adding.
   *
   * @param target Target Set to append results to. Gets modified in place
   * @param topDocs {@link TopDocs} instance
   */
  private static void mergeDocSet(final Collection<Integer> target,
      final TopDocs topDocs) {
    // add the matching documents to the list
    for (final ScoreDoc scoreDoc : topDocs.scoreDocs) {
      target.add(scoreDoc.doc);
    }
  }

  /**
   * Same as {@link #get(IndexSearcher, Query, int)}, but creates a new {@link
   * IndexSearcher} from the provided {@link IndexReader}.
   *
   * @param reader Reader to access the Lucene index
   * @param query Query to get matching documents
   * @param docCount Number of documents to return
   * @return List of Lucene document ids
   * @throws IOException Thrown on low-level I/O errors
   */
  public static Set<Integer> get(final IndexReader reader,
      final Query query, final int docCount)
      throws IOException {
    return get(new IndexSearcher(reader), query, docCount);
  }

  /**
   * Try to get a specific number of feedback documents matching a query. <br>
   * The documents in the returned set are ordered by their relevance. Sorting
   * is from best to worst matching.
   *
   * @param searcher Searcher for issuing the query
   * @param query Query to get matching documents
   * @param docCount Number of documents to return
   * @return List of Lucene document ids
   * @throws IOException Thrown on low-level I/O errors
   */
  public static Set<Integer> get(final IndexSearcher searcher,
      final Query query, final int docCount)
      throws IOException {
    Objects.requireNonNull(searcher, "IndexSearcher was null.");
    Objects.requireNonNull(query, "Query was null.");

    final int maxRetDocs; // maximum number of documents that can be returned
    if (docCount == -1) {
      maxRetDocs = searcher.getIndexReader().maxDoc();
    } else {
      maxRetDocs = getMaxDocs(searcher.getIndexReader(), docCount);
    }

    return createDocSet(getDocs(searcher, query, maxRetDocs));
  }

  /**
   * Same as {@link #getFixed(IndexSearcher, RelaxableQuery, int)}, but creates
   * a new {@link IndexSearcher} from the provided {@link IndexReader}.
   *
   * @param reader Reader to access the Lucene index
   * @param query Relaxable query to get matching documents
   * @param docCount Number of documents to try to reach. Results may be lower,
   * if there are less matching documents in the index
   * @return List of documents matching the (relaxed) query
   * @throws IOException Thrown on low-level I/O errors
   * @throws ParseException Thrown on errors parsing a relaxed query
   */
  public static Set<Integer> getFixed(final IndexReader reader,
      final RelaxableQuery query, final int docCount)
      throws IOException, ParseException {
    final IndexSearcher searcher = new IndexSearcher(reader);
    return getFixed(searcher, query, docCount);
  }

  /**
   * Try to get a fixed number of feedback documents. If the number of feedback
   * documents is not reached by running an initial query the query get relaxed
   * (simplified) to reach a higher number of matching feedback documents. If
   * the desired number of documents could not be reached by relaxing the query
   * all matching documents collected so far are returned. So the number of
   * returned documents may be lower than desired. <br> The documents in the
   * returned set are ordered by their relevance. Sorting is from best to worst
   * matching.
   *
   * @param searcher Searcher to issue queries
   * @param query Relaxable query to get matching documents
   * @param docCount Number of documents to try to reach. Results may be lower,
   * if there are less matching documents in the index
   * @return List of documents matching the (relaxed) query
   * @throws IOException Thrown on low-level I/O errors
   * @throws ParseException Thrown on errors parsing a relaxed query
   */
  public static Set<Integer> getFixed(final IndexSearcher searcher,
      final RelaxableQuery query, final int docCount)
      throws IOException, ParseException {
    Objects.requireNonNull(searcher, "IndexSearcher was null.");
    Objects.requireNonNull(query, "Query was null.");

    final int maxRetDocs = getMaxDocs(searcher.getIndexReader(), docCount);
    final Set<Integer> docIds = createDocSet(getDocs
        (searcher, query.getQueryObj(), docCount));

    int docsToGet;
    while (docIds.size() < docCount) {
      docsToGet = maxRetDocs - docIds.size();
      LOG.info("Got {} matching feedback documents. "
          + "Relaxing query to get additional {} feedback " +
          "documents...", docIds.size(), docsToGet);
      if (query.relax()) {
        final Set<Integer> result = createDocSet(getDocs(searcher,
            query.getQueryObj(), maxRetDocs));
        if (result.size() > docsToGet) {
          final Iterator<Integer> docIdIt = result.iterator();
          while (docIdIt.hasNext() && docIds.size() < docCount) {
            docIds.add(docIdIt.next());
          }
        } else {
          docIds.addAll(result);
        }
      } else {
        LOG.info("Cannot relax query any more. Returning only {} documents" +
            ".", docIds.size());
        break;
      }
    }

    LOG.debug("Returning {} documents.", docIds.size());
    return docIds;
  }
}
