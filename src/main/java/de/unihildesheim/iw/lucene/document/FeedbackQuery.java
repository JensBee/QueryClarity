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
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
public final class FeedbackQuery {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      FeedbackQuery.class);

  /**
   * Private constructor for utility class.
   */
  private FeedbackQuery() {
    // empty private constructor for utility class
  }

  /**
   * Same as {@link #get(IndexSearcher, Query, int)}, but creates a new {@link
   * IndexSearcher} from the provided {@link IndexReader}. Mainly used for
   * testing.
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

    return getDocs(searcher, query, maxRetDocs);
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
    if (docCount == Integer.MAX_VALUE) {
      return reader.maxDoc();
    }
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
  private static Set<Integer> getDocs(final IndexSearcher searcher,
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

    // extract document ids while keeping order
    final Set<Integer> docIds = new LinkedHashSet<>(
        results.scoreDocs.length);
    // add the matching documents to the list
    for (final ScoreDoc scoreDoc : results.scoreDocs) {
      docIds.add(scoreDoc.doc);
    }

//    if (!docIds.isEmpty()) {
//      LOG.debug("{}", searcher.explain(query, results.scoreDocs[0].doc));
//    }

    return docIds;
  }

  /**
   * Same as {@link #getMin(IndexSearcher, RelaxableQuery, int)}, but creates a
   * new {@link IndexSearcher} from the provided {@link IndexReader}.
   *
   * @param reader Reader to access the Lucene index
   * @param query Relaxable query to get matching documents
   * @param minDocCount Minimum number of documents to try to reach. Results may
   * be higher, if there are more matching documents in the index
   * @return List of documents matching the (relaxed) query
   * @throws IOException Thrown on low-level I/O errors
   * @throws ParseException Thrown on errors parsing a relaxed query
   */
  public static Set<Integer> getMin(final IndexReader reader,
      final RelaxableQuery query, final int minDocCount)
      throws IOException, ParseException {
    final IndexSearcher searcher = new IndexSearcher(reader);
    return getMin(searcher, query, minDocCount);
  }

  /**
   * Try to get a minimum number of feedback documents. If the number of
   * feedback documents is not reached by running an initial query the query get
   * relaxed (simplified) to reach a higher number of matching feedback
   * documents. If the minimum number of documents could not be reached by
   * relaxing the query all matching documents collected so far are
   * returned.<br> The documents in the returned set are ordered by their
   * relevance. Sorting is from best to worst matching.
   *
   * @param searcher Searcher to issue queries
   * @param query Relaxable query to get matching documents
   * @param minDocCount Minimum number of documents to try to reach. Results may
   * be higher, if there are more matching documents in the index
   * @return List of documents matching the (relaxed) query
   * @throws IOException Thrown on low-level I/O errors
   * @throws ParseException Thrown on errors parsing a relaxed query
   */
  public static Set<Integer> getMin(final IndexSearcher searcher,
      final RelaxableQuery query, final int minDocCount)
      throws IOException, ParseException {
    return getMinMax(searcher, query, minDocCount, Integer.MAX_VALUE);
  }

  /**
   * @param searcher Searcher to issue queries
   * @param query Relaxable query to get matching documents
   * @param minDocs Minimum number of documents to get. Must be greater than
   * zero.
   * @param maxDocCount Maximum number o documents to get. {@code -1} for
   * unlimited or greater than zero.
   * @return List of documents matching the (relaxed) query
   * @throws IOException Thrown on low-level I/O errors
   * @throws ParseException Thrown on errors parsing a relaxed query
   */
  public static Set<Integer> getMinMax(final IndexSearcher searcher,
      final RelaxableQuery query, final int minDocs, int maxDocCount)
      throws IOException, ParseException {
    Objects.requireNonNull(searcher, "IndexSearcher was null.");
    Objects.requireNonNull(query, "Query was null.");

    final int maxDocs;
    if (maxDocCount == -1) {
      maxDocs = Integer.MAX_VALUE;
    } else if (maxDocCount < 0) {
      throw new IllegalArgumentException("Maximum number of documents must " +
          "be -1 (unlimited) or greater than zero.");
    } else {
      maxDocs = maxDocCount;
    }
    if (minDocs <= 0) {
      throw new IllegalArgumentException("Minimum number of documents must be" +
          " greater than zero.");
    }

    final int maxRetDocs = getMaxDocs(searcher.getIndexReader(), maxDocs);
    final Query q = query.getQueryObj();
//    LOG.debug("getMinMax: initial q={}", q);
    final Set<Integer> docIds = getDocs(searcher, q, maxRetDocs);

    int docsToGet;
    while (docIds.size() < minDocs) {
      docsToGet = maxRetDocs - docIds.size();
      if (maxDocCount > 0) {
        LOG.info("Got {} matching feedback documents. "
            + "Relaxing query to get additional {} feedback " +
            "documents...", docIds.size(), docsToGet);
      } else {
        LOG.info("Got {} matching feedback documents. "
            + "Relaxing query to reach the minimum of {} feedback " +
            "documents...", docIds.size(), minDocs);
      }
      if (query.relax()) {
        final Set<Integer> result = getDocs(searcher, q, maxRetDocs);
        if (result.size() > docsToGet) {
          final Iterator<Integer> docIdIt = result.iterator();
          while (docIdIt.hasNext() && docIds.size() < maxDocs) {
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
    return getMinMax(searcher, query, docCount, docCount);
  }

  /**
   * Same as {@link #getMinMax(IndexSearcher, RelaxableQuery, int, int)}, but
   * creates a new {@link IndexSearcher} from the provided {@link IndexReader}.
   *
   * @param reader Reader to access the Lucene index
   * @param query Relaxable query to get matching documents
   * @param minDocCount Minimum number of documents to get. Must be greater than
   * zero.
   * @param maxDocCount Maximum number o documents to get. {@code -1} for
   * unlimited or greater than zero.
   * @return List of documents matching the (relaxed) query
   * @throws IOException Thrown on low-level I/O errors
   * @throws ParseException Thrown on errors parsing a relaxed query
   */
  public static Set<Integer> getMinMax(final IndexReader reader,
      final RelaxableQuery query, final int minDocCount, int maxDocCount)
      throws IOException, ParseException {
    final IndexSearcher searcher = new IndexSearcher(reader);
    return getMinMax(searcher, query, minDocCount, maxDocCount);
  }

  public static Set<Integer> getFixed(final IndexReader reader,
      final Query query, final String[] fields, final int docCount)
      throws IOException {
    final IndexSearcher searcher = new IndexSearcher(reader);
    return getFixed(searcher, query, fields, docCount);
  }

  public static Set<Integer> getFixed(final IndexSearcher searcher,
      final Query query, final String[] fields, final int docCount)
      throws IOException {
    Objects.requireNonNull(searcher, "IndexSearcher was null.");
    Objects.requireNonNull(query, "Query was null.");

//    LOG.debug("getFixed: q={}", query);

    final int maxRetDocs = getMaxDocs(searcher.getIndexReader(), docCount);
    final Set<Integer> docIds = getDocs(searcher, query, docCount);

    int docsToGet = maxRetDocs - docIds.size();
    if (docsToGet > 0) {
      LOG.info("Got {} matching feedback documents. "
          + "Getting additional {} random feedback " +
          "documents...", docIds.size(), docsToGet);
      final int numDocs = searcher.getIndexReader().numDocs();
      int randomDoc;
      Document doc;
      int trie = 0;
      final int maxTry = docsToGet * 10;
      while (docsToGet > 0 && trie >= maxTry) {
        randomDoc = RandomValue.getInteger(0, numDocs);
        if (!docIds.contains(randomDoc)) {
          doc = searcher.getIndexReader().document(randomDoc);
          for (final String field : fields) {
            if (doc.getField(field) != null) {
              docIds.add(randomDoc);
              docsToGet--;
              break;
            }
          }
        }
        trie++;
      }
      if (trie >= maxTry) {
        LOG.warn("Giving up searching for random documents.");
      }
    }

    LOG.debug("Returning {} documents.", docIds.size());
    return docIds;
  }
}
