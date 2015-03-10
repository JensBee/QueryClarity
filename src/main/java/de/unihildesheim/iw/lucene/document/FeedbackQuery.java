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

import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.RelaxableQuery;
import de.unihildesheim.iw.lucene.util.BitsUtils;
import de.unihildesheim.iw.lucene.util.DocIdSetUtils;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet.Builder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  private static int[] getDocs(final IndexSearcher searcher,
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running post query expecting {} results.", expResults);
      }
      results = searcher.search(query, expResults);
      fbDocCnt = results.totalHits;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Post query returned {} feedback documents.", fbDocCnt);
      }
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
    final int[] docIds = new int[results.scoreDocs.length];
    // add the matching documents to the list
    IntStream.range(0, results.scoreDocs.length)
        .forEach(i -> docIds[i] = results.scoreDocs[i].doc);

    return docIds;
  }

  /**
   * @param searcher Searcher to issue queries
   * @param query Relaxable query to get matching documents
   * @param minDocs Minimum number of documents to get. Must be greater than
   * zero.
   * @param maxDocCount Maximum number of documents to get. {@code -1} for
   * unlimited or greater than zero.
   * @return List of documents matching the (relaxed) query. Ranking order is
   * not preserved!
   * @throws IOException Thrown on low-level I/O errors
   */
  public static DocIdSet getMinMax(final IndexSearcher searcher,
      final RelaxableQuery query, final int minDocs, final int maxDocCount)
      throws IOException {
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
    final FixedBitSet bits = new FixedBitSet(maxDocCount);
    bits.or(BitsUtils.arrayToBits(getDocs(searcher, q, maxRetDocs)));

    int docsToGet;
    while (bits.cardinality() < minDocs) {
      final int bitsCount = bits.cardinality();
      docsToGet = maxRetDocs - bitsCount;
      if (maxDocCount > 0) {
        LOG.info("Got {} matching feedback documents. "
            + "Relaxing query to get additional {} feedback " +
            "documents...", bitsCount, docsToGet);
      } else {
        LOG.info("Got {} matching feedback documents. "
            + "Relaxing query to reach the minimum of {} feedback " +
            "documents...", bitsCount, minDocs);
      }
      if (query.relax()) {
        final int[] docs = getDocs(searcher, q, maxRetDocs);
        for (int i = docs.length - 1; i >= 0; i--) {
          bits.set(docs[i]);
          if (bits.cardinality() >= maxDocs) {
            break;
          }
        }
      } else {
        LOG.info("Cannot relax query any more. Returning only {} documents.",
            bits.cardinality());
        break;
      }
    }

    LOG.info("Returning {} documents.", bits.cardinality());
    return new BitDocIdSet(bits);
  }

  /**
   * Try to get a fixed number of feedback documents. If the number of feedback
   * documents is not reached by running an initial query the query get relaxed
   * (simplified) to reach a higher number of matching feedback documents. If
   * the desired number of documents could not be reached by relaxing the query
   * all matching documents collected so far are returned. So the number of
   * returned documents may be lower than desired. <br> The documents in the
   * returned set are ordered by their relevance. Sorting is from best to worst
   * matching. Creates a new {@link IndexSearcher} from the provided {@link
   * IndexReader}.
   *
   * @param searcher Searcher to search the Lucene index
   * @param dataProv DataProvider
   * @param query Relaxable query to get matching documents
   * @param docCount Number of documents to try to reach. Results may be lower,
   * if there are less matching documents in the index
   * @return List of documents matching the (relaxed) query. Ranking order is
   * not preserved!
   * @throws IOException Thrown on low-level I/O errors
   */
  public static DocIdSet getFixed(final IndexSearcher searcher,
      final IndexDataProvider dataProv,
      final RelaxableQuery query, final int docCount)
      throws IOException {
    DocIdSet docIds = getMinMax(searcher, query, docCount, docCount);
    if (DocIdSetUtils.cardinality(docIds) < docCount) {
      docIds = getRandom(dataProv, docCount, docIds);
    }
    return docIds;
  }

  /**
   * Get random documents from the index.
   *
   * @param dataProv DataProvider to request documents from
   * @param amount Amount of documents to get at total
   * @param docIds Document-ids already collected. Size must be lower than
   * {@code amount}.
   * @return DocIdSet containing all ids from {@code docIds} and a number of
   * random chosen documents. The size will be {@code amount}, if enough
   * additional documents are present in the index. Otherwise it may be lower.
   * @throws IOException Thrown on low-level i/o-errors
   */
  public static DocIdSet getRandom(final IndexDataProvider dataProv,
      final int amount, final DocIdSet docIds)
      throws IOException {
    final int[] results = new int[amount];
    // collect all provided document-ids, to skip them while choosing random
    // ones
    final int[] docIdsArr = StreamUtils.stream(docIds).sorted().toArray();
    // count collected documents
    int currentAmount = docIdsArr.length;
    if (currentAmount > 0) {
      // copy already provided doc-ids to results
      System.arraycopy(docIdsArr, 0, results, 0, currentAmount);
    }

    LOG.info("Getting {}/{} random feedback documents. {} documents provided.",
        amount - currentAmount, amount, currentAmount);

    final List<Integer> haveDocs = dataProv
        .getDocumentIds()
            // skip ids already provided
        .filter(id -> Arrays.binarySearch(docIdsArr, id) < 0)
        .boxed()
        .collect(Collectors.toList());

    if (haveDocs.isEmpty()) {
      LOG.warn("Giving up. No random documents. Got {} documents.",
          currentAmount);
    } else {
      // add as many documents as possible, while no adding more than the
      // amount specified
      for (int i = Math.min(amount - currentAmount, haveDocs.size());
           i > 0; i--) {
        results[currentAmount++] =
            haveDocs.remove(RandomValue.getInteger(0, haveDocs.size() - 1));
      }
      if (currentAmount < amount && haveDocs.isEmpty()) {
        LOG.warn("Giving up searching for random documents. Got {} documents.",
            currentAmount);
      }
    }
    Arrays.sort(results);
    final Builder result = new Builder(results[results.length -1]);
    Arrays.stream(results).forEach(result::add);
    return result.build();
  }
}
