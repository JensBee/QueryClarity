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
package de.unihildesheim.lucene.document.model.processing;

import de.unihildesheim.lucene.LuceneDefaults;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.util.Tuple;
import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.slf4j.LoggerFactory;

/**
 * {@link ProcessingTarget.TermQueue} processing target. Reads terms from a
 * {@link BlockingDeque} and runs queries on the Lucene index to get documents
 * matching each of those terms. Those matching documents are then fed into
 * the specified worker for further processing.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TargetSearch implements ProcessingTarget.TermQueue {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          TargetSearch.class);
  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "ProcTargetSearch_";

  /**
   * {@link ProcessingTermSource} instance using this calculator.
   */
  private final ProcessingSource.TermQueue source;
  /**
   * Lucene parser for all available document fields.
   */
  private final MultiFieldQueryParser mfQParser;

  /**
   * Termination flag.
   */
  private boolean terminate = false;

  /**
   * Name of this runnable.
   */
  private String runName;
  /**
   * Queue for models that should be processed.
   */
  private final BlockingDeque<Tuple.Tuple2<Integer, BytesWrap[]>> workQueue;
  /**
   * Size of the worker task queue.
   */
  private final int workQueueSize;
  /**
   * Factory creating worker threads.
   */
  private final ProcessingWorker.DocTerms.Factory workerFactory;
  /**
   * Number of max queued items per thread.
   */
  private static final int QUEUED_ITEMS_PER_THREAD
          = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
                  + "queuedItemsPerThread", 50);
  /**
   * Number of terms to take per query.
   */
  private static final int TERMS_PER_QUERY
          = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
                  + "termsPerQuery", 10);
  /**
   * Maximum time to wait for the next query term to be available (in
   * seconds).
   */
  private static final int TERMS_MAXWAIT = ClarityScoreConfiguration.INSTANCE.
          getInt(CONF_PREFIX + "queryTermMaxWait", 2);
  /**
   * Expected length of a general query term to pre-calculate query length.
   */
  private static final int EXP_TERM_LENGTH
          = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
                  + "expectedTermLength", 10);
  /**
   * Thread observing the work queue.
   */
  private final Thread wqoThread;
  /**
   * {@link WorkQueueObserver} instance to submit work to worker threads.
   */
  private WorkQueueObserver.DocTermWorkQueueObserver wqObserver;

  /**
   * Create a new {@link DocumentModelCalculator} instance for the given
   * pre-calculator.
   *
   * @param termsSource Source providing terms
   * @param termsTargetFactory Factory creating instances processing documents
   * and terms
   */
  public TargetSearch(final ProcessingSource.TermQueue termsSource,
          final ProcessingWorker.DocTerms.Factory termsTargetFactory) {
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
            CharArraySet.EMPTY_SET);
    this.source = termsSource;

    // query setup
    this.mfQParser = new MultiFieldQueryParser(LuceneDefaults.VERSION,
            this.source.getDataProvider().getTargetFields(), analyzer);
    // default to OR queries
    this.mfQParser.setDefaultOperator(QueryParser.Operator.OR);
    this.workQueueSize = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
            + "workQueueCap", this.source.getThreadCount()
            * QUEUED_ITEMS_PER_THREAD);
    this.workQueue = new LinkedBlockingDeque<>(workQueueSize);

    // threading setup
    this.workerFactory = termsTargetFactory;
    this.wqObserver = new WorkQueueObserver.DocTermWorkQueueObserver(
            "TargetSearch", this.source, this.workerFactory, this.workQueue);
    this.wqoThread = new Thread(wqObserver, "WorkQueueObserver");
  }

  /**
   * Set the termination flag for this thread causing it to finish the current
   * work and exit.
   */
  @Override
  public void terminate() {
    LOG.debug("({}) Runnable got terminating signal.", this.runName);
    this.terminate = true;
    this.wqObserver.terminate();
  }

  @Override
  public void run() {
    this.runName = Thread.currentThread().getName();
    this.wqoThread.start();
    // Query for the current term.
    Query query;
    // Searcher to access the index.
    IndexSearcher searcher
            = new IndexSearcher(this.source.getIndexReader());
    // Collector to get the total number of matching documents for a term.
    TotalHitCountCollector coll;
    // Expected number of documents to retrieve.
    int expResults;
    // how many terms have been received for this query run
    int termsReceived;
    // the resulting query string made of the current query terms
    StringBuilder queryString;

    try {
      while (!Thread.currentThread().isInterrupted() && !(this.terminate
              && this.source.getQueue().isEmpty())) {
        // collect the specified amount of terms
        termsReceived = 0;
        queryString = new StringBuilder(EXP_TERM_LENGTH * TERMS_PER_QUERY);
        final BytesWrap[] queryTerms = new BytesWrap[TERMS_PER_QUERY];

        while (termsReceived != TERMS_PER_QUERY) {
          final BytesWrap term = this.source.getQueue().poll(
                  TERMS_MAXWAIT, TimeUnit.SECONDS);

          if (term == null) {
            // no term available in expected time, stop here
            // this will also quit the loop, if there are less than the wanted
            // amount of terms
            break;
          }

          // build query string, trailing space should not harm
          queryString.append(BytesWrapUtil.bytesWrapToString(term)).append(
                  " ");
          // store term for later
          queryTerms[termsReceived] = term; // SEGVMARK
          // inc query term counter
          termsReceived++;
        }

        if (termsReceived == 0) {
          // there's no query to run (maybe no more terms?)
          LOG.debug("({}) No terms received.", this.runName);
          continue; // next try
        }
        LOG.trace("({}) Query for {}...", this.runName, queryString);

        try {
          query = this.mfQParser.parse(queryString.toString());
        } catch (ParseException ex) {
          LOG.error("(" + this.runName
                  + ") Caught exception while parsing term query '"
                  + queryString + "'.", ex);
          continue;
        }

        // run the queries
        try {
          coll = new TotalHitCountCollector();
          searcher.search(query, coll);
          expResults = coll.getTotalHits();
          LOG.trace("({}) Query for {} ({}) yields {} results.", this.runName,
                  queryString, query, expResults);

          if (expResults > 0) {
            // collect the results
            final TopDocs results = searcher.search(query, expResults);

            // put update data tuple into queue
            for (ScoreDoc scoreDoc : results.scoreDocs) {
              this.workQueue.put(Tuple.tuple2(scoreDoc.doc, queryTerms));
            }
          } else {
            LOG.
                    warn("Expecting 0 results for query='{}'. This is suspicious.",
                            queryString);
          }
        } catch (IOException ex) {
          LOG.error("({}) Caught exception while searching index.",
                  this.runName, ex);
        }
      }
      // processing done
      this.terminate = true; // signal observer threads we're done
      LOG.debug("({}) Waiting for WorkQueueObserver..", this.runName);
      this.wqoThread.join();
    } catch (InterruptedException ex) {
      LOG.warn("({}) Runnable interrupted.", this.runName, ex);
    }

    // decrement the thread tracking latch
    this.source.getTrackingLatch().countDown();
    LOG.debug("({}) Runnable terminated.", this.runName);
  }

  /**
   * Factory to create new {@link ProcessingTarget} instances of
   * {@link ProcessingTagetSearch} type.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Factory implements
          ProcessingTarget.TermQueue.Factory {

    /**
     * Processing term source to use.
     */
    private final ProcessingSource.TermQueue source;
    /**
     * Factory creating document, terms consuming instances.
     */
    private final ProcessingWorker.DocTerms.Factory workerFactory;

    /**
     * Initialize the factory with the given source and worker factory.
     *
     * @param termSource Term source to use by the created instances.
     * @param docTermWorkerFactory Factory to create worker instances
     * processing a document and a list of terms
     */
    public Factory(final ProcessingSource.TermQueue termSource,
            final ProcessingWorker.DocTerms.Factory docTermWorkerFactory) {
      this.source = termSource;
      this.workerFactory = docTermWorkerFactory;
    }

    @Override
    public ProcessingTarget.TermQueue newInstance() {
      return new TargetSearch(this.source, this.workerFactory);
    }
  }
}
