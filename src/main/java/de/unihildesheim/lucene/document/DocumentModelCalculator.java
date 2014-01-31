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

import de.unihildesheim.lucene.LuceneDefaults;
import de.unihildesheim.lucene.scoring.clarity.ClarityScorePrecalculator;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.lucene.search.TotalHitCountCollector;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DocumentModelCalculator implements Runnable {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DocumentModelCalculator.class);
  /**
   * {@link ClarityScorePrecalculator} instance using this calculator.
   */
  private final ClarityScorePrecalculator cspInstance;
  /**
   * Lucene parser for all available document fields.
   */
  protected final MultiFieldQueryParser mfQParser;

  /**
   * Termination flag.
   */
  private boolean terminate = false;

  /**
   * Name of this thread.
   */
  private String tName;
  /**
   * Queue for models that should be updated but are currently locked.
   */
  private final ArrayBlockingQueue<Tuple.Tuple2<Integer, BytesWrap[]>> updateQueue;
  /**
   * Size of the updater queue.
   */
  private static final int UPDATEQUEUE_SIZE = 50;
  /**
   * How long should we wait while trying to add e new updater to the queue
   * (ms).
   */
  private static final int UPDATEQUEUE_TIMEOUT = 500;
  /**
   * How many updater threads to run.
   */
  private static final int UPDATE_THREADS_COUNT = 5;
  /**
   * Factory creating updater threads.
   */
  private final DocumentModelUpdaterFactory dmuFactory;
  /**
   * Updater-threads manager.
   */
  private final ExecutorService executor;
  /**
   * Number of terms to take per query.
   */
  private static final int TERMS_PER_QUERY = 10;
  /**
   * Maximum time to wait for the next query term to be available (in seconds).
   */
  private static final int TERMS_MAXWAIT = 2;
  /**
   * Expected length of a general query term to pre-calculate query length.
   */
  private static final int EXP_TERM_LENGTH = 10;

  public DocumentModelCalculator(final ClarityScorePrecalculator pCalcInstance) {
    final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION,
            CharArraySet.EMPTY_SET);
    this.cspInstance = pCalcInstance;

    // query setup
    this.mfQParser = new MultiFieldQueryParser(LuceneDefaults.VERSION,
            this.cspInstance.getDataProvider().getTargetFields(), analyzer);
    // default to OR queries
    this.mfQParser.setDefaultOperator(QueryParser.Operator.OR);
    this.updateQueue = new ArrayBlockingQueue(UPDATEQUEUE_SIZE);

    // threading setup
    this.dmuFactory = new DocumentModelUpdaterFactory(this.cspInstance);
    this.executor = Executors.newFixedThreadPool(UPDATE_THREADS_COUNT,
            dmuFactory);
  }

  private boolean tryExecuteUpdater(final int docId, final BytesWrap[] terms) {
    if (this.cspInstance.getLockedModelsSet().add(docId)) {
      // successfully locked - feed it to an updater thread
      this.executor.execute(this.dmuFactory.newInstance(docId, terms));
      return true;
    }
    return false;
  }

  private void deQueueUpdaters() {
    Iterator<Tuple.Tuple2<Integer, BytesWrap[]>> updateQueueIt;
    if (!updateQueue.isEmpty()) {
      updateQueueIt = updateQueue.iterator();
      Tuple.Tuple2<Integer, BytesWrap[]> queueEntry;
      while (updateQueueIt.hasNext()) {
        queueEntry = updateQueueIt.next();
        // try to run current entry from queue
        if (tryExecuteUpdater(queueEntry.a, queueEntry.b)) {
          // succeeded, remove from queue and tracking list
          updateQueueIt.remove();
        }
      }
    }
  }

  /**
   * Updates all matching document models for the given term.
   *
   * @param querTerms Terms to use for updating
   * @param results Lucene query results for the specified term
   * @throws InterruptedException Thrown if thread was interrupted
   */
  private void updateModels(final BytesWrap[] queryTerms,
          final ScoreDoc[] results) throws InterruptedException {
    final ArrayList<ScoreDoc> matchingDocs = new ArrayList(Arrays.asList(
            results));
    Iterator<ScoreDoc> matchingDocsIt;
    ScoreDoc scoreDoc;

    // calculate models for results
    final TimeMeasure tmBuild = new TimeMeasure().start();
    while (!matchingDocs.isEmpty()) {
      matchingDocsIt = matchingDocs.iterator();
      while (matchingDocsIt.hasNext()) {
        scoreDoc = matchingDocsIt.next();
        if (tryExecuteUpdater(scoreDoc.doc, queryTerms)) {
          // remove from tracking list
          matchingDocsIt.remove();
        } else {
          // currently locked, try add to queue
          if (updateQueue.offer(Tuple.tuple2(scoreDoc.doc, queryTerms),
                  UPDATEQUEUE_TIMEOUT, TimeUnit.MILLISECONDS)) {
            // adding succeeded - remove from tracking list
            matchingDocsIt.remove();
          }
        }
        deQueueUpdaters();
      }
      deQueueUpdaters();
    }
    tmBuild.stop();
    if (((int) tmBuild.getElapsedSeconds()) > 0) {
      LOG.debug("({}) Spawning updaters took {}.", this.tName, tmBuild.
              getElapsedTimeString());
    }
  }

  public void terminate() {
    LOG.debug("({}) Thread is terminating.", this.tName);
    this.terminate = true;
  }

  @Override
  public void run() {
    this.tName = Thread.currentThread().getName();
    // Query for the current term.
    Query query;
    // Searcher to access the index.
    IndexSearcher searcher
            = new IndexSearcher(this.cspInstance.getIndexReader());
    // Collector to get the total number of matching documents for a term.
    TotalHitCountCollector coll;
    // Expected number of documents to retrieve.
    int expResults;
    // Scoring documents for a single term.
    ScoreDoc[] results;
    // how many terms have been received for this query run
    int termsReceived;
    // list of terms part of the query
    BytesWrap[] queryTerms = new BytesWrap[TERMS_PER_QUERY];
    // current term to do calculation for
    BytesWrap term;
    // the resulting query string made of the current query terms
    StringBuilder queryString;

    try {
      while (!Thread.currentThread().isInterrupted() && !(this.terminate
              && this.cspInstance.getTermQueue().isEmpty())) {
        // collect the specified amount of terms
        termsReceived = 0;
        queryString = new StringBuilder(EXP_TERM_LENGTH * TERMS_PER_QUERY);
        Arrays.fill(queryTerms, null); // empty previous results
        while (termsReceived != TERMS_PER_QUERY) {
          term = this.cspInstance.getTermQueue().poll(
                  TERMS_MAXWAIT, TimeUnit.SECONDS);
          if (term == null) {
            // no term available in expected time, stop here
            break;
          } else {
            // build query string, trailing space should not harm
            queryString.append(BytesWrapUtil.bytesWrapToString(term)).append(
                    " ");
            // store term for later
            queryTerms[termsReceived] = term;
            // inc query term counter
            termsReceived++;
          }
        }
        if (queryString.length() == 0) {
          // there's no query to run
          LOG.debug("({}) QueryString was empty...", this.tName);
          continue; // next try
        }
        LOG.trace("({}) Query for {}...", this.tName, queryString);

        try {
          query = this.mfQParser.parse(queryString.toString());
        } catch (ParseException ex) {
          LOG.error("(" + this.tName
                  + ") Caught exception while parsing term query '"
                  + queryString + "'.", ex);
          continue;
        }

        // run the queries
        try {
          coll = new TotalHitCountCollector();
          searcher.search(query, coll);
          expResults = coll.getTotalHits();
          LOG.trace("({}) Query for {} ({}) yields {} results.", this.tName,
                  queryString, query, expResults);

          if (expResults > 0) {
            // collect the results
            results = searcher.search(query, expResults).scoreDocs;
            updateModels(queryTerms, results);
          }
        } catch (IOException ex) {
          LOG.error("({}) Caught exception while searching index.",
                  this.tName, ex);
        }
      }
    } catch (InterruptedException ex) {
      LOG.warn("({}) Thread interrupted.", this.tName, ex);
    }
    // decrement the thread tracking latch
    this.cspInstance.getTrackingLatch().countDown();
  }
}
