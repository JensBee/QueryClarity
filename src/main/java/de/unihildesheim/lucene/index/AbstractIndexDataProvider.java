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
package de.unihildesheim.lucene.index;

import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.model.DocumentModel;
import de.unihildesheim.lucene.document.model.DocumentModelException;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.TimeMeasure;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link IndexDataProvider}. This abstract
 * class creates the basic data structures to handle pre-calculated index data
 * and provides basic accessors functions to those values.
 *
 * The calculation of all term frequency values respect the list of defined
 * document fields. So all values are only calculated for terms found in those
 * fields.
 *
 * The data storage {@link Map} implementations are assumed to be immutable,
 * so stored objects cannot be modified directly and have to be removed and
 * re-added to get modified.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public abstract class AbstractIndexDataProvider implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          AbstractIndexDataProvider.class);

  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "AbstractIDP_";

  /**
   * Index fields to operate on.
   */
  private transient String[] fields = new String[0];

  /**
   * Number of threads to use for creating document models.
   */
  private static final int DOCMODEL_CREATOR_THREADS
          = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
                  + "creatorThreads", Runtime.getRuntime().
                  availableProcessors());

  /**
   * Size of queue to feed document model creator worker threads.
   */
  private static final int DOCMODEL_CREATOR_QUEUE_CAPACITY
          = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
                  + "creatorQueueCap", DOCMODEL_CREATOR_THREADS * 10);

  /**
   * Updates the relative term frequency value for the given term. Thread
   * safe.
   *
   * @param term Term to update
   * @param value Value to overwrite any previously stored value. If there's
   * no value stored, then it will be set to the specified value.
   */
  protected abstract void setTermFreqValue(final BytesWrap term,
          final double value);

  /**
   * Updates the term frequency value for the given term. Thread safe.
   *
   * @param term Term to update
   * @param value Value to add to the currently stored value. If there's no
   */
  protected abstract void addToTermFreqValue(final BytesWrap term,
          final long value);

  /**
   * Calculate term frequencies for all terms in the index in the initial
   * given fields. This will collect all terms from all specified fields and
   * record their frequency in the whole index.
   *
   * @param reader Reader to access the index
   * @throws IOException Thrown, if the index could not be opened
   */
  protected final void calculateTermFrequencies(final IndexReader reader)
          throws IOException {
    if (reader == null) {
      throw new IllegalArgumentException("Reader was null.");
    }

    final TimeMeasure timeMeasure = new TimeMeasure().start();
    final Fields idxFields = MultiFields.getFields(reader);
    LOG.info("Calculating term frequencies for all unique terms in index.");

    Terms fieldTerms;
    TermsEnum fieldTermsEnum = null;

    // go through all fields..
    for (String field : this.fields) {
      fieldTerms = idxFields.terms(field);

      // ..check if we have terms..
      if (fieldTerms != null) {
        fieldTermsEnum = fieldTerms.iterator(fieldTermsEnum);

        // ..iterate over them..
        BytesRef bytesRef = fieldTermsEnum.next();
        while (bytesRef != null) {
          // fast forward seek to term..
          if (fieldTermsEnum.seekExact(bytesRef)) {
            // ..and update the frequency value for term
            addToTermFreqValue(new BytesWrap(bytesRef), fieldTermsEnum.
                    totalTermFreq());
          }
          bytesRef = fieldTermsEnum.next();
        }
      }
    }
    timeMeasure.stop();
    LOG.info("Calculation of term frequencies for {} unique terms in index "
            + "took {}.", getTermsCount(), timeMeasure.getElapsedTimeString());
  }

  /**
   * Create the document models used by this instance.
   *
   * Since the used {@link Map} implementation is unknown here, a map with
   * immutable objects is assumed and a modification of already stored entries
   * is prohibited. So an entry has to be removed to be updated.
   *
   * @param reader Reader to access the index
   * @throws DocumentModelException Thrown, if the {@link DocumentModel} of
   * the requested type could not be instantiated
   * @throws java.io.IOException Thrown on low-level I7O errors
   */
  protected final void createDocumentModels(final IndexReader reader) throws
          DocumentModelException, IOException {
    if (reader == null) {
      throw new IllegalArgumentException("Reader was null.");
    }

    final TimeMeasure timeMeasure = new TimeMeasure().start();
    final Bits liveDocs = MultiFields.getLiveDocs(reader);
    final int maxDoc = reader.maxDoc();
    LOG.info("Creating document models for approx. {} documents.", maxDoc);

    // show a progress status in 1% steps
    int[] runStatus = new int[]{0, (int) (maxDoc * 0.01),
      maxDoc, 0}; // counter, step, max, percentage
    TimeMeasure runTimeMeasure = new TimeMeasure();

    // threading
    final CountDownLatch trackingLatch = new CountDownLatch(
            DOCMODEL_CREATOR_THREADS);
    final BlockingQueue<Integer> docQueue = new ArrayBlockingQueue<Integer>(
            DOCMODEL_CREATOR_QUEUE_CAPACITY);
    final DocumentModelCreator[] dmcThreads
            = new DocumentModelCreator[DOCMODEL_CREATOR_THREADS];
    LOG.debug("Spawning {} threads for document model creation.",
            DOCMODEL_CREATOR_THREADS);
    for (int i = 0; i < DOCMODEL_CREATOR_THREADS; i++) {
      dmcThreads[i]
              = new DocumentModelCreator(reader, docQueue, trackingLatch);
      final Thread t = new Thread(dmcThreads[i], "DMCreator-" + i);
      t.start();
    }

    try {
      for (int docId = 0; docId < maxDoc; docId++) {
        // check if document is deleted
        if (liveDocs != null && !liveDocs.get(docId)) {
          // document is deleted
          continue;
        }

        docQueue.put(docId);
        // operating indicator
        if (++runStatus[0] % runStatus[1] == 0) {
          runTimeMeasure.stop();
          // estimate time needed
          long estimate = (long) ((runStatus[2] - runStatus[0])
                  / (runStatus[0]
                  / timeMeasure.getElapsedSeconds()));

          LOG.info("{} models of approx. {} documents created ({}s, {}%). "
                  + "Time left {}.", runStatus[0], runStatus[2],
                  runTimeMeasure.
                  getElapsedSeconds(), ++runStatus[3], TimeMeasure.
                  getTimeString(estimate));

          runTimeMeasure.start();
        }
      }

      // clean up
      for (int i = 0; i < DOCMODEL_CREATOR_THREADS; i++) {
        dmcThreads[i].terminate();
      }
      // wait until all waiting models are processed
      trackingLatch.await();
    } catch (InterruptedException ex) {
      LOG.error("Model creation thread interrupted. "
              + "This may have caused data corruption.", ex);
    }

    timeMeasure.stop();
    LOG.info("Calculation of document models for {} documents took {}.",
            runStatus[0], timeMeasure.getElapsedTimeString());
  }

  /**
   * Calculates the relative term frequency for each term in the index.
   * Overall term frequency values must be calculated beforehand by calling
   * {@link AbstractIndexDataProvider#calculateTermFrequencies(IndexReader)}.
   *
   * @param terms List of terms to do the calculation for. Usually this is a
   * list of all terms known from the index.
   */
  protected final void calculateRelativeTermFrequencies(
          final Set<BytesWrap> terms) {
    if (terms == null) {
      throw new IllegalArgumentException("Term set was null.");
    }
    LOG.info("Calculating relative term frequencies for {} terms.", terms.
            size());
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    final double cFreq = (double) getTermFrequency();
    Long termFreq;

    // show a progress status in 1% steps
    int[] runStatus = new int[]{0, (int) (terms.size() * 0.01), terms.size(),
      0}; // counter, step, max, percentage
    TimeMeasure runTimeMeasure = new TimeMeasure().start();

    for (BytesWrap term : terms) {
      // debug operating indicator
      if (runStatus[0] >= 0 && ++runStatus[0] % runStatus[1] == 0) {
        LOG.info("{} of {} terms calculated ({}s, {}%)", runStatus[0],
                runStatus[2], runTimeMeasure.stop().getElapsedSeconds(),
                ++runStatus[3]);
        runTimeMeasure.start();
      }

      termFreq = getTermFrequency(term);
      if (termFreq != null) {
        final double rTermFreq = (double) termFreq / cFreq;
        setTermFreqValue(term, rTermFreq);
      }
    }

    timeMeasure.stop();
    LOG.info("Calculation of relative term frequencies "
            + "for {} unique terms in index took {}.",
            getTermsCount(), timeMeasure.getElapsedTimeString());
  }

  @Override
  public final String[] getTargetFields() {
    return this.fields.clone();
  }

  /**
   * Get the document fields this {link IndexDataProvider} accesses.
   *
   * @return Array of document field names
   */
  public final String[] getFields() {
    return fields.clone();
  }

  /**
   * Set the document fields this {@link IndexDataProvider} accesses for
   * statics calculation. Note that changing fields while the values are
   * calculated may render the calculation results invalid. You should call
   * {@link AbstractIndexDataProvider#clearData()} to remove any
   * pre-calculated data if fields have changed and recalculate values as
   * needed.
   *
   * @param newFields List of field names
   */
  protected final void setFields(final String[] newFields) {
    if (newFields == null || newFields.length == 0) {
      throw new IllegalArgumentException("Empty fields specified.");
    }
    this.fields = newFields.clone();
  }

  /**
   * Worker thread to create {@link DocumentModel}s.
   */
  private final class DocumentModelCreator implements Runnable {

    /**
     * Prefix used to store configuration.
     */
    private static final String CONF_PREFIX = "DMCreator_";
    /**
     * Gather terms found in the document.
     */
    private Map<BytesWrap, Number> docTerms;
    /**
     * Enumerator enumerating over all specified document fields.
     */
    private final DocFieldsTermsEnum dftEnum;
    /**
     * Name of this thread.
     */
    private String tName;
    /**
     * Termination flag.
     */
    private boolean terminate = false;
    /**
     * Queue of doc-ids waiting to get processed.
     */
    private final BlockingQueue<Integer> docQueue;
    /**
     * Global latch to track running threads.
     */
    private final CountDownLatch latch;
    /**
     * Maximum time (s) to wait for a new document-id to become available.
     */
    private final int maxWait = ClarityScoreConfiguration.INSTANCE.
            getInt(CONF_PREFIX + "docIdMaxWait", 1);

    /**
     * Create a new worker thread.
     *
     * @param reader Index reader, to access the Lucene index
     * @param documentQueue Queue of documents to process
     * @param trackingLatch Shared latch to track running threads
     * @throws IOException Thrown on low-level I/O errors
     */
    public DocumentModelCreator(final IndexReader reader,
            final BlockingQueue<Integer> documentQueue,
            final CountDownLatch trackingLatch) throws IOException {
      if (reader == null) {
        throw new IllegalArgumentException("Reader was null.");
      }
      if (documentQueue == null) {
        throw new IllegalArgumentException("Queue was null.");
      }
      if (trackingLatch == null) {
        throw new IllegalArgumentException("Latch was null.");
      }
      this.dftEnum = new DocFieldsTermsEnum(reader,
              AbstractIndexDataProvider.this.fields);
      this.docQueue = documentQueue;
      this.latch = trackingLatch;
    }

    /**
     * Set the termination flag for this thread causing it to finish the
     * current work and exit.
     */
    public void terminate() {
      LOG.debug("({}) Thread got terminating signal.", this.tName);
      this.terminate = true;
    }

    @Override
    public void run() {
      this.tName = Thread.currentThread().getName();
      LOG.debug("({}) Runnable starting", this.tName);
      final long[] docTermEsitmate = new long[]{0L, 0L, 100L};

      Integer docId;
      BytesRef bytesRef;
      try {
        while (!Thread.currentThread().isInterrupted() && !(this.terminate
                && this.docQueue.isEmpty())) {
          docId = this.docQueue.poll(maxWait, TimeUnit.SECONDS);
          if (docId == null) {
            LOG.debug("({}) No doc received in {}s", maxWait);
            continue;
          }

          try {
            // go through all document fields..
            dftEnum.setDocument(docId);
          } catch (IOException ex) {
            LOG.error("Error retrieving document id={}.", docId, ex);
            continue;
          }

          try {
            // iterate over all terms in all specified fields
            bytesRef = dftEnum.next();
            if (bytesRef == null) {
              // nothing found
              continue;
            }
            docTerms = new HashMap<>((int) docTermEsitmate[2]);
            while (bytesRef != null) {
              final BytesWrap term = new BytesWrap(bytesRef);

              // update frequency counter for current term
              if (!docTerms.containsKey(term)) {
                docTerms.put(term.clone(), new AtomicLong(dftEnum.
                        getTotalTermFreq()));
              } else {
                ((AtomicLong) docTerms.get(term)).getAndAdd(dftEnum.
                        getTotalTermFreq());
              }
              LOG.trace("TermCount doc={} term={} count={}", docId, bytesRef.
                      utf8ToString(), dftEnum.getTotalTermFreq());

              bytesRef = dftEnum.next();
            }
          } catch (IOException ex) {
            LOG.error("Error while getting terms for document id {}", docId,
                    ex);
            continue;
          }

          final DocumentModel.DocumentModelBuilder dmBuilder
                  = new DocumentModel.DocumentModelBuilder(docId,
                          docTerms.size());

          // All terms from all document fields are gathered.
          // Store the document frequency of each document term to the model
          dmBuilder.setTermFrequency(docTerms);

          try {
            if (!AbstractIndexDataProvider.this.addDocumentModel(dmBuilder.
                    getModel())) {
              throw new IllegalArgumentException(
                      "Document model already known at creation time.");
            }
          } catch (Exception ex) {
            LOG.error("Caught exception while adding document model.", ex);
            continue;
          }

          // estimate size for term buffer
          docTermEsitmate[0] += docTerms.size();
          docTermEsitmate[1]++;
          docTermEsitmate[2] = docTermEsitmate[0] / docTermEsitmate[1];
          // take the default load factor into account
          docTermEsitmate[2] += docTermEsitmate[2] * 0.8;
        }
      } catch (InterruptedException ex) {
        LOG.warn("({}) Runnable interrupted.", this.tName, ex);
      }

      this.latch.countDown();
      LOG.debug("({}) Runnable terminated.", this.tName);
    }
  }
}
