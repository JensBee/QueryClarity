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
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.DocumentModelException;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.Processing;
import de.unihildesheim.util.Processing.CollectionSource;
import de.unihildesheim.util.Processing.Source;
import de.unihildesheim.util.Processing.Target;
import de.unihildesheim.util.ProcessingException;
import de.unihildesheim.util.TimeMeasure;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
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
   * Index fields to operate on.
   */
  private transient String[] fields = new String[0];

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

    new Processing(
            new DocModelCreator(new DocModelCreatorSource(reader), reader)
    ).process();
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
          final Collection<BytesWrap> terms) {
    if (terms == null) {
      throw new IllegalArgumentException("Term set was null.");
    }
    LOG.info("Calculating relative term frequencies for {} terms.", terms.
            size());

    new Processing(
            new RelTermFreqCalculator(new CollectionSource(terms))
    ).process();
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
   * {@link Processing.Source} providing document-ids to create document
   * models.
   */
  private final class DocModelCreatorSource extends Processing.Source<Integer>
          implements Processing.ObservableSource {

    /**
     * Expected number of documents to retrieve from Lucene.
     */
    private final int itemsCount;
    /**
     * Current number of items provided.
     */
    private int currentNum;

    /**
     * Create a new {@link Processing.Source} providing document-ids. Used to
     * generate document models.
     *
     * @param newReader Reader to access Lucene index
     */
    DocModelCreatorSource(final IndexReader newReader) {
      super();
      this.itemsCount = newReader.maxDoc();
      this.currentNum = -1;
    }

    @Override
    public synchronized Integer next() throws ProcessingException {
      Integer nextNum = null;
      if (++this.currentNum < itemsCount) {
        nextNum = this.currentNum;
      } else {
        stop();
      }
      return nextNum;
    }

    @Override
    public Integer getItemCount() {
      return this.itemsCount;
    }

    @Override
    public int getSourcedItemCount() {
      return this.currentNum < 0 ? 0 : this.currentNum;
    }
  }

  /**
   * {@link Processing.Target} create document models from a document-id
   * {@link Processing.Source}.
   */
  private final class DocModelCreator extends Processing.Target<Integer> {

    /**
     * Name to identify this {@link Runnable}.
     */
    private final String rId = "(" + DocModelCreator.class + "-" + this.
            hashCode() + ")";
    /**
     * Flag to indicate, if this {@link Runnable} should terminate.
     */
    private volatile boolean terminate;
    /**
     * Shared latch to track running threads.
     */
    private final CountDownLatch latch;
    /**
     * Reader to access Lucene index.
     */
    private final IndexReader reader;

    /**
     * Base constructor without setting a {@link CountDownLatch}. This
     * instance is not able to be run.
     *
     * @param newSource {@link Source} for this {@link Target}
     * @param newReader Reader to access Lucene index
     */
    public DocModelCreator(final Processing.Source<Integer> newSource,
            final IndexReader newReader) {
      super(newSource);
      this.terminate = false;
      this.reader = newReader;
      this.latch = null;
    }

    /**
     * Creates a new instance able to run. Meant to be called from the factory
     * method.
     *
     * @param source {@link Source} for this {@link Target}
     * @param newLatch Shared latch to track running threads
     * @param newReader Reader to access Lucene index
     */
    private DocModelCreator(final Processing.Source<Integer> source,
            final CountDownLatch newLatch, final IndexReader newReader) {
      super(source);
      this.terminate = false;
      this.reader = newReader;
      this.latch = newLatch;
    }

    @Override
    public void terminate() {
      LOG.debug("{} Received termination signal.", this.rId);
      this.terminate = true;
    }

    @Override
    public Processing.Target<Integer> newInstance(
            final CountDownLatch newLatch) {
      return new DocModelCreator(getSource(), newLatch, this.reader);
    }

    @Override
    public void run() {
      try {
        LOG.debug("{} Starting.", this.rId);
        final long[] docTermEsitmate = new long[]{0L, 0L, 100L};

        final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(this.reader,
                getFields());

        BytesRef bytesRef;
        Map<BytesWrap, Number> docTerms;

        while (!this.terminate && getSource().isRunning()) {
          final Integer docId = getSource().next();
          if (docId == null) {
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
      } catch (ProcessingException.SourceHasFinishedException ex) {
        LOG.debug("Source has finished unexpectedly.");
      } catch (Exception ex) {
        LOG.debug("{} Caught exception. Terminating.", this.rId, ex);
      } finally {
        this.latch.countDown();
      }
    }
  }

  /**
   * {@link Processing.Target} create document models from a document-id
   * {@link Processing.Source}.
   */
  private final class RelTermFreqCalculator
          extends Processing.Target<BytesWrap> {

    /**
     * Name to identify this {@link Runnable}.
     */
    private final String rId = "(" + DocModelCreator.class + "-" + this.
            hashCode() + ")";
    /**
     * Flag to indicate, if this {@link Runnable} should terminate.
     */
    private volatile boolean terminate;
    /**
     * Shared latch to track running threads.
     */
    private final CountDownLatch latch;

    /**
     * Base constructor without setting a {@link CountDownLatch}. This
     * instance is not able to be run.
     *
     * @param source {@link Source} for this {@link Target}
     */
    public RelTermFreqCalculator(final Processing.Source<BytesWrap> source) {
      super(source);
      this.terminate = false;
      this.latch = null;
    }

    /**
     * Creates a new instance able to run. Meant to be called from the factory
     * method.
     *
     * @param source {@link Source} for this {@link Target}
     * @param newLatch Shared latch to track running threads
     */
    private RelTermFreqCalculator(final Processing.Source<BytesWrap> source,
            final CountDownLatch newLatch) {
      super(source);
      this.terminate = false;
      this.latch = newLatch;
    }

    @Override
    public void terminate() {
      LOG.debug("{} Received termination signal.", this.rId);
      this.terminate = true;
    }

    @Override
    public Target<BytesWrap> newInstance(final CountDownLatch newLatch) {
      return new RelTermFreqCalculator(getSource(), newLatch);
    }

    @Override
    public void run() {
      final long collFreq = getTermFrequency();
      try {
        while (!this.terminate && getSource().isRunning()) {
          try {
            final BytesWrap term = getSource().next();
            if (term == null) {
              // nothing found
              continue;
            }
            final Long termFreq = getTermFrequency(term);
            if (termFreq != null) {
              final double rTermFreq = (double) termFreq / collFreq;
              setTermFreqValue(term, rTermFreq);
            }
          } catch (Exception ex) {
            LOG.debug("{} Caught exception while processing term.", this.rId,
                    ex);
          }
        }
      } catch (ProcessingException.SourceHasFinishedException ex) {
        LOG.debug("Source has finished unexpectedly.");
      } finally {
        this.latch.countDown();
      }
    }
  }
}
