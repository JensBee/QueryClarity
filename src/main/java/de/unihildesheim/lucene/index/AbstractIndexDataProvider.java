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
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.TimeMeasure;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 * Default implementation of the {@link IndexDataProvider}. This abstract class
 * creates the basic data structures to handle pre-calculated index data and
 * provides basic accessors functions to those values.
 *
 * The calculation of all term frequency values respect the list of defined
 * document fields. So all values are only calculated for terms found in those
 * fields.
 *
 * The data storage {@link Map} implementations are assumed to be immutable, so
 * stored objects cannot be modified directly and have to be removed and
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
   * Store mapping of <tt>term (bytes)</tt> -> <tt>frequency values</tt>.
   */
  @SuppressWarnings("ProtectedField")
  protected ConcurrentMap<BytesWrap, TermFreqData> termFreqMap;

  /**
   * Store mapping of <tt>document-id</tt> -> {@link DocumentModel}.
   */
  @SuppressWarnings("ProtectedField")
  protected ConcurrentMap<Integer, DocumentModel> docModelMap;

  /**
   * Index fields to operate on.
   */
  private transient String[] fields;

  /**
   * Summed frequency of all terms in the index.
   */
  private transient Long overallTermFreq = null;

  /**
   * Calculate term frequencies for all terms in the index in the initial given
   * fields.
   *
   * @param reader Reader to access the index
   * @throws IOException Thrown, if the index could not be opened
   */
  protected final void calculateTermFrequencies(final IndexReader reader) throws
          IOException {
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    final Fields idxFields = MultiFields.getFields(reader);

    Terms fieldTerms;
    TermsEnum fieldTermsEnum = null;
    BytesRef bytesRef;

    // go through all fields..
    for (String field : this.fields) {
      fieldTerms = idxFields.terms(field);

      // ..check if we have terms..
      if (fieldTerms != null) {
        fieldTermsEnum = fieldTerms.iterator(fieldTermsEnum);

        // ..iterate over them..
        bytesRef = fieldTermsEnum.next();
        while (bytesRef != null) {

          // fast forward seek to term..
          if (fieldTermsEnum.seekExact(bytesRef)) {
            // ..and update the frequency value for term
            updateTermFreqValue(BytesWrap.wrap(bytesRef),
                    fieldTermsEnum.totalTermFreq());
          }
          bytesRef = fieldTermsEnum.next();
        }
      }
    }
    timeMeasure.stop();
    LOG.info("Calculation of term frequencies for {} unique terms in index "
            + "took {} seconds.", termFreqMap.size(), timeMeasure.
            getElapsedSeconds());
  }

  /**
   * Updates the term frequency value for the given term. Thread safe.
   *
   * @param term Term to update
   * @param value Value to add to the currently stored value. If there's no
   */
  private void updateTermFreqValue(final BytesWrap term, final long value) {
    final TermFreqData tfData = new TermFreqData(value);
    term.duplicate();
    TermFreqData oldValue;
    TermFreqData newValue;

    for (;;) {
      oldValue = this.termFreqMap.putIfAbsent(term, tfData);
      if (oldValue == null) {
        // data was not already stored
        break;
      }

      newValue = this.termFreqMap.get(term);
      newValue.addToTotalFreq(value);
      if (this.termFreqMap.replace(term, oldValue, newValue)) {
        // replacing actually worked
        break;
      }
    }
    this.setTermFrequency(this.getTermFrequency() + value);
  }

  /**
   * Updates the relative term frequency value for the given term. Thread safe.
   *
   * @param term Term to update
   * @param value Value to overwrite any previously stored value. If there's no
   * value stored, then it will be set to the specified value.
   */
  private void updateTermFreqValue(final BytesWrap term,
          final double value) {

    TermFreqData tfData;
    TermFreqData newValue;
    for (;;) {
      tfData = this.termFreqMap.get(term);
      if (tfData == null) {
        throw new IllegalStateException("Term " + BytesWrapUtil.
                bytesWrapToString(term) + " not found.");
      }

      newValue = new TermFreqData(tfData.getTotalFreq(), value);
      if (this.termFreqMap.replace(term, tfData, newValue)) {
        // replacing actually worked
        break;
      }
    }
  }

  /**
   * {@inheritdoc} This replaces the specified {@link DocumentModel} without
   * checking for any concurrent modifications. This must be handled externally.
   *
   * @param docModel Document model to replace. The model to replace will be
   * identified by the document-id returned by the model
   */
  @Override
  public final void updateDocumentModel(final DocumentModel docModel) {
    if (this.docModelMap.replace(docModel.getDocId(), docModel) == null) {
      throw new IllegalArgumentException("Document model id=" + docModel.
              getDocId() + " cant't be updated, because it's not known.");
    }
  }

  /**
   * Create the document models used by this instance.
   *
   * Since the used {@link Map} implementation is unknown here, a map with
   * immutable objects is assumed and a modification of already stored entries
   * is prohibited. So an entry has to be removed to be updated.
   *
   * @param modelType {@link DocumentModel} implementation to create
   * @param reader Reader to access the index
   * @throws de.unihildesheim.lucene.document.DocumentModelException Thrown, if
   * the {@link DocumentModel} of the requested type could not be instantiated
   * @throws java.io.IOException Thrown on low-level I7O errors
   */
  protected final void createDocumentModels(
          final Class<? extends DocumentModel> modelType,
          final IndexReader reader) throws DocumentModelException, IOException {
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    // create an enumerator enumerating over all specified document fields
    final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(reader,
            this.fields);
    // gather terms found in the document
    final ConcurrentMap<BytesWrap, AtomicLong> docTerms
            = new ConcurrentHashMap<>(1000);
    final Bits liveDocs = MultiFields.getLiveDocs(reader);
    BytesWrap term;
    DocumentModel docModel;
    BytesRef bytesRef;

    // debug helpers
    int[] dbgStatus = new int[]{-1, 100, reader.maxDoc()};
    int docModelCount = 0;
    TimeMeasure dbgTimeMeasure = null;
    if (LOG.isDebugEnabled() && dbgStatus[2] > dbgStatus[1]) {
      dbgStatus[0] = 0;
      dbgTimeMeasure = new TimeMeasure();
    }

    for (int docId = 0; docId < reader.maxDoc(); docId++) {
      // check if document is deleted
      if (liveDocs != null && !liveDocs.get(docId)) {
        // document is deleted
        continue;
      }

      // document model should be unique - otherwise this is an error
      if (this.docModelMap.containsKey(docId)) {
        throw new IllegalArgumentException(
                "Document model already known at creation time.");
      }

      // debug operating indicator
      if (dbgStatus[0] >= 0 && ++dbgStatus[0] % dbgStatus[1] == 0) {
        LOG.info("{} models of  approx. {} documents created ({}s)",
                dbgStatus[0], dbgStatus[2], dbgTimeMeasure.stop().
                getElapsedSeconds());
        dbgTimeMeasure.start();
      }

      // clear previous cached document term values
      docTerms.clear();
      try {
        // go through all document fields..
        dftEnum.setDocument(docId);

        try {
          bytesRef = dftEnum.next();
          while (bytesRef != null) {
            term = BytesWrap.duplicate(bytesRef);

            // update frequency counter for current term
            if (!docTerms.containsKey(term)) {
              docTerms.put(term, new AtomicLong(0));
            }
            docTerms.get(term).getAndAdd(dftEnum.getTotalTermFreq());
            LOG.trace("TermCount doc={} term={} count={}", docId, bytesRef.
                    utf8ToString(), dftEnum.getTotalTermFreq());

            bytesRef = dftEnum.next();
          }
        } catch (IOException ex) {
          LOG.error("Error while getting terms for document id {}", docId, ex);
        }

        try {
          // create a new model with the given id and
          // the expected number of terms
          docModel = modelType.newInstance();
          docModel.create(docId, docTerms.size());
        } catch (InstantiationException | IllegalAccessException ex) {
          LOG.error("Error creating document model.", ex);
          throw new DocumentModelException(ex);
        }

        // All terms from all document fields are gathered.
        // Store the document frequency of each document term to the model
        for (Map.Entry<BytesWrap, AtomicLong> entry : docTerms.entrySet()) {
          try {
            docModel.setTermFrequency(entry.getKey(), entry.getValue().get());
          } catch (ArrayIndexOutOfBoundsException ex) {
            LOG.error("docId={} bytes={}", docId, entry.getKey());
          }
        }
        docModel.lock(); // make model immutable for storage now
        this.docModelMap.put(docId, docModel);
        if (LOG.isDebugEnabled()) {
          docModelCount++;
        }
      } catch (IOException ex) {
        LOG.error("Error retrieving document id={}.", docId, ex);
      }
    }

    timeMeasure.stop();
    LOG.info("Calculation of document models for {} documents took {} seconds.",
            docModelCount, timeMeasure.getElapsedSeconds());
  }

//  /**
//   * Updates the document model by using it's document-id. This is done by
//   * replacing any previous model associated with the document-id.
//   *
//   * Since the used {@link Map} implementation is unknown here, a map with
//   * immutable objects is assumed and a modification of already stored entries
//   * is prohibited. So an entry has to be removed to be updated.
//   *
//   * @param newDocModel Document model to update. The document id will be
//   * retrieved from this model.
//   */
//  protected final void updateDocumentModel(final DocumentModel newDocModel) {
//    if (newDocModel == null) {
//      return;
//    }
//    final int docId = newDocModel.getDocId();
//    this.docModelMap.remove(docId);
//    this.docModelMap.put(docId, newDocModel);
//  }
  /**
   * Clears all pre-calculated data.
   */
  protected final void clearData() {
    this.docModelMap.clear();
    this.termFreqMap.clear();
  }

  /**
   * Calculates the relative term frequency for each term in the index. Overall
   * term frequency values must be calculated beforehand by calling
   * {@link AbstractIndexDataProvider#calculateTermFrequencies(IndexReader)}.
   */
  protected final void calculateRelativeTermFrequencies() {
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    final double cFreq = (double) getTermFrequency();

    // Create a shallow copy of all term keys from the map. This is needed,
    // because we update the map while iterating through. The set of terms
    // stays the same, as we (should) re-add entries immediately.
    // Using EntrySet to modify entries is prohibited, since we assume immutable
    // entries.
    final Set<BytesWrap> terms = Collections.unmodifiableSet(termFreqMap.
            keySet());
//    double tFreq;
    double rTermFreq;

    // debug helpers
    int[] dbgStatus = new int[]{-1, 10000, terms.size()};
    TimeMeasure dbgTimeMeasure = null;
    if (LOG.isDebugEnabled() && dbgStatus[2] > dbgStatus[1]) {
      dbgStatus[0] = 0;
      dbgTimeMeasure = new TimeMeasure();
    }

    for (BytesWrap term : terms) {
      // debug operating indicator
      if (dbgStatus[0] >= 0 && ++dbgStatus[0] % dbgStatus[1] == 0) {
        LOG.info("{} of {} terms calculated ({}s)", dbgStatus[0],
                dbgStatus[2], dbgTimeMeasure.stop().getElapsedSeconds());
        dbgTimeMeasure.start();
      }

      if (this.termFreqMap.containsKey(term)) {
        rTermFreq = (double) this.termFreqMap.get(term).getTotalFreq() / cFreq;
        updateTermFreqValue(term, rTermFreq);
      }
    }

    timeMeasure.stop();
    LOG.info("Calculation of relative term frequencies "
            + "for {} unique terms in index took {} seconds.", termFreqMap.
            size(), timeMeasure.getElapsedSeconds());
  }

  @Override
  public final long getTermFrequency() {
    if (this.overallTermFreq == null) {
      LOG.info("Collection term frequency not found. Need to recalculate.");
      this.overallTermFreq = 0L;
      for (TermFreqData freq : this.termFreqMap.values()) {
        if (freq == null) {
          // value should never be null
          throw new IllegalStateException("Frequency value was null.");
        } else {
          this.overallTermFreq += freq.getTotalFreq();
        }
      }
      LOG.debug("Calculated collection term frequency: {}",
              this.overallTermFreq);
    }

    return this.overallTermFreq;
  }

  @Override
  public final long getTermFrequency(final BytesWrap term) {
    final TermFreqData freq = this.termFreqMap.get(term);
    long value;

    if (freq == null) {
      // term not found
      value = 0L;
    } else {
      value = freq.getTotalFreq();
    }

    LOG.trace("TermFrequency t={} f={}", term, value);
    return value;
  }

  @Override
  public final double getRelativeTermFrequency(final BytesWrap term) {
    final TermFreqData freq = this.termFreqMap.get(term);
    double value;

    if (freq == null) {
      // term not found
      value = 0d;
    } else {
      value = freq.getRelFreq();
    }

    return value;
  }

  @Override
  public final int getTermsCount() {
    return this.termFreqMap.size();
  }

  @Override
  public final Iterator<BytesWrap> getTermsIterator() {
    return Collections.unmodifiableSet(this.termFreqMap.keySet()).iterator();
  }

  @Override
  public final String[] getTargetFields() {
    return this.fields.clone();
  }

  @Override
  public final DocumentModel getDocumentModel(final int docId) {
    return this.docModelMap.get(docId);
  }

  @Override
  public final Iterator<DocumentModel> getDocModelIterator() {
    return Collections.unmodifiableCollection(this.docModelMap.values()).
            iterator();
  }

  @Override
  public final int getDocModelCount() {
    return this.docModelMap.size();
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
   * Set the document fields this {@link IndexDataProvider} accesses for statics
   * calculation. Note that changing fields while the values are calculated may
   * render the calculation results invalid. You should call
   * {@link AbstractIndexDataProvider#clearData()} to remove any pre-calculated
   * data if fields have changed and recalculate values as needed.
   *
   * @param newFields List of field names
   */
  protected final void setFields(final String[] newFields) {
    this.fields = newFields.clone();
  }

  /**
   * Set the overall frequency of all terms in the index.
   *
   * @param oTermFreq Overall frequency of all terms in the index
   */
  protected final void setTermFrequency(final Long oTermFreq) {
    this.overallTermFreq = oTermFreq;
  }

  @Override
  public final void addDocumentModel(final DocumentModel docModel) {
    if (docModel != null) {
      this.docModelMap.put(docModel.getDocId(), docModel);
    }
  }

  @Override
  public final DocumentModel removeDocumentModel(final int docId) {
    return this.docModelMap.remove(docId);
  }
}
