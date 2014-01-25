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
import de.unihildesheim.util.TimeMeasure;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
  protected Map<byte[], TermFreqData> termFreqMap;

  /**
   * Store mapping of <tt>document-id</tt> -> {@link DocumentModel}.
   */
  protected Map<Integer, DocumentModel> docModelMap;

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
    String term;

    // go through all fields..
    for (String field : this.fields) {
      fieldTerms = idxFields.terms(field);

      // ..check if we have terms..
      if (fieldTerms != null) {
        fieldTermsEnum = fieldTerms.iterator(fieldTermsEnum);

        // ..iterate over them,,
        bytesRef = fieldTermsEnum.next();
        while (bytesRef != null) {

          // fast forward seek to term..
          if (fieldTermsEnum.seekExact(bytesRef)) {
            // ..and update the frequency value for term
            updateTermFreqValue(bytesRef.bytes, fieldTermsEnum.totalTermFreq());
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
   * Updates the term frequency value for the given term.
   *
   * Since the used {@link Map} implementation is unknown here, a map with
   * immutable objects is assumed and a modification of already stored entries
   * is prohibited. So an entry has to be removed to be updated.
   *
   * @param term Term to update
   * @param value Value to add to the currently stored value. If there's no
   */
  protected abstract void updateTermFreqValue(final byte[] term,
          final long value);

  /**
   * Updates the relative term frequency value for the given term.
   *
   * Since the used {@link Map} implementation is unknown here, a custom
   * implementation is needed.
   *
   * @param term Term to update
   * @param value Value to overwrite any previously stored value. If there's no
   * value stored, then it will be set to the specified value.
   */
  protected abstract void updateTermFreqValue(final byte[] term,
          final double value);

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
   */
  protected final void createDocumentModels(
          final Class<? extends DocumentModel> modelType,
          final IndexReader reader) throws DocumentModelException {
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    // create an enumerator enumarating over all specified document fields
    final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(reader,
            this.fields);
    // gather terms found in the document
//    final Map<String, Long> docTerms = new HashMap(1000);
    final Map<byte[], Long> docTerms = new HashMap(1000);
    Long termCount;
    final Bits liveDocs = MultiFields.getLiveDocs(reader);
//    String term;
    byte[] term;
    DocumentModel docModel;
    long docTermFrequency;
    BytesRef bytesRef;

    for (int docId = 0; docId < reader.maxDoc(); docId++) {
      // check if document is deleted
      if (liveDocs != null && !liveDocs.get(docId)) {
        // document is deleted
        continue;
      }

      // clear previous cached document term values
      docTerms.clear();

      // go through all document fields..
      dftEnum.setDocument(docId);
      try {
        bytesRef = dftEnum.next();
        while (bytesRef != null) {
          // get the document frequency of the current term
          docTermFrequency = dftEnum.getTotalTermFreq();

          // get string representation of current term
//          try {
          term = bytesRef.bytes.clone();
//          } catch (ArrayIndexOutOfBoundsException ex) {
//            LOG.error("docId={} bytes={} hex={} length={} offset={}", docId,
//                    bytesRef.bytes, bytesRef.toString(), bytesRef.length,
//                    bytesRef.offset);
//            term = null;
//          }

//          if (term != null) {
          // update frequency counter for current term
          termCount = docTerms.get(term);
          if (termCount == null) {
            termCount = docTermFrequency;
          } else {
            termCount += docTermFrequency;
          }

          docTerms.put(term, termCount);
//          }
          bytesRef = dftEnum.next();
        }
      } catch (IOException ex) {
        LOG.error("Error while getting terms for document id {}", docId, ex);
      }

      // document model should be unique - otherwise this is an error
      if (this.docModelMap.containsKey(docId)) {
        throw new IllegalArgumentException(
                "Document model already known at creation time.");
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
//      for (Map.Entry<String, Long> entry : docTerms.entrySet()) {
      for (Map.Entry<byte[], Long> entry : docTerms.entrySet()) {
        try {
          docModel.setTermFrequency(entry.getKey(), entry.getValue());
        } catch (ArrayIndexOutOfBoundsException ex) {
          LOG.error("docId={} bytes={} hex={} length={}", docId,
                  entry.getKey(), entry.getKey().toString(), entry.
                  getKey().length);
        }
      }
      docModel.lock(); // make model immutable for storage now
      this.docModelMap.put(docId, docModel);
    }

    timeMeasure.stop();
    LOG.info("Calculation of document models for {} documents took {} seconds.",
            this.docModelMap.size(), timeMeasure.getElapsedSeconds());
  }

  /**
   * Updates the document model by using it's document-id. This is done by
   * replacing any previous model associated with the document-id.
   *
   * Since the used {@link Map} implementation is unknown here, a map with
   * immutable objects is assumed and a modification of already stored entries
   * is prohibited. So an entry has to be removed to be updated.
   *
   * @param newDocModel Document model to update. The document id will be
   * retrieved from this model.
   */
  protected final void updateDocumentModel(final DocumentModel newDocModel) {
    if (newDocModel == null) {
      return;
    }
    final int docId = newDocModel.getDocId();
    this.docModelMap.remove(docId);
    this.docModelMap.put(docId, newDocModel);
  }

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
    final Set<byte[]> terms = new HashSet(termFreqMap.size());
    terms.addAll(termFreqMap.keySet());

    double tFreq;
    double rTermFreq;
    for (byte[] term : terms) {
      tFreq = (double) getTermFrequency(term);
      rTermFreq = tFreq / cFreq;
      updateTermFreqValue(term, rTermFreq);
    }

    timeMeasure.stop();
    LOG.info("Calculation of relative term frequencies "
            + "for {} unique terms in index took {} seconds.", termFreqMap.
            size(), timeMeasure.getElapsedSeconds());
  }

  @Override
  public final long getTermFrequency() {
    if (this.overallTermFreq == null) {
      this.overallTermFreq = 0L;
      for (TermFreqData freq : this.termFreqMap.values()) {
        if (freq == null) {
          // value should never be null
          throw new IllegalStateException("Frequency value was null.");
        } else {
          this.overallTermFreq += freq.getTotalFreq();
        }
      }
      LOG.debug("Calculated overall term frequency: {}", this.overallTermFreq);
    }

    return this.overallTermFreq;
  }

  @Override
  public final long getTermFrequency(final byte[] term) {
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
  public final double getRelativeTermFrequency(final byte[] term) {
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
  public final Iterator<byte[]> getTermsIterator() {
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
  public int getDocModelCount() {
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
   * Get the overall frequency of all terms in the index.
   *
   * @return Overall frequency of all terms in the index
   */
  public final Long getOverallTermFreq() {
    return overallTermFreq;
  }

  /**
   * Set the overall frequency of all terms in the index.
   *
   * @param oTermFreq Overall frequency of all terms in the index
   */
  protected final void setOverallTermFreq(final Long oTermFreq) {
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
