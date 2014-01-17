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

import de.unihildesheim.lucene.document.DefaultDocumentModel;
import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.DocumentModel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
 * provides basic accessor functions to those values.
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
   * Store mapping of <tt>term</tt> -> <tt>frequency values</tt>
   */
  private Map<String, TermFreqData> termFreq;

  /**
   * Store mapping of <tt>document-id</tt> -> {@link DocumentModel}.
   */
  private Map<Integer, DocumentModel> docModels;

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
    final long startTime = System.nanoTime();
    final Fields idxFields = MultiFields.getFields(reader); // NOPMD

    Terms fieldTerms;
    TermsEnum fieldTermsEnum = null; // NOPMD
    BytesRef bytesRef;

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
            final String term = bytesRef.utf8ToString();

            // ..and update the frequency value for term
            updateTermFreqValue(term, fieldTermsEnum.totalTermFreq());
          }
          bytesRef = fieldTermsEnum.next();
        }
      }
    }
    final double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.info("Calculation of term frequencies for {} terms in index "
            + "took {} seconds.", termFreq.size(), estimatedTime);
  }

  /**
   * Updates the term frequency value for the given term.
   *
   * The used {@link Map} implementation is unknown here. Therefore a map with
   * immutable objects is assumed and a modification of already stored entries
   * is permitted. So an entry has to be removed to be updated.
   *
   * @param term Term to update
   * @param value Value to add to the currently stored value. If there's no
   */
  protected final void updateTermFreqValue(final String term,
          final long value) {
    TermFreqData freq = this.getTermFreq().remove(term);

    if (freq == null) {
      freq = new TermFreqData(value);
    } else {
      // add new value to the already stored value
      freq.addFreq(value);
    }
    this.getTermFreq().put(term, freq);

    // reset overall value
    this.setOverallTermFreq(null); // force recalculation
  }

  /**
   * Updates the relative term frequency value for the given term.
   *
   * The used {@link Map} implementation is unknown here. Therefore a map with
   * immutable objects is assumed and a modification of already stored entries
   * is permitted. So an entry has to be removed to be updated.
   *
   * @param term Term to update
   * @param value Value to overwrite any previously stored value. If there's no
   * value stored, then it will be set to the specified value.
   */
  protected void updateTermFreqValue(final String term, final double value) {
    TermFreqData freq = this.getTermFreq().remove(term);

    if (freq == null) {
      freq = new TermFreqData(value);
    } else {
      // overwrite relative term freqency value
      freq.setRelFreq(value);
    }
    this.getTermFreq().put(term, freq);
  }

  /**
   * Create the document models used by this instance.
   *
   * The used {@link Map} implementation is unknown here. Therefore a map with
   * immutable objects is assumed and a modification of already stored entries
   * is permitted. So an entry has to be removed to be updated.
   *
   * @param reader Reader to access the index
   * @param fields Database fields to use
   */
  protected final void createDocumentModels(final IndexReader reader,
          final String[] fields) {
    final long startTime = System.nanoTime();
    // create an enumerator enumarating over all specified document fields
    final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(reader, fields);

    // gather terms found in the document
    final Map<String, Long> docTerms = new HashMap(1000);

    Long termCount;

    final Bits liveDocs = MultiFields.getLiveDocs(reader); // NOPMD
    for (int docId = 0; docId < reader.maxDoc(); docId++) {
      // check if document is deleted
      if (liveDocs != null && !liveDocs.get(docId)) {
        // document is deleted
        continue;
      }

      // get/create the document model for the current document
      DocumentModel docModel = this.getDocModels().remove(docId);
      if (docModel == null) {
        docModel = new DefaultDocumentModel(docId, this.getTermFreq().size());
      }

      // clear previous cached document term values
      docTerms.clear();

      // go through all document fields..
      dftEnum.setDocument(docId);
      try {
        BytesRef bytesRef = dftEnum.next();
        while (bytesRef != null) {
          // get the document frequency of the current term
          final long docTermFrequency = dftEnum.getTotalTermFreq();

          // get string representation of current term
          final String term = bytesRef.utf8ToString();

          // update frequency counter for current term
          termCount = docTerms.get(term);
          if (termCount == null) {
            termCount = docTermFrequency;
          } else {
            termCount += docTermFrequency;
          }

          docTerms.put(term, termCount);
          bytesRef = dftEnum.next();
        }
      } catch (IOException ex) {
        LOG.error("Error while getting terms for document id {}", docId, ex);
      }

      // All terms from all document fields are gathered.
      // Store the document frequency of each document term to the model
      for (Map.Entry<String, Long> entry : docTerms.entrySet()) {
        docModel.setTermFrequency(entry.getKey(), entry.getValue());
      }
      this.getDocModels().put(docId, docModel);
    }

    final double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.info("Calculation of document models for {} documents took {} seconds.",
            this.getDocModels().size(), estimatedTime);
  }

  /**
   * Clears all pre-calculated data.
   */
  protected final void clearData() {
    this.docModels.clear();
    this.termFreq.clear();
  }

  /**
   * Calculates the relative term frequency for each term in the index. Overall
   * term frequency values must be calculated beforehand by calling
   * {@link AbstractIndexDataProvider#calculateTermFrequencies(IndexReader)}.
   */
  protected final void calculateRelativeTermFrequencies() {
    final long startTime = System.nanoTime();

    final double cFreq = (double) getTermFrequency(); // NOPMD

    for (String term : termFreq.keySet()) {
      final double tFreq = (double) getTermFrequency(term);
      final double rTermFreq = tFreq / cFreq;
      updateTermFreqValue(term, rTermFreq);
    }

    final double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.info("Calculation of relative term frequencies "
            + "for {} terms in index took {} seconds.", termFreq.size(),
            estimatedTime);
  }

  @Override
  public final long getTermFrequency() {
    if (this.overallTermFreq == null) {
      this.overallTermFreq = 0L;
      for (TermFreqData freq : this.termFreq.values()) {
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
  public final long getTermFrequency(final String term) {
    final TermFreqData freq = this.termFreq.get(term);
    long value;

    if (freq == null) {
      // term not found
      value = 0L;
    } else {
      value = freq.getTotalFreq();
    }

    return value;
  }

  @Override
  public final double getRelativeTermFrequency(final String term) {
    final TermFreqData freq = this.termFreq.get(term);
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
  public Iterator<String> getTermsIterator() {
    return this.termFreq.keySet().iterator();
  }

  @Override
  public final String[] getTargetFields() {
    return this.fields.clone();
  }

  @Override
  public final DocumentModel getDocumentModel(final int docId) {
    return this.docModels.get(docId);
  }

  @Override
  public final Iterator<DocumentModel> getDocModelIterator() {
    return this.docModels.values().iterator();
  }

  /**
   * Get the document models known by this instance. This map may be huge. You
   * should use {@link AbstractIndexDataProvider#getDocModelIterator()} instead.
   *
   * @return Immutable map with <tt>document-id -> {@link DocumentModel}</tt>
   * mapping
   */
  public final Map<Integer, DocumentModel> getDocModels() {
    return docModels;
  }

  /**
   * Set the document models known by this instance.
   *
   * @param newDocModels Document models
   */
  public final void setDocModels(final Map<Integer, DocumentModel> newDocModels) {
    this.docModels = newDocModels;
  }

  /**
   * Get the calculated frequency values for each term in the index.
   *
   * @return Immutable map with <tt>Term->(Long) overall index frequency,
   * (Double) relative index frequency</tt> mapping for every term in the index
   */
  public final Map<String, TermFreqData> getTermFreq() {
    return termFreq;
  }

  /**
   * Set the calculated frequency values for each term in the index. Note that
   * <tt>null</tt> is not allowed for any value.
   *
   * @param newTermFreq Mapping of <tt>Term->(Long) overall index frequency,
   * (Double) relative index frequency</tt> for every term in the index
   */
  public final void setTermFreq(final Map<String, TermFreqData> newTermFreq) {
    this.termFreq = newTermFreq;
  }

  /**
   * Get the document fields this {link IndexDataProvider} accesses.
   *
   * @return Arry of document field names
   */
  public final String[] getFields() {
    return fields.clone();
  }

  /**
   * Set the document fields this {link IndexDataProvider} accesses for statics
   * calculation. Note that changing fields while the values are calculated may
   * render the calculation results invalid. You should call
   * {@link AbstractIndexDataProvider#clearData()} to remove any pre-calculated
   * data if fields have changed and recolculate values as needed.
   *
   * @param newFields List of field names
   */
  public final void setFields(final String[] newFields) {
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
  public final void setOverallTermFreq(final Long oTermFreq) {
    this.overallTermFreq = oTermFreq;
  }
}
