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
import java.io.IOException;
import java.util.Collection;
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
   * Store mapping of <tt>term</tt> -> <tt>frequency values</tt>.
   */
  private Map<String, TermFreqData> termFreqMap;

  /**
   * Store mapping of <tt>document-id</tt> -> {@link DocumentModel}.
   */
  private Map<Integer, DocumentModel> docModelMap;

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
            + "took {} seconds.", termFreqMap.size(), estimatedTime);
  }

  /**
   * Updates the term frequency value for the given term.
   *
   * Since the used {@link Map} implementation is unknown here, a map with
   * immutable objects is assumed and a modification of already stored entries
   * is permitted. So an entry has to be removed to be updated.
   *
   * @param term Term to update
   * @param value Value to add to the currently stored value. If there's no
   */
  protected final void updateTermFreqValue(final String term,
          final long value) {
    TermFreqData freq = this.termFreqMap.remove(term);

    if (freq == null) {
      freq = new TermFreqData(value);
    } else {
      // add new value to the already stored value
      freq = freq.addToTotalFreq(value);
    }
    this.termFreqMap.put(term, freq);

    // reset overall value
    this.setOverallTermFreq(null); // force recalculation
  }

  /**
   * Updates the relative term frequency value for the given term.
   *
   * Since the used {@link Map} implementation is unknown here, a map with
   * immutable objects is assumed and a modification of already stored entries
   * is permitted. So an entry has to be removed to be updated.
   *
   * @param term Term to update
   * @param value Value to overwrite any previously stored value. If there's no
   * value stored, then it will be set to the specified value.
   */
  protected final void updateTermFreqValue(final String term,
          final double value) {
    TermFreqData freq = this.termFreqMap.remove(term);

    if (freq == null) {
      freq = new TermFreqData(value);
    } else {
      // overwrite relative term freqency value
      freq = freq.setRelFreq(value);
    }
    this.termFreqMap.put(term, freq);
  }

  /**
   * Create the document models used by this instance.
   *
   * Since the used {@link Map} implementation is unknown here, a map with
   * immutable objects is assumed and a modification of already stored entries
   * is permitted. So an entry has to be removed to be updated.
   *
   * @param modelType {@link DocumentModel} implementation to create
   * @param reader Reader to access the index
   * @throws de.unihildesheim.lucene.document.DocumentModelException Thrown, if
   * the {@link DocumentModel} of the requested type could not be instantiated
   */
  protected final void createDocumentModels(
          final Class<? extends DocumentModel> modelType,
          final IndexReader reader) throws DocumentModelException {
    final long startTime = System.nanoTime();
    // create an enumerator enumarating over all specified document fields
    final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(reader,
            this.fields);

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
      DocumentModel docModel = this.docModelMap.remove(docId);
      if (docModel == null) {
        try {
          docModel = modelType.newInstance().setDocId(docId);
        } catch (InstantiationException | IllegalAccessException ex) {
          LOG.error("Error creating document model.", ex);
          throw new DocumentModelException(ex);
        }
//        docModel.setDocId(docId);
//        docModel.setTermCount(this.termFreqMap.size());
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
        docModel = docModel.addTermFrequency(entry.getKey(), entry.getValue());
      }
      this.docModelMap.put(docId, docModel);
    }

    final double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.info("Calculation of document models for {} documents took {} seconds.",
            this.docModelMap.size(), estimatedTime);
  }

  /**
   * Updates the document model by using it's document-id. This is done by
   * replacing any previous model associated with the document-id.
   *
   * Since the used {@link Map} implementation is unknown here, a map with
   * immutable objects is assumed and a modification of already stored entries
   * is permitted. So an entry has to be removed to be updated.
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
    final long startTime = System.nanoTime();

    final double cFreq = (double) getTermFrequency(); // NOPMD

    for (String term : termFreqMap.keySet()) {
      final double tFreq = (double) getTermFrequency(term);
      final double rTermFreq = tFreq / cFreq;
      updateTermFreqValue(term, rTermFreq);
    }

    final double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.info("Calculation of relative term frequencies "
            + "for {} terms in index took {} seconds.", termFreqMap.size(),
            estimatedTime);
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
  public final long getTermFrequency(final String term) {
    final TermFreqData freq = this.termFreqMap.get(term);
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
  public final Iterator<String> getTermsIterator() {
    return Collections.unmodifiableMap(termFreqMap).keySet().iterator();
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
    return this.docModelMap.values().iterator();
//    return new Iterator<DocumentModel>() {
//      private final Iterator<DocumentModel> dmIt;
//
//      {
//        this.dmIt = AbstractIndexDataProvider.this.docModelMap.values().
//                iterator();
//      }
//
//      @Override
//      public boolean hasNext() {
//        return dmIt.hasNext();
//      }
//
//      @Override
//      public DocumentModel next() {
//        return new ImmutableDocumentModel(dmIt.next());
//      }
//
//      @Override
//      public void remove() {
//        throw new UnsupportedOperationException("Not supported.");
//      }
//    };
  }

  /**
   * Get the document models known by this instance. This map may be huge. You
   * should use {@link AbstractIndexDataProvider#getDocModelIterator()} instead.
   *
   * FIXME: map objects should be immutable too.
   *
   * @return Immutable map with <tt>document-id -> {@link DocumentModel}</tt>
   * mapping
   */
  protected final Map<Integer, DocumentModel> getDocModelMap() {
    return Collections.unmodifiableMap(docModelMap);
  }

  @Override
  public final Collection<DocumentModel> getDocModels() {
    final Set<DocumentModel> modelSet = new HashSet(this.docModelMap.size());
    modelSet.addAll(this.docModelMap.values());
    return modelSet;
  }

  /**
   * Set the document models known by this instance.
   *
   * @param newDocModels Document models
   */
  protected final void setDocModelMap(
          final Map<Integer, DocumentModel> newDocModels) {
    this.docModelMap = newDocModels;
  }

  /**
   * Get the calculated frequency values for each term in the index.
   *
   * FIXME: map objects should be immutable too.
   *
   * @return Immutable map with <tt>Term->(Long) overall index frequency,
   * (Double) relative index frequency</tt> mapping for every term in the index
   */
  protected final Map<String, TermFreqData> getTermFreqMap() {
    return Collections.unmodifiableMap(termFreqMap);
  }

  /**
   * Set the calculated frequency values for each term in the index. Note that
   * <tt>null</tt> is not allowed for any value.
   *
   * @param newTermFreq Mapping of <tt>Term->(Long) overall index frequency,
   * (Double) relative index frequency</tt> for every term in the index
   */
  protected final void setTermFreqMap(
          final Map<String, TermFreqData> newTermFreq) {
    this.termFreqMap = newTermFreq;
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
  public void addDocumentModel(final DocumentModel docModel) {
    if (docModel != null) {
      this.docModelMap.put(docModel.getDocId(), docModel);
    }
  }

  @Override
  public DocumentModel removeDocumentModel(final int docId) {
    return this.docModelMap.remove(docId);
  }
}
