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
package de.unihildesheim.lucene.queryclarity.indexdata;

import de.unihildesheim.lucene.queryclarity.documentmodel.DefaultDocumentModel;
import de.unihildesheim.lucene.queryclarity.documentmodel.DocumentModel;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jdbm.PrimaryHashMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DefaultIndexDataProvider implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DefaultIndexDataProvider.class);

  /**
   * Reader instance for the target lucene index.
   */
  private final IndexReader reader;

  /**
   * Store term -> frequency in Lucene's index.
   */
  private final PrimaryHashMap<String, Long> termFreq;

  /**
   * Multiplier for relative term frequency inside documents.
   */
  private double langModelWeight = 0.6d;

  /**
   * Store term -> relative term frequencies.
   */
  private final PrimaryHashMap<String, Double> relativeTermFreq;

  /**
   * Store document-id -> document model
   */
  private final PrimaryHashMap<Integer, DefaultDocumentModel> docModels;

  /**
   * Stores default values for Document->term probabilities, if the term could
   * not be found in the document.
   */
  private final PrimaryHashMap<String, Double> defaultDocTermProbability;

  /**
   * Lucene fields to operate on.
   */
  private final Collection<String> targetFields;

  /**
   * Create a new {@link IndexDataProvider} from an existing lucene
   * index in the local file system.
   *
   * @param index Lucene index directory location
   * @param fields Lucene fields to operate on
   * @throws IOException Thrown, if the index could not be opened
   * @throws de.unihildesheim.lucene.queryclarity.indexData.IndexDataException
   * Thrown, if not all requested fields are present in the index
   */
  public DefaultIndexDataProvider(final IndexReader indexReader,
          final String[] fields) throws IOException, IndexDataException {
    super();
    // HashSet removes any possible duplicates
    this.targetFields = new HashSet(Arrays.asList(fields));
    this.reader = indexReader;

    LOG.info("DefaultIndexDataProvider instance reader={} fields={}.",
            this.reader, fields);

    // get all indexed fields from index - other fields are not of interes here
    final Collection<String> indexedFields = MultiFields.getIndexedFields(
            this.reader);

    // pre-check if all requested fields are available
    if (!indexedFields.containsAll(this.targetFields)) {
      throw new IndexDataException(IndexDataException.Type.FIELD_NOT_PRESENT,
              this.targetFields, indexedFields);
    }

    // try to create persitent disk backed storage
    String fileName = "data/cache/DefaulIndexDataProviderCache";
    LOG.info("Initializing disk storage ({})", fileName);
    RecordManager recMan = RecordManagerFactory.createRecordManager(fileName);

    relativeTermFreq = recMan.hashMap("relativeTermFreq");
    docModels = recMan.hashMap("docModels");
    termFreq = recMan.hashMap("termFreq");
    defaultDocTermProbability = recMan.hashMap("defaultDocTermProbability");

    // storage created - check if we have values
    boolean needsCommit = false;
    if (termFreq.size() == 0) {
      calculateTermFrequencies();
      needsCommit = true;
    } else {
      LOG.info("Term frequency values loaded from cache.");
    }
    if (relativeTermFreq.size() == 0) {
      calculateRelativeTermFrequencies();
      needsCommit = true;
    } else {
      LOG.info("Relative term frequency values loaded from cache.");
    }
    if (this.docModels.size() == 0) {
      calculateDocumentTermProbability();
      needsCommit = true;
    } else {
      LOG.info("Document models loaded from cache.");
    }
    if (this.defaultDocTermProbability.size() == 0) {
      calculateDefaultDocumentTermProbability();
      needsCommit = true;
    } else {
      LOG.info("Default document-term probabilities loaded from cache.");
    }
    if (needsCommit) {
      recMan.commit();
    }
  }

  private final void calculateDefaultDocumentTermProbability() {
    long startTime = System.nanoTime();
    for (String term : this.termFreq.keySet()) {
      final double defaultProb = (1 - langModelWeight) * relativeTermFreq.get(
              term);
      defaultDocTermProbability.put(term, defaultProb);
    }
    double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.info("Calculation of default document-term probabilities "
            + "for {} terms took {} seconds.", termFreq.size(),
            estimatedTime);
  }

  private final void calculateDocumentTermProbability() throws IOException {
    long startTime = System.nanoTime();
    // create an enumerator enumarating over all specified document fields
    final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(this.reader,
            this.targetFields.toArray(new String[this.targetFields.size()]));

    // cache terms found in the document
    final Map<String, Long> docTerms = new HashMap();
    Long termCount;

    for (int docId = 0; docId < this.reader.maxDoc(); docId++) {
      BytesRef bytesRef;
      int pointer;

      // get/create the document model for the current document
      DefaultDocumentModel docModel = docModels.get(docId);
      if (docModel == null) {
        docModel = new DefaultDocumentModel(docId, termFreq.size());
        docModels.put(docId, docModel);
      }
      // clear previous cached document term values
      docTerms.clear();

      // go through all fields..
      dftEnum.setDocument(docId);
      while ((bytesRef = dftEnum.next()) != null) {
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
      }

      for (String docTerm : docTerms.keySet()) {
        // store the document frequency of the current term to the model
        docModel.setTermFrequency(docTerm, docTerms.get(docTerm));
      }

      // now overall document frequency is available
      final double docTermCount = (double) docModel.termFrequency();

      // calculate probability values
      for (String docTerm : docTerms.keySet()) {
        // document frequency of the current term
        final double docTermFreq = (double) docModel.termFrequency(docTerm);
        // relative document frequency of the current term
        final double relDocTermFreq = docTermFreq / docTermFreq;

        // calculate probability
        final double probability = (langModelWeight * relDocTermFreq)
                + ((1 - langModelWeight)
                * getRelativeTermFrequency(docTerm));

        // store calculated value to model
        docModel.setTermProbability(docTerm, probability);
      }
    }
    double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.info("Calculation of term probabilities for "
            + "all {} terms and {} documents in index "
            + "took {} seconds.", termFreq.size(), this.reader.maxDoc(),
            estimatedTime);
  }

  /**
   * Calculate term frequencies for all terms in the index (in the initial given
   * fields).
   *
   * @throws IOException Thrown, if the index could not be opened
   */
  private final void calculateTermFrequencies() throws IOException {
    long startTime = System.nanoTime();
    final Fields idxFields = MultiFields.getFields(this.reader);

    Terms fieldTerms;
    TermsEnum fieldTermsEnum = null;
    BytesRef bytesRef;
    String term;

    // go through all fields..
    for (String field : this.targetFields) {
      fieldTerms = idxFields.terms(field);

      // ..check if we have terms..
      if (fieldTerms != null) {
        fieldTermsEnum = fieldTerms.iterator(fieldTermsEnum);

        // ..iterate over them,,
        while ((bytesRef = fieldTermsEnum.next()) != null) {
          term = bytesRef.utf8ToString();

          // fast forward seek to term..
          if (fieldTermsEnum.seekExact(bytesRef)) {
            LOG.debug("term={}, freq={}", term, fieldTermsEnum.totalTermFreq());

            // ..and get the frequency value for term + field
            if (termFreq.containsKey(term)) {
              termFreq.put(term, termFreq.get(term) + fieldTermsEnum.
                      totalTermFreq());
            } else {
              termFreq.put(term, fieldTermsEnum.totalTermFreq());
            }
          }
        }
      }
    }
    double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.info("Calculation of term frequencies for all {} terms in index "
            + "took {} seconds.", termFreq.size(), estimatedTime);
  }

  private void calculateRelativeTermFrequencies() {
    double rTermFreq;
    final long cFreq = getTermFrequency();

    for (String term : termFreq.keySet()) {
      final long tFreq = getTermFrequency(term);
      rTermFreq = (double) tFreq / (double) cFreq;
      relativeTermFreq.put(term, rTermFreq);
      LOG.debug("[pC(t)] t={} p={}", term, rTermFreq);
    }
  }

  @Override
  public DocumentModel getDocumentModel(final int documentId) {
    return this.docModels.get(documentId);
  }

  @Override
  public Set<String> getTerms() {
    return termFreq.keySet();
  }

  @Override
  public long getTermFrequency() {
    long termFrequency = 0L;
    for (long freq : termFreq.values()) {
      termFrequency += freq;
    }
    return termFrequency;
  }

  @Override
  public long getTermFrequency(final String term) {
    Long freq = termFreq.get(term);
    if (freq == null) {
      freq = 0L;
    }
    return freq;
  }

  @Override
  public long getTermFrequency(final int documentId) {
    long freq;

    DefaultDocumentModel docModel = this.docModels.get(documentId);
    if (docModel == null) {
      freq = 0l;
    } else {
      freq = docModel.termFrequency();
    }

    return freq;
  }

  @Override
  public long getTermFrequency(final int documentId, final String term) {
    long freq;

    DefaultDocumentModel docModel = this.docModels.get(documentId);
    if (docModel == null) {
      freq = 0l;
    } else {
      freq = docModel.termFrequency(term);
    }

    return freq;
  }

  @Override
  public double getRelativeTermFrequency(String term) {
    Double rTermFreq = relativeTermFreq.get(term);

    // term was not found in the index
    if (rTermFreq == null) {
      rTermFreq = 0d;
    }

    return rTermFreq;
  }

  /**
   * Close this instance.
   */
  @Override
  public void dispose() {
    try {
      this.reader.close();
    } catch (IOException e) {
      LOG.error("Error while disposing instance.", e);
    }
  }

  @Override
  public String[] getTargetFields() {
    return this.targetFields.toArray(new String[this.targetFields.size()]);
  }

  @Override
  public double getDocumentTermProbability(int documentId, String term) {
    Double prob = this.docModels.get(documentId).termProbability(term);
    if (prob == 0) {
      prob = this.defaultDocTermProbability.get(term);
      if (prob == null) {
        prob = 0d;
      }
    }
    return prob;
  }
}
