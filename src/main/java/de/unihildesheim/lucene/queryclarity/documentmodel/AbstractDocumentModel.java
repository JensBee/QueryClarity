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
package de.unihildesheim.lucene.queryclarity.documentmodel;

import de.unihildesheim.lucene.queryclarity.indexdata.AbstractIndexDataProvider;
import de.unihildesheim.lucene.queryclarity.indexdata.DocFieldsTermsEnum;
import java.io.IOException;
import java.util.Set;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data collection for a single document model instance.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public abstract class AbstractDocumentModel {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          AbstractDocumentModel.class);

  /**
   * Lucene document id to which this model belongs to.
   */
  private final int docId;

  /**
   * Create a new document model with the given lucene document-id, pointing at
   * the document associated with this model.
   *
   * @param documentId Lucene document-id
   */
  AbstractDocumentModel(final int documentId) {
    docId = documentId;
  }

  /**
   * Get the lucene document-id for the associated document with this model.
   *
   * @return Lucene document-id for this model
   */
  public int getDocId() {
    return docId;
  }

  /**
   * Calculate the product of all probability values for all terms specified in
   * the given set of terms.
   *
   * @param terms Terms which probability values should be multiplied
   * @return Result of multiplication of all probability values for all given
   * terms
   */
  public double getProbabilityProduct(final Set<String> terms) {
    double result = 1d;
    for (String term : terms) {
      result *= termProbability(term);
    }
    return result;
  }

  /**
   * Get the {@link DocFieldsTermsEnum} used by this instance.
   *
   * @return {@link DocFieldsTermsEnum} used by this instance
   */
  protected abstract DocFieldsTermsEnum getDocFieldsTermsEnum();

  /**
   * Get the multiplier for the languagemodel weighting.
   *
   * @return Multiplier value for the languagemodel weighting.
   */
  protected abstract double getLangmodelWeight();

  /**
   * Get the {@link AbstractIndexDataProvider} used by this instance.
   *
   * @return {@link AbstractIndexDataProvider} used by this instance
   */
  protected abstract AbstractIndexDataProvider getAbstractIndexDataProvider();

  /**
   * Calculate the document probability pD(t) for a given term t. For better
   * perfomance these values should be pre-calculated.
   *
   * @param term The term to do the calculation for
   * @return Calculated document probability value for term t. May return null,
   * if there were errors calculating the value.
   */
  public Double termProbability(final String term) {
    Double probability;

    // try to get the pre-calculated value from
    // {@link AbstractIndexDataProvider}
    try {
      probability = getAbstractIndexDataProvider().getDocumentTermProbability(
              getDocId(), term);
    } catch (UnsupportedOperationException e) {
      probability = null;
    }

    if (probability == null) {
      LOG.trace("[pD(t)] D={} t={}", term, getDocId());
      // total number of terms in document
      long numTerms = getTermFrequency();
      // frequency of searchterm in document
      long numQueryTerm = getTermFrequency(term);
      probability = null;

      if (numTerms != 0) {
        LOG.debug("[pD(t)] D={} t={} ft,D={} fD={}", getDocId(), term,
                numQueryTerm, numTerms);

        final double relativeTermFreq = (double) numQueryTerm
                / (double) numTerms;
        probability = (getLangmodelWeight() * relativeTermFreq) + ((1
                - getLangmodelWeight()) * getAbstractIndexDataProvider().
                getRelativeTermFrequency(term));
      }
    }

    if (probability <= 0) {
      throw new IllegalArgumentException("[pD(t)] for term=" + term
              + " was 0. This should only happen, "
              + "if you passed in a term not found in the collection.");
    }

    LOG.debug("[pD(t)] D={} t={} p={}", getDocId(), term, probability);

    return probability;
  }

  /**
   * Get the frequency of the given term in this document.
   *
   * @param term Term to lookup
   * @return Frequency of the given term in this document
   */
  public long getTermFrequency(final String term) {
    long frequency = 0;

    BytesRef bytesRef;
    String docTerm;
    long freq;
    DocFieldsTermsEnum dftEnum = getDocFieldsTermsEnum();
    dftEnum.setDocument(getDocId());

    try {
      while ((bytesRef = dftEnum.next()) != null) {
        docTerm = bytesRef.utf8ToString();
        // get the frequency value from index
        freq = dftEnum.getTotalTermFreq();

        LOG.trace("[pD(t)] term={} freq={}", docTerm, freq);

        if (term != null && docTerm.equals(term)) {
          // count only the requested term
          frequency += freq;
        } else if (term == null) {
          // count all terms
          frequency += freq;
        }
      }
    } catch (IOException e) {
      LOG.error("Caught exception while enumerating terms for document id="
              + getDocId() + ".");
      frequency = 0;
    }

    return frequency;
  }

  /**
   * Get the overall frequency of all terms in this document.
   *
   * @return Summed frequency of all terms in this document
   */
  public long getTermFrequency() {
    return getTermFrequency(null);
  }
}
