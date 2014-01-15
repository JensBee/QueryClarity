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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DefaultDocumentModel implements DocumentModel, Serializable {

  private static final long serialVersionUID = 7526472295622776147L;
  private final int docId;
  private final Map<String, Long> termFreq;
  private final Map<String, Double> termProb;

  /**
   * Creates a new DocumentModel for a specific document and the number of terms
   * in the document.
   *
   * @param documentId Lucene's document-id
   * @param termsCount Number of terms found in the document. This value is used
   * to initialize the term storage to the proper size.
   */
  public DefaultDocumentModel(final int documentId, final int termsCount) {
    this.docId = documentId;
    this.termFreq = new HashMap(termsCount);
    this.termProb = new HashMap(termsCount);
  }

  /**
   * Set the term frequency value for a specific term.
   *
   * @param term Term
   * @param frequency Document frequency for the specific term
   */
  public void setTermFrequency(final String term, final long frequency) {
    this.termFreq.put(term, frequency);
  }

  /**
   * Set the term probability value for a specific term. The term-index is
   * maintained by {@link SimpleIndexDataProvider#termIdx}.
   *
   * @param term Term
   * @param probability Calculated probability value for the specific term
   */
  public void setTermProbability(final String term, final double probability) {
    this.termProb.put(term, probability);
  }

  @Override
  public int id() {
    return this.docId;
  }

  @Override
  public long termFrequency() {
    long value = 0L;
    for (Long freq : termFreq.values()) {
      value += freq;
    }
    return value;
  }

  @Override
  public long termFrequency(final String term) {
    Long value = this.termFreq.get(term);

    if (value == 0) {
      value = 0L;
    }

    return value;
  }

  @Override
  public double termProbability(final String term) {
    Double value = this.termProb.get(term);

    if (value == null) {
      value = 0d;
    }

    return value;
  }

}
