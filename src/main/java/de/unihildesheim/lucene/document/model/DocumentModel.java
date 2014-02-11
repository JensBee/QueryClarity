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
package de.unihildesheim.lucene.document.model;

import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.lucene.util.BytesWrap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocumentModel {

  /**
   * Global configuration object.
   */
  private static final ClarityScoreConfiguration CONF
          = ClarityScoreConfiguration.getInstance();

  /**
   * Referenced Lucene document id.
   */
  public final int id;
  /**
   * Overall frequency of all terms in the document.
   */
  public final long termFrequency;
  /**
   * Term->document-frequency mapping for every known term in the document.
   */
  public final Map<BytesWrap, Long> termFreqMap;

  /**
   * Create a new model with data from the given builder.
   *
   * @param builder Builder to use
   */
  private DocumentModel(final DocumentModelBuilder builder) {
    if (builder == null) {
      throw new NullPointerException("Builder was null.");
    }
    this.id = builder.docId;
    this.termFrequency = builder.termFreq;
    this.termFreqMap = Collections.unmodifiableMap(builder.termFreqMap);
  }

  /**
   * Check if a term is known for this document.
   *
   * @param term Term to lookup
   * @return True if known
   */
  public boolean contains(final BytesWrap term) {
    if (term == null) {
      return false;
    }
    return this.termFreqMap.containsKey(term);
  }

  /**
   * Get the document-frequency for a specific term.
   *
   * @param term Term to lookup
   * @return Frequency in the associated document or <tt>null</tt>, if unknown
   */
  public Long termFrequency(final BytesWrap term) {
    if (term == null) {
      return null;
    }
    return this.termFreqMap.get(term);
  }

  /**
   * Builder to create new {@link DocumentModel}s.
   */
  public static final class DocumentModelBuilder {

    /**
     * Prefix used to store configuration.
     */
    private static final String CONF_PREFIX = "DMBuilder_";
    /**
     * Default number of terms to expect for a document. Used to initialize data
     * storage to a appropriate size.
     */
    private static final int DEFAULT_TERMS_COUNT = CONF.getInt(CONF_PREFIX
            + "defaultTermsCount", 100);
    /**
     * Term -> frequency mapping for every nown term in the document.
     */
    private final Map<BytesWrap, Long> termFreqMap;
    /**
     * Id to identify the corresponding document.
     */
    private int docId;
    /**
     * Overall term frequency of the corresponding document.
     */
    private long termFreq = 0L;

    /**
     * Builds a new {@link DocumentModel} with the given id.
     *
     * @param documentId Referenced document id
     */
    public DocumentModelBuilder(final int documentId) {
      this.docId = documentId;
      this.termFreqMap = new HashMap<>(DEFAULT_TERMS_COUNT);
    }

    /**
     * Builds a new {@link DocumentModel} based on an already existing one.
     *
     * @param docModel Model to get the data from
     */
    public DocumentModelBuilder(final DocumentModel docModel) {
      if (docModel == null) {
        throw new IllegalArgumentException("Model was null.");
      }
      this.docId = docModel.id;
      this.termFreqMap = new HashMap<>(docModel.termFreqMap.size());
      this.termFreqMap.putAll(docModel.termFreqMap);
      this.termFreq = docModel.termFrequency;
    }

    /**
     * Builds a new {@link DocumentModel} with the given id and the expected
     * amount of terms for this document.
     *
     * @param documentId Referenced document id
     * @param termsCount Expected number of terms
     */
    public DocumentModelBuilder(final int documentId, final int termsCount) {
      this.docId = documentId;
      this.termFreqMap = new HashMap<>(termsCount);
    }

    /**
     * Set the overall term frequency.
     *
     * @param tFreq Overall term frequency
     * @return Self reference
     */
    public DocumentModelBuilder setTermFrequency(final long tFreq) {
      this.termFreq = tFreq;
      return this;
    }

    /**
     * Set the document frequency for a specific term.
     *
     * @param term Term
     * @param freq Frequency of term
     * @return Self reference
     */
    public DocumentModelBuilder setTermFrequency(final BytesWrap term,
            final long freq) {
      if (term == null) {
        throw new IllegalArgumentException("Term was null.");
      }
      this.termFreq += freq; // add current value to overall value
      this.termFreqMap.put(term.clone(), freq);
      return this;
    }

    /**
     * Set the document frequency for a list of terms.
     *
     * @param map Map containing <tt>term -> frequency</tt> mapping
     * @return Self reference
     */
    public DocumentModelBuilder setTermFrequency(
            final Map<BytesWrap, Number> map) {
      for (Entry<BytesWrap, Number> entry : map.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          throw new NullPointerException("Encountered null value in "
                  + "termFreqMap.");
        }
        this.termFreqMap.put(entry.getKey().clone(), entry.getValue().
                longValue());
      }
      return this;
    }

    /**
     * Build the document model.
     *
     * @return New document model with the data of this builder set
     */
    public DocumentModel getModel() {
      return new DocumentModel(this);
    }
  }
}
