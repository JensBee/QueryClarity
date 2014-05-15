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
package de.unihildesheim.iw.lucene.document;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.index.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Meta-data model for document related information.
 *
 * @author Jens Bertram
 */
public final class DocumentModel {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      DocumentModel.class);
  /**
   * Referenced Lucene document id.
   */
  @SuppressWarnings("checkstyle:visibilitymodifier")
  public final int id;
  /**
   * Overall frequency of all terms in the document.
   */
  @SuppressWarnings("checkstyle:visibilitymodifier")
  public final long termFrequency;
  /**
   * Term->document-frequency mapping for every known term in the document.
   */
  @SuppressWarnings("checkstyle:visibilitymodifier")
  public final Map<ByteArray, Long> termFreqMap;

  /**
   * Pre-calculated hash code for this object.
   */
  private int hashCode;

  /**
   * {@link Metrics.DocumentMetrics} instance for this model.
   */
  private Metrics.DocumentMetrics metrics;

  /**
   * Create a new model with data from the given builder.
   *
   * @param builder Builder to use
   */
  private DocumentModel(final Builder builder) {
    if (builder == null) {
      throw new NullPointerException("Builder was null.");
    }
    this.id = builder.docId;
    this.termFrequency = builder.termFreq;
    this.termFreqMap = new HashMap<>(builder.termFreqMap.size());
    this.termFreqMap.putAll(builder.termFreqMap);
    calcHash();
  }

  /**
   * Check if a term is known for this document.
   *
   * @param term Term to lookup
   * @return True if known
   */
  public boolean contains(final ByteArray term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    return this.termFreqMap.containsKey(term);
  }

  /**
   * Get the document-frequency for a specific term.
   *
   * @param term Term to lookup
   * @return Frequency in the associated document or <tt>0</tt>, if unknown
   */
  public Long tf(final ByteArray term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    final Long tFreq = this.termFreqMap.get(term);
    if (tFreq == null) {
      return 0L;
    }
    return tFreq;
  }

  /**
   * Get the number of unique terms in document.
   *
   * @return Number of unique terms in document
   */
  public long termCount() {
    final Integer count = this.termFreqMap.size();
    // check for case where are more than Integer.MAX_VALUE entries
    if (count == Integer.MAX_VALUE) {
      Long manualCount = 0L;
      for (final ByteArray ignored : this.termFreqMap.keySet()) {
        manualCount++;
      }
      return manualCount;
    }
    return count.longValue();
  }

  /**
   * Get a {@link Metrics.DocumentMetrics} instance for this model.
   *
   * @return {@link Metrics.DocumentMetrics} instance loaded with this model
   */
  public Metrics.DocumentMetrics metrics() {
    if (this.metrics == null) {
      this.metrics = new Metrics.DocumentMetrics(this);
    }
    return this.metrics;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof DocumentModel)) {
      return false;
    }

    final DocumentModel other = (DocumentModel) o;

    if (this.id != other.id || this.termFrequency != other.termFrequency
        || this.termFreqMap.size() != other.termFreqMap.size()) {
      return false;
    }

    if (!other.termFreqMap.keySet().containsAll(this.termFreqMap.keySet())) {
      LOG.debug("FAIL 2");
      return false;
    }

    for (final Entry<ByteArray, Long> entry : this.termFreqMap.entrySet()) {
      if (entry.getValue().compareTo(other.termFreqMap.get(entry.getKey()))
          != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Calculate the hash value for this object.
   */
  @SuppressWarnings("checkstyle:magicnumber")
  private void calcHash() {
    this.hashCode = 7;
    this.hashCode = 19 * this.hashCode + this.id;
    this.hashCode = 19 * this.hashCode + (int) (this.termFrequency
        ^ (this.termFrequency
        >>> 32));
    this.hashCode = 19 * this.hashCode * this.termFreqMap.size();
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  /**
   * Builder to create new {@link DocumentModel}s.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder {

    /**
     * Default number of terms to expect for a document. Used to initialize data
     * storage to a appropriate size.
     */
    private static final int DEFAULT_TERMS_COUNT = 100;
    /**
     * Term -> frequency mapping for every known term in the document.
     */
    private final Map<ByteArray, Long> termFreqMap;
    /**
     * Id to identify the corresponding document.
     */
    private final int docId;
    /**
     * Overall term frequency of the corresponding document.
     */
    private long termFreq;

    /**
     * Builds a new {@link DocumentModel} with the given id.
     *
     * @param documentId Referenced document id
     */
    public Builder(final int documentId) {
      this.docId = documentId;
      this.termFreqMap = new HashMap<>(DEFAULT_TERMS_COUNT);
    }

    /**
     * Builds a new {@link DocumentModel} based on an already existing one.
     *
     * @param docModel Model to get the data from
     */
    public Builder(final DocumentModel docModel) {
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
    public Builder(final int documentId,
        final int termsCount) {
      this.docId = documentId;
      this.termFreqMap = new HashMap<>(termsCount);
    }

    /**
     * Set the document frequency for a specific term.
     *
     * @param term Term
     * @param freq Frequency of term
     * @return Self reference
     */
    public Builder setTermFrequency(final ByteArray term,
        final long freq) {
      if (term == null) {
        throw new IllegalArgumentException("Term was null.");
      }
      this.termFreqMap.put(term, freq);
      return this;
    }

    /**
     * Set the document frequency for a list of terms.
     *
     * @param map Map containing <tt>term -> frequency</tt> mapping
     * @return Self reference
     */
    public Builder setTermFrequency(
        final Map<ByteArray, Long> map) {
      for (final Entry<ByteArray, Long> entry : map.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          throw new NullPointerException("Encountered null value in "
              + "termFreqMap.");
        }
        this.termFreqMap.put(entry.getKey(), entry.getValue());
      }
      return this;
    }

    /**
     * Build the document model.
     *
     * @return New document model with the data of this builder set
     */
    public DocumentModel getModel() {
      for (final Long tf : this.termFreqMap.values()) {
        this.termFreq += tf;
      }
      return new DocumentModel(this);
    }
  }
}
