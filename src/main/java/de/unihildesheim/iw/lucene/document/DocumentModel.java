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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Meta-data model for document related information.
 *
 * @author Jens Bertram
 */
public final class DocumentModel
    implements Serializable {
  /**
   * Serialization id.
   */
  private static final long serialVersionUID = -5258723302171674355L;
  /**
   * Referenced Lucene document id.
   */
  public final int id;
  /**
   * Overall frequency of all terms in the document.
   */
  public final long termFrequency;
  /**
   * Mapping of {@code Term} to {@code document-frequency} for every known term
   * in the document.
   */
  private final Map<ByteArray, Long> termFreqMap;

  /**
   * Pre-calculated hash code for this object.
   */
  private int hashCode;

  /**
   * {@link Metrics.DocumentMetrics} instance for this model.
   */
  private transient volatile Metrics.DocumentMetrics metrics;

  /**
   * Create a new model with data from the provided builder.
   *
   * @param builder Builder to use
   */
  DocumentModel(final Builder builder) {
    assert builder != null;

    this.id = builder.docId;
    long tf = 0L;
    for (final Long tfVal : builder.termFreqMap.values()) {
      tf += tfVal;
    }
    this.termFrequency = tf;
    this.termFreqMap = new HashMap<>(builder.termFreqMap.size());
    this.termFreqMap.putAll(builder.termFreqMap);
    calcHash();
  }

  /**
   * Calculate the hash value for this object.
   */
  private void calcHash() {
    this.hashCode = 7;
    this.hashCode = 19 * this.hashCode + this.id;
    this.hashCode = 19 * this.hashCode + (int) (this.termFrequency
        ^ (this.termFrequency
        >>> 32));
    this.hashCode = 19 * this.hashCode * this.termFreqMap.size();
  }

  /**
   * Checks, if a term is known for this document.
   *
   * @param term Term to lookup
   * @return True if it's known
   */
  public boolean contains(final ByteArray term) {
    return this.termFreqMap.containsKey(Objects.requireNonNull(term,
        "Term was null."));
  }

  /**
   * Get the document-frequency for a specific term.
   *
   * @param term Term to lookup
   * @return Frequency in the associated document or <tt>0</tt>, if unknown
   */
  public Long tf(final ByteArray term) {
    final Long tFreq = this.termFreqMap.get(Objects.requireNonNull(term,
        "Term was null."));
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
   * @return {@link Metrics.DocumentMetrics} instance initialized with this
   * model
   */
  public Metrics.DocumentMetrics metrics() {
    if (this.metrics == null) {
      this.metrics = new Metrics.DocumentMetrics(this);
    }
    return this.metrics;
  }

  @Override
  public int hashCode() {
    return this.hashCode;
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
        || this.termFreqMap.size() != other.getTermFreqMap().size()) {
      return false;
    }

    if (!other.getTermFreqMap()
        .keySet().containsAll(this.termFreqMap.keySet())) {
      return false;
    }

    for (final Entry<ByteArray, Long> entry : this.termFreqMap.entrySet()) {
      if (entry.getValue().compareTo(other.getTermFreqMap().get(entry.getKey()))
          != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get a mapping of {@code Term} to {@code document-frequency} for every known
   * term in the document. The returned Map is immutable.
   *
   * @return {@code Term} to {@code document-frequency} mapping (immutable)
   */
  public Map<ByteArray, Long> getTermFreqMap() {
    return Collections.unmodifiableMap(this.termFreqMap);
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
    @SuppressWarnings("PackageVisibleField")
    final Map<ByteArray, Long> termFreqMap;

    /**
     * Id to identify the corresponding document.
     */
    @SuppressWarnings("PackageVisibleField")
    final int docId;

    /**
     * Initializes the Builder with the given document-id.
     *
     * @param documentId Referenced document-id
     */
    public Builder(final int documentId) {
      this.docId = documentId;
      this.termFreqMap = new HashMap<>(DEFAULT_TERMS_COUNT);
    }

    /**
     * Builds a new {@link DocumentModel} based on an already existing one. The
     * document-id and term-frequency map are copied to the new model.
     *
     * @param docModel Model to copy the data from
     */
    public Builder(final DocumentModel docModel) {
      Objects.requireNonNull(docModel, "DocumentModel was null.");

      this.docId = docModel.id;
      this.termFreqMap = new HashMap<>(docModel.getTermFreqMap().size());
      this.termFreqMap.putAll(docModel.getTermFreqMap());
    }

    /**
     * Builds a new {@link DocumentModel} with the given document-id and the
     * expected amount of terms for this document. <br> The amount of terms is
     * used to initialize data structures.
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
     * @param freq Document frequency of the term
     * @return Self reference
     */
    public Builder setTermFrequency(final ByteArray term,
        final long freq) {
      this.termFreqMap.put(Objects.requireNonNull(term, "Term was null."),
          freq);
      return this;
    }

    /**
     * Set the document frequency for a list of terms.
     *
     * @param map Map containing {@code term} to  {@code frequency} mappings
     * @return Self reference
     */
    public Builder setTermFrequency(
        final Map<ByteArray, Long> map) {
      Objects.requireNonNull(map, "Term frequency map was null.");
      for (final Entry<ByteArray, Long> entry : map.entrySet()) {
        this.termFreqMap.put(
            Objects.requireNonNull(entry.getKey(),
                "Null as key is not allowed."),
            Objects.requireNonNull(entry.getValue(),
                "Null as value is not allowed.")
        );
      }
      return this;
    }

    /**
     * Builds the {@link DocumentModel} using the current data.
     *
     * @return New document model with the data of this builder set
     */
    public DocumentModel getModel() {
      return new DocumentModel(this);
    }
  }
}
