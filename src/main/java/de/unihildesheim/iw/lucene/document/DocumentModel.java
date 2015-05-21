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

import de.unihildesheim.iw.util.Buildable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

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
  private static final long serialVersionUID = -7661310855831860522L;

  /**
   * Referenced Lucene document id.
   */
  public final int id;
  /**
   * Overall frequency of all terms in the document.
   */
  private transient long termFrequency;
  /**
   * Terms stored in this model.
   */
  private transient BytesRefHash terms;
  /**
   * Frequencies of all terms stored in this model. The array index is related
   * to the term index in {@link #terms}.
   */
  private final long[] freqs;

  /**
   * Pre-calculated hash code for this object.
   */
  private int hashCode;

  /**
   * Create a new model with data from the provided builder.
   *
   * @param builder Builder to use
   */
  @SuppressWarnings("WeakerAccess")
  DocumentModel(@NotNull final Builder builder) {
    this.id = builder.docId;
    this.terms = builder.terms;
    this.freqs = builder.freqs.stream().mapToLong(l -> l).toArray();
    this.termFrequency = builder.freqs.stream().mapToLong(l -> l).sum();
    calcHash();
  }

  /**
   * Create a new model with data from the provided builder.
   *
   * @param builder Builder to use
   */
  @SuppressWarnings("WeakerAccess")
  DocumentModel(@NotNull final SerializationBuilder builder) {
    this.id = builder.docId;
    this.terms = builder.terms;
    this.freqs = builder.freqs;
    this.termFrequency = Arrays.stream(this.freqs).sum();
    calcHash();
  }

  /**
   * POJO serialization. Customized to handle serialization of the {@link #terms
   * terms list}.
   *
   * @param out Stream
   * @throws IOException Thrown on low-level i/o-errors
   */
  private void writeObject(final ObjectOutputStream out)
      throws IOException {
    out.defaultWriteObject();
    final int size = this.terms.size();
    out.writeInt(size);
    final BytesRef spare = new BytesRef();
    for (int i = 0; i < size; i++) {
      this.terms.get(i, spare);
      out.writeInt(spare.length);
      out.write(spare.bytes, spare.offset, spare.length);
    }
  }

  /**
   * POJO serialization. Customized to handle de-serialization of the {@link
   * #terms terms list}.
   *
   * @param in Stream
   * @throws IOException Thrown on low-level i/o-errors
   * @throws ClassNotFoundException Thrown if de-serialization of the
   * term-frw-map failed.
   */
  @SuppressWarnings({"ObjectAllocationInLoop"})
  private void readObject(final ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.terms = new BytesRefHash();

    final int size = in.readInt();
    final BytesRef spare = new BytesRef();

    for (int i = 0; i < size; i++) {
      final int bytesAvailable = in.readInt();
      spare.bytes = new byte[bytesAvailable];
      spare.length = spare.bytes.length;
      spare.offset = 0;
      final int bytesRead = in.read(spare.bytes);
      if (bytesRead < bytesAvailable) {
        throw new IllegalStateException("Not enough bytes to read. " +
            "Expected " + bytesAvailable + " but got " + bytesRead + '.');
      }
      this.terms.add(spare);
    }

    this.termFrequency = Arrays.stream(this.freqs).sum();
  }

  /**
   * Calculate the hash value for this object.
   */
  private void calcHash() {
    this.hashCode = 7;
    this.hashCode = 19 * this.hashCode + this.id;
    this.hashCode = 19 * this.hashCode + this.terms.hashCode();
    this.hashCode = 19 * this.hashCode + Arrays.hashCode(this.freqs);
  }

  /**
   * Get the relative frequency for a specific term in the document.
   *
   * @param term Term to lookup
   * @return Frequency in the associated document or {@code 0}, if unknown
   */
  public double relTf(@NotNull final BytesRef term) {
    final int idx = this.terms.find(term);
    if (idx == -1) {
      return 0d;
    }
    final long tf = this.freqs[idx];
    if (tf == 0L) {
      return 0d;
    }
    return (double) tf / (double) this.termFrequency;
  }

  /**
   * Get the frequency of all terms in the document.
   *
   * @return Summed frequency of all terms in document
   */
  public long tf() {
    return this.termFrequency;
  }

  /**
   * Get the frequency for a specific term in the document.
   *
   * @param term Term to lookup
   * @return Frequency in the associated document or <tt>0</tt>, if unknown
   */
  public long tf(@NotNull final BytesRef term) {
    final int idx = this.terms.find(term);
    return idx == -1 ? 0L : this.freqs[idx];
  }

  /**
   * Get the number of unique terms in document.
   *
   * @return Number of unique terms in document
   */
  public int termCount() {
    return this.terms.size();
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

    if (this.id != other.id
        || this.termFrequency != other.termFrequency
        || this.terms.size() != other.terms.size()) {
      return false;
    }

    if (!Arrays.equals(this.freqs, other.freqs)) {
      return false;
    }

    final BytesRef spare = new BytesRef();
    for (int i = this.terms.size() - 1; i >= 0; i--) {
      if (other.terms.find(this.terms.get(i, spare)) != i) {
        return false;
      }
    }

    return true;
  }

  /**
   * Get the term values for serialization.
   *
   * @return Current term values
   */
  public BytesRefHash getTermsForSerialization() {
    return this.terms;
  }

  /**
   * Get the frequency values for serialization.
   *
   * @return Current frequency values
   */
  @SuppressFBWarnings("EI_EXPOSE_REP")
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public long[] getFreqsForSerialization() {
    return this.freqs;
  }

  /**
   * Builder to create new {@link DocumentModel}s.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder implements Buildable<DocumentModel> {

    /**
     * Default number of terms to expect for a document. Used to initialize data
     * storage to a appropriate size.
     */
    private static final int DEFAULT_TERMS_COUNT = 100;

    /**
     * Id to identify the corresponding document.
     */
    final int docId;

    /**
     * Terms contained in the new model.
     */
    final BytesRefHash terms;
    /**
     * Frequency values for all terms in the new model.
     */
    final List<Long> freqs;

    /**
     * Initializes the Builder with the given document-id.
     *
     * @param documentId Referenced document-id
     */
    public Builder(final int documentId) {
      this.docId = documentId;
      this.terms = new BytesRefHash();
      this.freqs = new ArrayList<>(DEFAULT_TERMS_COUNT);
    }

    /**
     * Set the document frequency for a list of terms. This operation is not
     * thread safe as only {@link ConcurrentMap#put(Object, Object)} is called
     * for each entry..
     *
     * @param map Map containing {@code term} to  {@code frequency} mappings.
     * Frequency values must be >=0.
     * @return Self reference
     */
    public Builder setTermFrequency(@NotNull final Map<BytesRef, Long> map) {
      map.entrySet().stream()
          .peek(e -> {
            if (e.getValue() == null || e.getKey() == null) {
              throw new IllegalArgumentException("Null entries not allowed.");
            }
          })
          .forEach(e -> setTermFrequency(e.getKey(), e.getValue()));
      return this;
    }

    /**
     * Set the term frequency value for a single term. Terms must be unique.
     *
     * @param term Non null Term
     * @param freq Frequency. Must be >=0.
     * @return Self reference
     */
    public Builder setTermFrequency(
        @NotNull final BytesRef term, final long freq) {
      if (freq < 0L) {
        throw new IllegalArgumentException("Frequency values must be >=0. " +
            "Got '" + freq + '\'');
      }
      if (freq > 0L) { // skip empty terms
        final int idx = this.terms.add(term);
        if (idx >= 0) {
          if (this.freqs.isEmpty()) {
            this.freqs.add(freq);
          } else {
            // pad long array size, if needed
            final int diff = idx - (this.freqs.size() - 1);
            if (diff > 0) {
              for (int i = 0; i < diff; i++) {
                this.freqs.add(0L);
              }
            }
            this.freqs.add(idx, freq);
          }
        } else {
          // terms must be unique
          throw new IllegalArgumentException(
              "Terms must be unique. Term '" + term.utf8ToString() +
                  "' is already present.");
        }
      }
      return this;
    }

    /**
     * Builds the {@link DocumentModel} using the current data.
     *
     * @return New document model with the data of this builder set
     */
    @NotNull
    @Override
    public DocumentModel build() {
      return new DocumentModel(this);
    }
  }

  /**
   * Builder used for creating a model from serialization data.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SerializationBuilder
      implements Buildable<DocumentModel> {
    /**
     * Id to identify the corresponding document.
     */
    final int docId;

    /**
     * Terms contained in the new model.
     */
    final BytesRefHash terms;
    /**
     * Frequency values for all terms in the new model.
     */
    final long[] freqs;

    /**
     * Initialize the builder with all base data.
     *
     * @param docId Document-id
     * @param termCount Number of terms in the model
     * @param termFreqs Frequency values for all terms added later. Order of
     * terms added by using {@link #addTerm(BytesRef)} must match this order.
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    public SerializationBuilder(
        final int docId, final int termCount,
        @NotNull final long... termFreqs) {
      this.docId = docId;
      this.freqs = termFreqs;
      this.terms = new BytesRefHash(
          new ByteBlockPool(new DirectAllocator()),
          termCount,
          new DirectBytesStartArray(termCount)
      );
    }

    /**
     * Add a term to the documents terms list.
     *
     * @param term Term
     */
    public void addTerm(@NotNull final BytesRef term) {
      this.terms.add(term);
    }

    /**
     * Builds the {@link DocumentModel} using the current data.
     *
     * @return New document model with the data of this builder set
     */
    @NotNull
    @Override
    public DocumentModel build() {
      return new DocumentModel(this);
    }
  }
}
