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
package de.unihildesheim.lucene.document;

import java.io.IOException;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enumerator iterating over the terms of multiple document fields in lucene
 * index. This steps through each {@link TermsEnum} for each specified field.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocFieldsTermsEnum {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DocFieldsTermsEnum.class);

  /**
   * Lucene fields to operate on.
   */
  private final String[] fields;

  /**
   * Currently active enumerator.
   */
  private TermsEnum currentEnum = null;

  /**
   * {@link IndexReader} used by this instance.
   */
  private final IndexReader reader;

  /**
   * Lucene document-id for the target document to enumerate over.
   */
  private Integer docId = null;

  /**
   * Current field index the enumerator accesses.
   */
  private int currentFieldIdx;

  /**
   * Indicates whether a enumerator is set.
   */
  private boolean hasEnum = false;

  /**
   * Document fields to enumerate over.
   */
  private Fields docFields;

  /**
   * Generic reusable {@link DocFieldsTermsEnum} instance. To actually reuse this
   * instance the {@link #setDocument(int)} function must be called before
   * {@link #next()} can be used, to set the document to operate on.
   *
   * @param indexReader {@link IndexReader} instance to use
   * @param targetFields Lucene index fields to operate on
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  public DocFieldsTermsEnum(final IndexReader indexReader,
          final String[] targetFields) throws IOException {
    this(indexReader, targetFields, null);
  }

  /**
   * {@link DocFieldsTermsEnum} instance with initial document set.
   *
   * @param documentId Lucene document-id for which the enumeration should be
   * done
   * @param indexReader {@link IndexReader} instance to use
   * @param targetFields Lucene index fields to operate on (list must be unique)
   * and not modified any more, since passed in.
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  public DocFieldsTermsEnum(final IndexReader indexReader,
          final String[] targetFields, final Integer documentId) throws
          IOException {
    if (indexReader == null || targetFields == null) {
      throw new IllegalArgumentException("IndexReader or TargetFields were null");
    }
    if (targetFields.length == 0) {
      throw new IllegalArgumentException("No target fields were specified.");
    }
    this.fields = targetFields.clone();
    this.reader = indexReader;
    if (documentId != null) {
      setDocument(documentId);
    }
  }

  /**
   * Set the id for the document whose terms should be enumerated.
   *
   * @param documentId Lucene document id
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  public void setDocument(final int documentId) throws IOException {
    this.docId = documentId;
    this.docFields = this.reader.getTermVectors(documentId);
    reset();
  }

  /**
   * Resets the iterator keeping the current document-id.
   */
  public void reset() {
    this.hasEnum = false;
    this.currentFieldIdx = 0;
  }

  /**
   * Steps through all fields and provides access to the {@link TermsEnum} for
   * each field. You have to specify a document-id by calling
   * {@link #setDocument(int)} before calling this function.
   *
   * @return The resulting {@link BytesRef} or <code>null</code> if the end of
   * the all field iterators is reached
   * @throws IOException If there is a low-level I/O error
   */
  public BytesRef next() throws IOException {
    if (this.docId == null) {
      throw new IllegalArgumentException("No document-id was specified.");
    }

    if (!this.hasEnum) {
      updateCurrentEnum();
    }

    return getNextValue();
  }

  /**
   * Get the total number of occurrences of this term in the current field.
   *
   * @return The total number of occurrences
   * @throws IOException If there is a low-level I/O error
   */
  public long getTotalTermFreq() throws IOException {
    return this.currentEnum.totalTermFreq();
  }

  /**
   * Try to get the next value from the {@link TermEnum} instance.
   *
   * @return The next {@link ByteRef} value or <code>null</code>, if there a no
   * more values
   * @throws IOException If there is a low-level I/O error
   */
  private BytesRef getNextValue() throws IOException {
    // try to get an iterator which has a value
    BytesRef nextValue;
    if (!this.hasEnum) {
      nextValue = null;
    } else {
      nextValue = this.currentEnum.next();
    }

    while (nextValue == null && this.currentFieldIdx < this.fields.length) {
      updateCurrentEnum();
      if (!this.hasEnum) {
        nextValue = null;
      } else {
        nextValue = this.currentEnum.next();
      }
    }

    return nextValue;
  }

  /**
   * Get the next {@link TermsEnum} pointing at the next field in list. This
   * will try to get the TermVector stored for a field and creates a new
   * {@link TermsEnum} instance for those. If there are no TermVectors stored it
   * will try the next, until all are exhausted.
   *
   * @throws IOException If there is a low-level I/O error
   */
  private void updateCurrentEnum() throws IOException {
    this.hasEnum = false;

    // try all fields. If there are no term vectors stored for the current
    // field, then try the next, until all fields are exhausted
    while (!this.hasEnum && this.currentFieldIdx < this.fields.length) {
      try {
        this.currentEnum = this.docFields.terms(
                this.fields[this.currentFieldIdx]).iterator(this.currentEnum);
        this.hasEnum = true;
      } catch (NullPointerException ex) {
        LOG.error("Caught NullPointerException for field={} doc={}. "
                + "Either the field does not exist or "
                + "there were no term-vectors stored.",
                this.fields[this.currentFieldIdx], this.docId, ex);
      }
      this.currentFieldIdx++;
    }
  }
}
