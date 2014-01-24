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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
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
  private final Set<String> targetFields;

  /**
   * Lucene fields to operate on.
   */
  private LinkedList<String> currentFields;

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
   * Generic reusable {@link DocFieldsTermEnum} instance. To actually reuse this
   * instance the {@link setDocument} function must be called before
   * {@link next} can be used, to set the document to operate on.
   *
   * @param indexReader {@link IndexReader} instance to use
   * @param fields Lucene index fields to operate on
   */
  public DocFieldsTermsEnum(final IndexReader indexReader,
          final String[] fields) {
    this(indexReader, fields, null);
  }

  /**
   * {@link DocFieldsTermEnum} instance with initial document set.
   *
   * @param documentId Lucene document-id for which the enumeration should be
   * done
   * @param indexReader {@link IndexReader} instance to use
   * @param fields Lucene index fields to operate on
   */
  public DocFieldsTermsEnum(final IndexReader indexReader,
          final String[] fields, final Integer documentId) {
    if (indexReader == null) {
      throw new IllegalArgumentException("IndexReader was null.");
    }
    if (fields == null || fields.length == 0) {
      throw new IllegalArgumentException("No target fields were specified.");
    }
    this.targetFields = new HashSet(Arrays.asList(fields));
    this.currentFields = new LinkedList(this.targetFields);
    this.reader = indexReader;
    this.docId = documentId;
  }

  /**
   * Set the id for the document whose terms should be enumerated.
   *
   * @param documentId Lucene document id
   */
  public void setDocument(final int documentId) {
    this.docId = documentId;
    reset();
  }

  /**
   * Resets the iterator keeping the current document-id.
   */
  public void reset() {
    this.currentFields = new LinkedList(this.targetFields);
    this.currentEnum = null;
  }

  /**
   * Steps through all fields and provides access to the {@link TermsEnum} for
   * each field. You have to specify a document-id by calling
   * {@link setDocument} before calling this function.
   *
   * @return The resulting {@link BytesRef} or <code>null</code> if the end of
   * the all field iterators is reached
   * @throws IOException If there is a low-level I/O error
   */
  public BytesRef next() throws IOException {
    BytesRef nextValue;

    if (this.docId == null) {
      throw new IllegalArgumentException("No document-id was specified.");
    }

    if (this.currentEnum == null) {
      updateCurrentEnum();
    }

    nextValue = getNextValue();
    return nextValue;
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
    if (this.currentEnum == null) {
      nextValue = null;
    } else {
      nextValue = this.currentEnum.next();
    }

    while (nextValue == null && !this.currentFields.isEmpty()) {
      updateCurrentEnum();
      if (this.currentEnum == null) {
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
    String targetField;
    Terms termVector;

    // try all fields. If there are no term vectors stored for the current
    // field, then try the next, until all fields are exhausted
    while (!this.currentFields.isEmpty()) {
      // get next field
      targetField = this.currentFields.pop();
      LOG.trace("Trying field field={} remaining={}", targetField,
              this.currentFields.size());
      // try to get term vectors for this field
      termVector = this.reader.getTermVector(this.docId, targetField);

      // check if we have TermVectors set
      if (termVector != null) {
        LOG.trace("Trying field field={} tv=true", targetField);
        this.currentEnum = termVector.iterator(null);
        break;
      }
      this.currentEnum = null;
      LOG.warn("No TermVector found for doc={} field={}. "
              + "Unable to get any term information for this docment field.",
              this.docId, targetField);
    }
  }
}
