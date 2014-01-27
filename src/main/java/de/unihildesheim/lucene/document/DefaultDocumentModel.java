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

import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.Tuple;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link DocumentModel} interface. Immutable
 * document model.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultDocumentModel implements DocumentModel, Serializable {

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  /**
   * Logger instance for this class.
   */
  private static final transient Logger LOG = LoggerFactory.getLogger(
          DefaultDocumentModel.class);

  /**
   * If true the model is locked and immutable.
   */
  private boolean locked = false;

  /**
   * Message to throw, if model is locked.
   */
  private static final String LOCKED_MSG = "Operation not supported. "
          + "Object is locked.";

  /**
   * Id of the document associated with this model.
   */
  private Integer docId;

  /**
   * Initial size of {@link DefaultDocumentModel#termFreqMap} storage, if no
   * value was supplied.
   */
  private static final int INITIAL_TERMFREQMAP_SIZE = 100;

  /**
   * List storing triples: Term, Key, Value
   */
  private List<Tuple.Tuple3<BytesWrap, String, Number>> termDataList;

  /**
   * Stores the calculated overall term frequency of all terms from the index.
   */
  private long overallTermFrequency = 0L;

  /**
   * Internal constructor used to create a new {@link DocumentModel} for a
   * specific document denoted by it's Lucene document id. The
   * <code>termsCount</code> value will be used to initialize the internal data
   * store.
   *
   * @param documentId Lucene's document-id
   * @param termsCount Number of terms expected for this document
   */
  protected DefaultDocumentModel(final int documentId, final int termsCount) {
    this.docId = documentId;
    this.termDataList = new ArrayList(termsCount);
  }

  /**
   * Creates a new empty DocumentModel.
   */
  public DefaultDocumentModel() {
    // empty constructor
    this.docId = null;
    this.termDataList = null;
  }

  /**
   * Internal constructor used to create new instances and de-serialize objects.
   *
   * @param documentId Lucene document-id
   * @param newTermDataList
   */
  protected DefaultDocumentModel(final int documentId,
          final List<Tuple.Tuple3<BytesWrap, String, Number>> newTermDataList) {
    this.docId = documentId;
    this.termDataList = newTermDataList;
  }

  /**
   * Get the internal term data. Used for serialization of the instance.
   *
   * @return The internal term data
   */
  protected List<Tuple.Tuple3<BytesWrap, String, Number>> getTermData() {
    return this.termDataList;
  }

  private void createDataStore() {
    this.termDataList = new ArrayList(INITIAL_TERMFREQMAP_SIZE);
  }

  /**
   * {@inheritDoc} Adds the given term to the list of known terms, if it's not
   * already known.
   *
   * @param key Must not start with an underscore (reserved for internal use).
   */
  @Override
  public void setTermData(final BytesWrap term, final String key,
          final Number value) {
    if (this.locked) {
      throw new UnsupportedOperationException(LOCKED_MSG);
    }

    if (this.termDataList == null) {
      createDataStore();
    }
    this.termDataList.add(Tuple.tuple3(term.duplicate(), key, value));
  }

  @Override
  public Number getTermData(final BytesWrap term, final String key) {
    Number retVal = null;

    if (this.termDataList != null) {
      // temporary object for fast lookup (?)
      final int index = this.termDataList.indexOf(Tuple.tupleMatcher(term, key,
              null));

      if (index > -1) {
        // start searching at the first occourence of the term + key
        Tuple.Tuple3<BytesWrap, String, Number> tuple3;
        for (int i = index; i < this.termDataList.size(); i++) {
          tuple3 = this.termDataList.get(i);
          if (tuple3 != null && tuple3.a.equals(term) && tuple3.b.equals(key)) {
            retVal = tuple3.c;
            break;
          }
        }
      } else {
        // not found
        retVal = null;
      }
    }

    return retVal;
  }

  @Override
  public boolean containsTerm(final BytesWrap term) {
    if (term == null || this.termDataList == null) {
      return false;
    }
    return this.termDataList.indexOf(Tuple.tupleMatcher(term, null, null)) > -1;
  }

  @Override
  public void setTermFrequency(final BytesWrap term,
          final long frequency) {
    if (this.locked) {
      throw new UnsupportedOperationException(LOCKED_MSG);
    }
    if (this.termDataList == null) {
      createDataStore();
    }
    this.termDataList.add(Tuple.tuple3(term.duplicate(), "_freq",
            (Number) frequency));
  }

  @Override
  public int getDocId() {
    if (this.docId == null) {
      throw new IllegalStateException("Document id have not been initialized.");
    }
    return this.docId;
  }

  @Override
  public void setDocId(final int documentId) {
    if (this.locked) {
      throw new UnsupportedOperationException(LOCKED_MSG);
    }
    this.docId = documentId;
  }

  @Override
  public long getTermFrequency() {
    if (this.overallTermFrequency == 0L && this.termDataList != null) {
      for (Tuple.Tuple3<BytesWrap, String, Number> tuple3 : this.termDataList) {
        if (tuple3.b.equals("_freq")) {
          this.overallTermFrequency += (long) tuple3.c;
        }
      }
    }
    return this.overallTermFrequency;
  }

  @Override
  public long getTermFrequency(final BytesWrap term) {
    if (term == null) {
      throw new IllegalArgumentException("Term must not be null.");
    }

    final Long value = (Long) getTermData(term, "_freq");
    return value == null ? 0L : value;
  }

  /**
   * {@inheritDoc} Can only be used to initialize an instance. Any further calls
   * will throw an {@link IllegalStateException}.
   */
  @Override
  public void create(final int documentId, final int termsCount) {
    if (this.locked) {
      throw new UnsupportedOperationException(LOCKED_MSG);
    }
    if (this.docId != null || this.termDataList != null) {
      throw new IllegalStateException("Instance already initialized.");
    }
    this.docId = documentId;
    createDataStore();
  }

  @Override
  @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DefaultDocumentModel dDocMod = (DefaultDocumentModel) o;

    if (docId == null ? dDocMod.docId != null : !docId.equals(dDocMod.docId)) {
      return false;
    }

    if (termDataList == null ? dDocMod.termDataList != null : !termDataList.
            equals(dDocMod.termDataList)) {
      return false;
    }

    return false;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 41 * hash + Objects.hashCode(this.docId);
    hash = 41 * hash + Objects.hashCode(this.termDataList);
    return hash;
  }

  @Override
  public void lock() {
    this.locked = true;
  }

  @Override
  public void unlock() {
    this.locked = false;
  }

  @Override
  public boolean isLocked() {
    return this.locked;
  }
}
